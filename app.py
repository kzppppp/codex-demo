from __future__ import annotations

import asyncio
import math
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
app = FastAPI(title="ETF Quant T Advisor", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# 监控池。用户可以直接在页面输入任意 6 位 ETF 代码。
SYMBOLS: dict[str, dict[str, str]] = {
    "510300": {"name": "沪深300ETF", "market": "1", "group": "宽基"},
    "510500": {"name": "中证500ETF", "market": "1", "group": "宽基"},
    "510880": {"name": "红利ETF", "market": "1", "group": "红利"},
    "512100": {"name": "中证1000ETF", "market": "1", "group": "宽基"},
    "512480": {"name": "半导体ETF", "market": "1", "group": "科技"},
    "512690": {"name": "酒ETF", "market": "1", "group": "消费"},
    "588000": {"name": "科创50ETF", "market": "1", "group": "科技"},
    "159915": {"name": "创业板ETF", "market": "0", "group": "成长"},
    "159919": {"name": "沪深300ETF（深）", "market": "0", "group": "宽基"},
    "159949": {"name": "创业板50ETF", "market": "0", "group": "成长"},
    "159865": {"name": "养殖ETF", "market": "0", "group": "行业"},
    "159928": {"name": "消费ETF", "market": "0", "group": "消费"},
    "159995": {"name": "芯片ETF", "market": "0", "group": "科技"},
}

QUOTE_FIELDS = "f43,f57,f58,f169,f170,f46,f44,f45,f47,f48,f60,f168"
KLINE_FIELDS = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
CODE_RE = re.compile(r"^\d{6}$")

# 轻量缓存让扫描时使用同一时间窗口的数据，减少接口限流和同一只 ETF 的重复请求。
_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_LOCK = asyncio.Lock()


async def cache_get(key: str) -> Any | None:
    async with _CACHE_LOCK:
        item = _CACHE.get(key)
        if not item or item[0] <= time.monotonic():
            _CACHE.pop(key, None)
            return None
        return item[1]


async def cache_set(key: str, value: Any, ttl: float) -> None:
    async with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic() + ttl, value)


def symbol_info(symbol: str) -> dict[str, str]:
    symbol = symbol.strip()
    if not CODE_RE.fullmatch(symbol):
        raise HTTPException(status_code=400, detail="ETF代码应为6位数字，例如159915")
    known = SYMBOLS.get(symbol)
    if known:
        return {"code": symbol, **known}
    market = "1" if symbol.startswith(("50", "51", "52", "56", "58")) else "0"
    return {"code": symbol, "name": f"ETF {symbol}", "market": market, "group": "自定义"}


async def eastmoney_json(url: str, cache_key: str | None = None, ttl: float = 0) -> dict[str, Any]:
    if cache_key and ttl:
        cached = await cache_get(cache_key)
        if cached is not None:
            return cached
    headers = {
        "User-Agent": "Mozilla/5.0 ETF-Quant-T-Advisor/0.2",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://quote.eastmoney.com/",
    }
    try:
        async with httpx.AsyncClient(timeout=12, headers=headers) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=f"行情服务暂时不可用：{exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="行情服务返回格式异常")
    if cache_key and ttl:
        await cache_set(cache_key, payload, ttl)
    return payload


def number(data: dict[str, Any], key: str, scale: float = 1.0) -> float | None:
    value = data.get(key)
    if value is None:
        return None
    try:
        result = float(value) / scale
        return None if math.isnan(result) or math.isinf(result) else round(result, 6)
    except (TypeError, ValueError):
        return None


async def fetch_quote(info: dict[str, str]) -> dict[str, Any]:
    secid = f"{info['market']}.{info['code']}"
    url = "https://push2.eastmoney.com/api/qt/stock/get" f"?secid={secid}&fields={QUOTE_FIELDS}"
    payload = await eastmoney_json(url, cache_key=f"quote:{info['code']}", ttl=12)
    data = payload.get("data") or {}
    if not data:
        raise HTTPException(status_code=404, detail="找不到该 ETF 行情，请检查代码")
    return {
        "code": info["code"],
        "name": data.get("f58") or info["name"],
        "price": number(data, "f43", 100),
        "pre_close": number(data, "f60", 100),
        "change_percent": number(data, "f170", 100),
        "change_amount": number(data, "f169", 100),
        "open": number(data, "f46", 100),
        "high": number(data, "f44", 100),
        "low": number(data, "f45", 100),
        "volume": number(data, "f47"),
        "amount": number(data, "f48"),
        "turnover": number(data, "f168", 100),
        "timestamp": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
    }


async def fetch_history(info: dict[str, str], days: int = 240) -> pd.DataFrame:
    days = max(90, min(days, 500))
    secid = f"{info['market']}.{info['code']}"
    begin = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}&fields1=f1,f2,f3,f4&fields2={KLINE_FIELDS}"
        f"&klt=101&fqt=1&beg={begin}&end=20500101"
    )
    payload = await eastmoney_json(url, cache_key=f"history:{info['code']}:{days}", ttl=120)
    rows = (payload.get("data") or {}).get("klines") or []
    parsed: list[list[Any]] = []
    for row in rows[-days:]:
        parts = str(row).split(",")
        if len(parts) < 6:
            continue
        try:
            parsed.append(
                [
                    parts[0],
                    float(parts[1]),
                    float(parts[2]),
                    float(parts[3]),
                    float(parts[4]),
                    float(parts[5]),
                    float(parts[6]) if len(parts) > 6 else 0.0,
                ]
            )
        except (TypeError, ValueError):
            continue
    if len(parsed) < 60:
        raise HTTPException(status_code=404, detail="历史行情不足，暂时无法计算可靠信号")
    return pd.DataFrame(parsed, columns=["date", "open", "high", "low", "close", "volume", "amount"])


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sma5"] = out["close"].rolling(5, min_periods=5).mean()
    out["sma20"] = out["close"].rolling(20, min_periods=20).mean()
    out["ema12"] = out["close"].ewm(span=12, adjust=False).mean()
    out["ema26"] = out["close"].ewm(span=26, adjust=False).mean()
    out["macd"] = out["ema12"] - out["ema26"]
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    delta = out["close"].diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, float("nan"))
    out["rsi14"] = (100 - (100 / (1 + rs))).fillna(50)
    prev_close = out["close"].shift(1)
    true_range = pd.concat(
        [out["high"] - out["low"], (out["high"] - prev_close).abs(), (out["low"] - prev_close).abs()], axis=1
    ).max(axis=1)
    out["atr14"] = true_range.rolling(14, min_periods=14).mean()
    out["atr_pct"] = out["atr14"] / out["close"] * 100
    out["vol_ratio"] = out["volume"] / out["volume"].rolling(20, min_periods=20).mean()
    out["return_5d"] = out["close"].pct_change(5) * 100
    out["return_20d"] = out["close"].pct_change(20) * 100
    return out


def safe_float(value: Any, digits: int = 4) -> float | None:
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None
        return round(result, digits)
    except (TypeError, ValueError):
        return None


def score_row(row: pd.Series, include_reasons: bool = True) -> dict[str, Any]:
    """固定权重、可解释的方向评分。历史回测和当前信号共用这一套规则，避免展示分和回测分不一致。"""
    close = float(row["close"])
    sma5 = float(row["sma5"]) if pd.notna(row["sma5"]) else close
    sma20 = float(row["sma20"]) if pd.notna(row["sma20"]) else close
    rsi = float(row["rsi14"]) if pd.notna(row["rsi14"]) else 50.0
    macd = float(row["macd"]) if pd.notna(row["macd"]) else 0.0
    macd_signal = float(row["macd_signal"]) if pd.notna(row["macd_signal"]) else 0.0
    vol_ratio = float(row["vol_ratio"]) if pd.notna(row["vol_ratio"]) else 1.0
    return_5d = float(row["return_5d"]) if pd.notna(row["return_5d"]) else 0.0
    atr_pct = float(row["atr_pct"]) if pd.notna(row["atr_pct"]) else 2.0

    score = 50
    reasons: list[str] = []
    if close > sma20:
        score += 14
        reasons.append("现价在20日均线上方")
    else:
        score -= 14
        reasons.append("现价在20日均线下方")
    if sma5 > sma20:
        score += 12
        reasons.append("5日均线向上")
    else:
        score -= 12
        reasons.append("5日均线偏弱")
    if macd > macd_signal:
        score += 10
        reasons.append("MACD动能偏强")
    else:
        score -= 10
        reasons.append("MACD动能偏弱")
    if 45 <= rsi <= 68:
        score += 8
        reasons.append("RSI处于可跟随区间")
    elif rsi < 30:
        score += 3
        reasons.append("RSI超卖，反弹需确认")
    elif rsi > 75:
        score -= 8
        reasons.append("RSI过热，不追高")
    else:
        reasons.append("RSI处于过渡区")
    if return_5d > 0:
        score += 8
        reasons.append("近5日动量为正")
    else:
        score -= 8
        reasons.append("近5日动量为负")
    if 0.8 <= vol_ratio <= 1.8:
        score += 3
        reasons.append("成交量处于正常放大范围")
    elif vol_ratio > 2.5:
        score -= 3
        reasons.append("成交量异常放大，波动风险高")
    if atr_pct > 4:
        score -= 6
        reasons.append("ATR波动偏高，降低仓位")

    score = max(0, min(100, int(round(score))))
    if score >= 70 and rsi <= 75:
        side, label, tone, action = "BUY", "买入候选", "positive", "只在回踩区分批买，不追盘中急拉"
    elif score <= 38:
        side, label, tone, action = "REDUCE", "回避/减仓", "negative", "反弹优先减仓，暂停补仓"
    else:
        side, label, tone, action = "WAIT", "等待", "caution", "等待价格回踩或均线重新确认"
    return {
        "score": score,
        "side": side,
        "label": label,
        "tone": tone,
        "action": action,
        "reasons": reasons if include_reasons else [],
    }


def walk_forward_backtest(data: pd.DataFrame, cost_bps: float = 12.0) -> dict[str, Any]:
    """用过去已完成的日线，按 t 的信号交易 t+1，避免把未来数据泄漏进当前信号。"""
    start = max(40, data["sma20"].first_valid_index() or 40)
    returns: list[float] = []
    wins = 0
    trades = 0
    for idx in range(int(start), len(data) - 1):
        signal = score_row(data.iloc[idx], include_reasons=False)
        if signal["side"] == "WAIT":
            continue
        next_return = float(data.iloc[idx + 1]["close"] / data.iloc[idx]["close"] - 1)
        direction = 1 if signal["side"] == "BUY" else -1
        net_return = direction * next_return - cost_bps / 10000
        returns.append(net_return)
        trades += 1
        wins += int(net_return > 0)
    if not returns:
        return {"trades": 0, "wins": 0, "win_rate": None, "avg_return": None, "max_drawdown": None, "coverage": 0}
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for ret in returns:
        equity *= 1 + ret
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1)
    return {
        "trades": trades,
        "wins": wins,
        "win_rate": round(wins / trades * 100, 1),
        "avg_return": round(sum(returns) / trades * 100, 3),
        "max_drawdown": round(max_drawdown * 100, 2),
        "coverage": round(trades / max(1, len(data) - int(start) - 1) * 100, 1),
        "cost_bps": cost_bps,
    }


def build_analysis(info: dict[str, str], quote: dict[str, Any], history: pd.DataFrame) -> dict[str, Any]:
    data = add_indicators(history)
    latest = data.iloc[-1]
    latest_close = float(latest["close"])
    price = float(quote.get("price") or latest_close)
    score = score_row(latest)
    backtest = walk_forward_backtest(data)
    atr = safe_float(latest["atr14"], 4) or price * 0.015
    atr_pct = safe_float(latest["atr_pct"], 2) or 1.5
    quote_delta = (price / latest_close - 1) * 100 if latest_close else 0.0
    aligned = abs(quote_delta) <= max(0.35, atr_pct * 0.35)

    # 交易区间只做风险预算参考，不是自动下单价格。
    entry_low = price - atr * 0.65
    entry_high = price - atr * 0.2
    stop = price - atr * 1.5
    target = price + atr * 1.5
    if score["side"] == "REDUCE":
        entry_low, entry_high = price - atr * 0.3, price

    chart = []
    for _, row in data.tail(90).iterrows():
        chart.append({
            "date": str(row["date"]),
            "close": safe_float(row["close"], 4),
            "sma20": safe_float(row["sma20"], 4),
        })

    return {
        "symbol": info,
        "quote": quote,
        "signal": {
            **score,
            "backtest_win_rate": backtest["win_rate"],
            "sample_trades": backtest["trades"],
            "updated_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
        },
        "indicators": {
            "sma5": safe_float(latest["sma5"]),
            "sma20": safe_float(latest["sma20"]),
            "rsi14": safe_float(latest["rsi14"], 2),
            "macd": safe_float(latest["macd"], 5),
            "macd_signal": safe_float(latest["macd_signal"], 5),
            "atr14": safe_float(latest["atr14"], 4),
            "atr_pct": atr_pct,
            "vol_ratio": safe_float(latest["vol_ratio"], 2),
            "return_5d": safe_float(latest["return_5d"], 2),
            "return_20d": safe_float(latest["return_20d"], 2),
        },
        "backtest": backtest,
        "trade_plan": {
            "entry_low": round(entry_low, 4),
            "entry_high": round(entry_high, 4),
            "stop": round(stop, 4),
            "target": round(target, 4),
            "position_rule": "单次计划仓位不超过可用资金的25%；跌破止损线停止补仓。先扣除双边佣金和滑点，再判断是否值得做T。",
        },
        "tracking": {
            "latest_bar_date": str(latest["date"]),
            "latest_daily_close": round(latest_close, 4),
            "quote_vs_daily_close_pct": round(quote_delta, 3),
            "price_aligned": aligned,
            "price_source": "Eastmoney实时报价",
            "model_source": "Eastmoney复权日线",
            "note": "当前价用于下单区间校准，指标只使用已经完成的历史K线，减少实时价与日线收盘价错配。",
        },
        "chart": chart,
        "data_quality": {
            "bars": int(len(data)),
            "last_bar": str(latest["date"]),
            "history_ready": len(data) >= 90,
        },
        "disclaimer": "这是公开行情上的研究参考，不构成投资建议，也不能保证预测准确。历史胜率不代表未来收益；ETF仍有跳空、跟踪误差、流动性、手续费和滑点风险。",
    }


async def _fetch_inputs(info: dict[str, str]) -> tuple[dict[str, Any], pd.DataFrame]:
    quote_data, history = await asyncio.gather(fetch_quote(info), fetch_history(info))
    return quote_data, history


async def analyze_one(info: dict[str, str]) -> dict[str, Any]:
    quote_data, history = await _fetch_inputs(info)
    return build_analysis(info, quote_data, history)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


@app.get("/api/symbols")
async def symbols() -> list[dict[str, str]]:
    return [{"code": code, **value} for code, value in SYMBOLS.items()]


@app.get("/api/quote")
async def quote(symbol: str = Query("159915")) -> dict[str, Any]:
    info = symbol_info(symbol)
    return {"symbol": info, "quote": await fetch_quote(info)}


@app.get("/api/analyze")
async def analyze(symbol: str = Query("159915")) -> dict[str, Any]:
    return await analyze_one(symbol_info(symbol))


@app.get("/api/scan")
async def scan(limit: int = Query(13, ge=1, le=30)) -> dict[str, Any]:
    infos = list(SYMBOLS.items())[:limit]
    tasks = [analyze_one({"code": code, **meta}) for code, meta in infos]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for (code, _), result in zip(infos, results):
        if isinstance(result, Exception):
            errors.append({"code": code, "detail": str(result)})
            continue
        s = result["signal"]
        q = result["quote"]
        rows.append({
            "code": code,
            "name": result["symbol"]["name"],
            "group": result["symbol"].get("group", ""),
            "price": q.get("price"),
            "change_percent": q.get("change_percent"),
            "side": s["side"],
            "label": s["label"],
            "tone": s["tone"],
            "score": s["score"],
            "backtest_win_rate": s["backtest_win_rate"],
            "sample_trades": s["sample_trades"],
            "atr_pct": result["indicators"]["atr_pct"],
            "quote_vs_daily_close_pct": result["tracking"]["quote_vs_daily_close_pct"],
            "updated_at": s["updated_at"],
        })
    rows.sort(key=lambda row: (row["side"] == "BUY", row["score"]), reverse=True)
    return {
        "rows": rows,
        "errors": errors,
        "updated_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
        "method": "同一套评分规则 + t→t+1 步进回测 + 实时价校准",
    }


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")}
