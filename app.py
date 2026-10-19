from __future__ import annotations

import asyncio
import math
import re
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
app = FastAPI(title="ETF Quant T Advisor", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

SYMBOLS: dict[str, dict[str, str]] = {
    "510300": {"name": "沪深300ETF", "market": "1"},
    "510500": {"name": "中证500ETF", "market": "1"},
    "510880": {"name": "红利ETF", "market": "1"},
    "512100": {"name": "中证1000ETF", "market": "1"},
    "512480": {"name": "半导体ETF", "market": "1"},
    "512690": {"name": "酒ETF", "market": "1"},
    "588000": {"name": "科创50ETF", "market": "1"},
    "159915": {"name": "创业板ETF", "market": "0"},
    "159919": {"name": "沪深300ETF（深）", "market": "0"},
    "159949": {"name": "创业板50ETF", "market": "0"},
    "159865": {"name": "养殖ETF", "market": "0"},
    "159928": {"name": "消费ETF", "market": "0"},
    "159995": {"name": "芯片ETF", "market": "0"},
}

QUOTE_FIELDS = "f43,f57,f58,f169,f170,f46,f44,f45,f47,f48,f60,f71,f168,f171"
KLINE_FIELDS = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
CODE_RE = re.compile(r"^\d{6}$")


def symbol_info(symbol: str) -> dict[str, str]:
    symbol = symbol.strip()
    if not CODE_RE.fullmatch(symbol):
        raise HTTPException(status_code=400, detail="ETF代码应为6位数字，例如159915")
    known = SYMBOLS.get(symbol)
    if known:
        return {"code": symbol, **known}
    # 51/52/56/58 开头通常是沪市，15/16/18 开头通常是深市。
    market = "1" if symbol.startswith(("50", "51", "52", "56", "58")) else "0"
    return {"code": symbol, "name": f"ETF {symbol}", "market": market}


async def eastmoney_json(url: str) -> dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 ETF-Quant-T-Advisor/0.1",
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
    return payload


async def fetch_quote(info: dict[str, str]) -> dict[str, Any]:
    secid = f"{info['market']}.{info['code']}"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={secid}&fields={QUOTE_FIELDS}"
    )
    payload = await eastmoney_json(url)
    data = payload.get("data") or {}
    if not data:
        raise HTTPException(status_code=404, detail="找不到该ETF行情，请检查代码")

    def num(key: str, scale: float = 1.0) -> float | None:
        value = data.get(key)
        if value is None:
            return None
        try:
            return round(float(value) / scale, 6)
        except (TypeError, ValueError):
            return None

    return {
        "code": info["code"],
        "name": data.get("f58") or info["name"],
        "price": num("f43", 100),
        "pre_close": num("f60", 100),
        "change_percent": num("f170", 100),
        "change_amount": num("f169", 100),
        "open": num("f46", 100),
        "high": num("f44", 100),
        "low": num("f45", 100),
        "volume": num("f47"),
        "amount": num("f48"),
        "turnover": num("f168", 100),
        "timestamp": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
    }


async def fetch_history(info: dict[str, str], days: int = 180) -> pd.DataFrame:
    days = max(60, min(days, 500))
    secid = f"{info['market']}.{info['code']}"
    begin = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}&fields1=f1,f2,f3,f4&fields2={KLINE_FIELDS}"
        f"&klt=101&fqt=1&beg={begin}&end=20500101"
    )
    payload = await eastmoney_json(url)
    rows = (payload.get("data") or {}).get("klines") or []
    if len(rows) < 30:
        raise HTTPException(status_code=404, detail="历史行情不足，暂时无法计算信号")
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
    if len(parsed) < 30:
        raise HTTPException(status_code=502, detail="历史行情解析失败")
    return pd.DataFrame(parsed, columns=["date", "open", "high", "low", "close", "volume", "amount"])


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sma5"] = out["close"].rolling(5).mean()
    out["sma20"] = out["close"].rolling(20).mean()
    out["ema12"] = out["close"].ewm(span=12, adjust=False).mean()
    out["ema26"] = out["close"].ewm(span=26, adjust=False).mean()
    out["macd"] = out["ema12"] - out["ema26"]
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]
    delta = out["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, float("nan"))
    out["rsi14"] = (100 - (100 / (1 + rs))).fillna(50)
    prev_close = out["close"].shift(1)
    true_range = pd.concat(
        [out["high"] - out["low"], (out["high"] - prev_close).abs(), (out["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    out["atr14"] = true_range.rolling(14).mean()
    out["vol_ratio"] = out["volume"] / out["volume"].rolling(20).mean()
    out["return_5d"] = out["close"].pct_change(5) * 100
    return out


def safe_float(value: Any, digits: int = 4) -> float | None:
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None
        return round(result, digits)
    except (TypeError, ValueError):
        return None


def build_analysis(info: dict[str, str], quote: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    data = add_indicators(df)
    latest = data.iloc[-1]
    price = quote.get("price") or float(latest["close"])
    sma5 = safe_float(latest["sma5"])
    sma20 = safe_float(latest["sma20"])
    rsi = safe_float(latest["rsi14"], 2) or 50.0
    macd = safe_float(latest["macd"], 5) or 0.0
    macd_signal = safe_float(latest["macd_signal"], 5) or 0.0
    atr = safe_float(latest["atr14"], 4) or price * 0.015
    vol_ratio = safe_float(latest["vol_ratio"], 2) or 1.0
    return_5d = safe_float(latest["return_5d"], 2) or 0.0

    score = 0
    reasons: list[str] = []
    if sma5 and sma20 and sma5 > sma20:
        score += 1
        reasons.append("5日均线在20日均线上方")
    elif sma5 and sma20:
        score -= 1
        reasons.append("5日均线在20日线下方")
    if price > (sma20 or price):
        score += 1
        reasons.append("现价站在20日均线之上")
    else:
        score -= 1
        reasons.append("现价低于20日均线")
    if macd > macd_signal:
        score += 1
        reasons.append("MACD动能偏强")
    else:
        score -= 1
        reasons.append("MACD动能偏弱")
    if 45 <= rsi <= 68:
        score += 1
        reasons.append("RSI处于相对健康区间")
    elif rsi > 75:
        score -= 1
        reasons.append("RSI偏高，追涨风险增加")
    elif rsi < 30:
        reasons.append("RSI偏低，存在超跌反弹可能")
    if return_5d > 0:
        score += 1
        reasons.append("近5个交易日涨幅为正")
    else:
        score -= 1
        reasons.append("近5个交易日走势偏弱")
    if vol_ratio > 1.2:
        reasons.append("成交量高于20日均量")

    if score >= 4:
        label, tone, action = "偏多", "positive", "回踩后分批做T"
    elif score >= 2:
        label, tone, action = "震荡偏多", "caution", "等回踩，不追高"
    elif score <= -3:
        label, tone, action = "偏空", "negative", "降低仓位，反弹优先减仓"
    else:
        label, tone, action = "中性", "neutral", "观望，等待方向确认"

    strength = min(85, 52 + abs(score) * 7)
    entry_low = price - atr * 0.55
    entry_high = price - atr * 0.15
    stop = price - atr * 1.5
    target = price + atr * 1.8
    latest_rows = data.tail(60)
    chart = []
    for _, row in latest_rows.iterrows():
        chart.append(
            {
                "date": str(row["date"]),
                "close": safe_float(row["close"], 4),
                "sma20": safe_float(row["sma20"], 4),
            }
        )

    return {
        "symbol": info,
        "quote": quote,
        "signal": {
            "score": score,
            "max_score": 5,
            "label": label,
            "tone": tone,
            "action": action,
            "strength": strength,
            "reasons": reasons,
            "updated_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
        },
        "indicators": {
            "sma5": sma5,
            "sma20": sma20,
            "rsi14": rsi,
            "macd": macd,
            "macd_signal": macd_signal,
            "atr14": atr,
            "vol_ratio": vol_ratio,
            "return_5d": return_5d,
        },
        "trade_plan": {
            "entry_low": round(entry_low, 4),
            "entry_high": round(entry_high, 4),
            "stop": round(stop, 4),
            "target": round(target, 4),
            "position_rule": "单次计划仓位不超过可用资金的25%；若跌破止损线，停止补仓。",
        },
        "chart": chart,
        "disclaimer": "这是基于公开行情与技术指标的研究参考，不构成投资建议，也不能保证预测准确。ETF存在跳空、流动性、跟踪误差和交易成本风险。",
    }


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
    info = symbol_info(symbol)
    quote_data, history = await _fetch_inputs(info)
    return build_analysis(info, quote_data, history)


async def _fetch_inputs(info: dict[str, str]) -> tuple[dict[str, Any], pd.DataFrame]:
    quote_data, history = await asyncio.gather(fetch_quote(info), fetch_history(info))
    return quote_data, history


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")}
