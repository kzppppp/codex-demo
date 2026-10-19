# ETF Quant T Advisor

一个面向 A 股 ETF 的轻量化量化参考工具：输入 ETF 代码后，自动读取公开行情，计算均线、MACD、RSI、ATR、量比和近 5 日动量，输出可解释的方向信号与做 T 风险区间。

## 功能

- 支持常见 ETF 代码，也可以输入其他 6 位 ETF 代码。
- 通过 Eastmoney 公开行情接口读取实时行情和日线数据。
- 每 20 秒刷新一次价格；页面可直接在电脑和手机浏览器打开。
- 用 5 日/20 日均线、MACD、RSI、成交量和动量计算一个简单评分。
- 给出回踩参考区、止损线、止盈线和仓位上限，便于人工复核。
- 不需要数据库、账号或 API Key，适合作为个人研究的第一版。

## 本地运行

```bash
python -m venv .venv
# Windows PowerShell
.venv\\Scripts\\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
uvicorn app:app --reload
```

打开 <http://127.0.0.1:8000>，输入 ETF 代码即可使用。

## 设计说明

- `app.py`：FastAPI 后端、行情代理、指标计算与信号生成。
- `static/index.html`：无需构建工具的响应式前端。
- 信号只使用公开行情和技术指标，不包含新闻、资金流、盘口或基本面因子。
- 实盘接入前应增加行情缓存、备用数据源、请求限流、日志、回测和交易成本模型。

## 风险边界

“预测涨跌”和“精准做 T”无法由程序保证。实时接口可能延迟、限流或短暂不可用；ETF 还存在跳空、流动性、跟踪误差、手续费和滑点风险。页面结果只作为研究参考，不能替代自己的交易决策，也不会自动下单。
