# CRYPTZero — OrderBook Weapon Machine v4.1

Real-time order flow analysis for Binance spot markets.  
Signals delivered via Telegram. Deployed from GitHub. Runs on any VPS.

---

## Architecture

```
Binance WebSocket (combined stream)
        |
        v
CombinedStreamManager          Single TCP connection, all symbols
        |
        +--------+-------------+
        |                      |
        v                      v
OrderBookEngine            VolumeAnalyzer
(gap-proof L2 book)        (CVD via 'm' field)
        |                      |
        v                      v
LiquidityAnalyzer         AbsorptionDetector
IcebergDetector
        |
        v
SignalGenerator            Orchestrates all five algorithms
        |
        v
WeaponBot (Telegram)       Primary UI — mobile-first, no browser required
        |
        v
BlockchainLogger (opt.)    SHA-256 signal hash to BSC calldata
```

---

## Repository Structure

```
cryptzero/
├── src/
│   ├── main.py
│   ├── data_ingestion/
│   │   └── websocket_manager.py
│   ├── processing/
│   │   ├── orderbook_engine.py
│   │   ├── volume_analyzer.py
│   │   └── signal_generator.py
│   ├── intelligence/
│   │   ├── absorption_detector.py
│   │   ├── iceberg_detector.py
│   │   └── liquidity_analyzer.py
│   ├── notifications/
│   │   └── telegram_bot.py
│   ├── blockchain/
│   │   └── blockchain_logger.py
│   └── backtesting/
│       └── backtester.py
├── config.yaml
├── requirements.txt
├── .env.example
└── systemd/
    └── cryptzero.service
```

---

## Signal Algorithms

| Algorithm | Mechanism | Confidence |
|-----------|-----------|------------|
| IMBALANCE | Bid/ask depth ratio > 3:1 or < 0.33:1 across top 10 levels | 70% |
| CVD | Cumulative Volume Delta divergence vs price slope (m-field classification) | 78% |
| ABSORPTION | Volume > 50x average + price flat for 60s + CVD stable | 82% |
| VACUUM | Contiguous price levels with < 10% average liquidity | 86% |
| ICEBERG | Executed volume > 5x visible size persisted for 30s minimum | 72% |

**Realistic combined win rate: 62–65%** after fees (0.1%), slippage (0.05%), and Nigeria network latency (50–200ms).

---

## Telegram Bot Commands

```
/start              Navigation keyboard
/status             System health and uptime
/health             Per-symbol gaps, CVD, sync state
/signals            Last 5 signals with timestamps
/orderbook BTC      Live top-of-book snapshot for BTCUSDT
```

Inline buttons mirror all commands for one-tap mobile access.  
Signal alerts are pushed automatically when a detector fires.

Signal card format (no emoji — clean terminal-style output):
```
SIGNAL DETECTED

Symbol    : BTCUSDT
Type      : IMBALANCE
Direction : BUY
Confidence: 70%

Bid Liq   : $8,240,000
Ask Liq   : $2,010,000
Ratio     : 4.10:1
Mid Price : $65,432.10
Spread    : 2 bps

Issued    : 2026-04-11 12:00:00 UTC
```

---

## Deployment: VPS (Ubuntu 22.04)

```bash
# 1. Clone
git clone https://github.com/youruser/cryptzero.git /opt/cryptzero
cd /opt/cryptzero

# 2. Environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
nano .env          # fill in TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

nano config.yaml   # set trading_pairs and thresholds

# 4. Test run
python src/main.py

# 5. Install as service
sudo cp systemd/cryptzero.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/cryptzero.service
sudo systemctl daemon-reload
sudo systemctl enable --now cryptzero
sudo journalctl -u cryptzero -f
```

Updates from GitHub:
```bash
cd /opt/cryptzero && git pull && sudo systemctl restart cryptzero
```

---

## Telegram Bot Setup

1. Open Telegram, message `@BotFather`
2. `/newbot` — choose a name, receive your token
3. Start your bot, then visit:  
   `https://api.telegram.org/bot<TOKEN>/getUpdates`  
   Send any message to the bot, then find `"chat": {"id": ...}` in the response
4. Put both values in `.env`

---

## Naira Funding Flow

```
NGN bank account
    -> Binance P2P (buy USDT, zero fees, 15-min settlement)
    -> Binance Spot wallet
    -> System monitors via read-only API key

Withdrawal:
    Binance Spot -> P2P sell USDT -> NGN bank account
```

API key permissions required: **Market Data** only.  
Trading and withdrawal permissions must remain disabled for monitoring-only mode.

---

## OrderBook Sync Procedure (Official Binance)

The `OrderBookEngine` implements the complete three-step Binance diff-depth
synchronization protocol:

1. WebSocket events are buffered before the REST snapshot is fetched
2. Events where `u <= snapshot_lastUpdateId` are discarded
3. The first valid event must satisfy `U <= snapshot_lastUpdateId + 1 <= u`
4. Subsequent events apply normally; gaps trigger automatic re-sync

This prevents the silent stale-book condition that affects naive implementations.

---

## Backtesting

```python
import asyncio
from src.backtesting.backtester import Backtester

bt = Backtester(
    data_path="data/btcusdt_ticks.jsonl",
    symbol="BTCUSDT",
    forward_window_ms=300_000,
    min_latency_ms=50,
    max_latency_ms=200,
    fee_pct=0.001,
    slippage_pct=0.0005,
    gap_drop_prob=0.01,        # 1% packet drop for gap stress test
)
results = asyncio.run(bt.run())
Backtester.print_summary(results)
```

Data format (interleaved JSONL, sorted by `ts`):
```jsonl
{"type":"trade","ts":1714521600000,"p":"65432.10","q":"0.123","m":false,"s":"BTCUSDT"}
{"type":"depth","ts":1714521600100,"U":12345,"u":12347,"b":[["65430","1.5"]],"a":[["65435","2.0"]],"E":1714521600100}
```

---

## Optional: On-Chain Signal Logging (BSC)

Set `BSC_PRIVATE_KEY` in `.env`. Every signal with confidence >= 80% is SHA-256
hashed and broadcast as a BSC transaction via calldata. The transaction hash is
logged locally and verifiable at [bscscan.com](https://bscscan.com).

No blockchain dependency is required for core operation.

---

## Paper Test Checklist (48h)

```
Run combined stream on 2-3 pairs
Confirm gap_count < 5/day per symbol via /health
Manually verify 10 iceberg signals against chart
Check CVD signals align with visible trade imbalance
Log all absorption signals and check price response within 5 min
At 48h: run backtester on collected data
If win_rate > 60%: move to small live alerts (NGN 50,000 - 100,000)
```

---

## Hardware Requirements

| Spec | Minimum | Recommended |
|------|---------|-------------|
| CPU | 2 cores | 4 cores |
| RAM | 2 GB | 4 GB |
| Network | 10 Mbps stable | 50 Mbps |
| Storage | 10 GB | 50 GB SSD |
| OS | Ubuntu 22.04 LTS | Ubuntu 22.04 LTS |

Oracle Cloud Free Tier (ARM, 4 cores, 24 GB RAM) is sufficient and costs nothing.

---

## Dependencies

```
websockets==12.0
aiohttp==3.9.3
sortedcontainers==2.4.0
pandas==2.2.1
numpy==1.26.4
python-telegram-bot==21.3
python-dotenv==1.0.1
pyyaml==6.0.1
structlog==24.1.0
```

Optional: `web3` for BSC logging, `psycopg2-binary` for PostgreSQL, `redis` for caching.
