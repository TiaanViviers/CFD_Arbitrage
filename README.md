# CFD Arbitrage

A multi-broker CFD arbitrage engine built on MetaTrader 5.

The system watches the same instrument across several retail CFD brokers in parallel, detects short-lived cross-broker price divergences, and opens hedged two-leg trades to capture the gap as prices mean-revert.

This is a **systems / production trading project**, not a research notebook: process isolation around the MT5 API, risk-aware sizing, position reconciliation, session gates, and operational alerting.

---

## Why this exists

Retail CFD brokers often quote the “same” crypto or index at slightly different prices because of feed latency, liquidity, and dealing-desk behaviour. Those gaps are usually brief.

Capturing them live is mostly an engineering problem:

- MT5 allows **one terminal connection per Python process**
- Symbol names and lot economics differ per broker
- Two-leg fills are **not atomic** across firms
- Orphan legs, rogue positions, and session gaps can dominate P&L

This repo is an end-to-end runtime for that problem.

---

## Features

- **Cross-broker divergence detection** on live bid/ask ticks
- **Master / worker multiprocessing** — one MT5 worker per broker
- **Hedged two-leg execution** with orphan rollback if either leg fails
- **Risk-aware lot sizing** from capital allocation, free margin, and worst-case divergence
- **Value-per-point normalisation** so “1 lot” is economically comparable across brokers
- **Position sync + rogue cleanup** — broker state is treated as source of truth
- **Broker quarantine** after failed opens
- **Trading schedules + FOMC blackout days**
- **Telegram alerts** for opens, fails, closes, and daily summaries (optional via `.env`)
- **CSV trade logging** on shutdown

---

## Architecture

```
main.py
  └─ spawns one worker process per broker (MT5 terminal)
  └─ runs master loop

master  ──cmd queue──►  worker (IC Markets)
      ◄─resp queue──
      ──cmd queue──►  worker (Exness)
      ◄─resp queue──
      ──cmd queue──►  worker (FXTM / XM / …)
```

Each loop iteration (while the market is open):

1. Collect ticks, balances, and max lots from all workers  
2. Detect bid/ask divergences above the asset entry threshold  
3. Open at most **one** trade pair per cycle (fresh prices beat greed)  
4. Update combined floating P&L on open pairs  
5. Close when mean-reverted P&L clears threshold + slippage buffer  
6. Reconcile in-memory trades with live broker positions  
7. Force-close any untracked (“rogue”) positions  

---

## Supported assets & brokers

| Assets | Brokers (configured) |
|---|---|
| `BTCUSD`, `US30`, `US100`, `GER40`, `JP225` | IC Markets, Exness, FXTM, XM |

Broker symbol aliases (e.g. `USTEC` vs `NAS100` vs `US100Cash`) are mapped in config.

> **Platform:** MetaTrader 5 terminals are Windows-only in this setup. Run the bot on Windows (or a Windows VM) with each broker terminal installed and logged in.

---

## Project structure

```
├── src/
│   ├── main.py               # CLI entrypoint, process bootstrap
│   ├── master.py             # Arbitrage orchestration loop
│   ├── worker.py             # Per-broker command loop
│   ├── mt5_broker.py         # MetaTrader 5 API wrapper
│   ├── trade.py              # Trade dataclass
│   ├── telebot.py            # Telegram notifications
│   ├── io_utils.py           # Config + CSV I/O
│   ├── trading_schedules.py  # UTC session windows
│   └── lim.py                # Legacy loss-injection module (not wired into the live loop)
│
├── config/
│   ├── asset_config.yml              # Thresholds, sizing, slip
│   ├── broker_config.example.json    # Public template (placeholder paths)
│   ├── broker_config.json            # Local only — real MT5 paths (gitignored)
│   └── fomc.txt                      # High-impact news dates
│
├── .env.example              # Telegram credential placeholders
├── .env                      # Local only (gitignored)
├── requirements.txt
└── README.md
```

---

## Quick start

### 1. Clone & install

```bash
git clone https://github.com/TiaanViviers/CFD_Arbitrage.git
cd CFD_Arbitrage
pip install -r requirements.txt
```

Requires **Python 3.10+**.

### 2. Local config (never commit these)

```bash
cp .env.example .env
cp config/broker_config.example.json config/broker_config.json
```

Edit:

- `.env` — `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` (optional; alerts disable cleanly if unset)
- `config/broker_config.json` — your local `terminal64.exe` paths per broker

### 3. Run

From `src/`:

```bash
python main.py --asset BTCUSD
```

---

## Configuration

### Asset parameters (`config/asset_config.yml`)

| Key | Meaning |
|---|---|
| `entry_threshold` | Minimum cross-broker divergence to open |
| `buffer` | Extra P&L cushion required before exit (slippage) |
| `max_divergence` | Worst-case gap used for lot / drawdown sizing |
| `capital_allocation` | Fraction of broker balance available for this asset |
| `min_lot` | Minimum lot size after flooring |
| `allowed_slip` | Max price deviation sent to MT5 as order deviation |

Set `capital_allocation: 0.0` to soft-disable an asset without removing it from config.

### Broker mapping (`config/broker_config.json`)

Maps each broker to:

- local MT5 terminal path  
- internal symbol → broker-specific symbol  
- instrument type (`crypto`, `index.us`, `index.eu`, `index.as`)

### Schedules & news

- `trading_schedules.py` — UTC windows that avoid broker daily maintenance gaps  
- `fomc.txt` — dates when the master idles instead of trading  

---

## Trading logic (short version)

**Entry**

\[
\text{divergence} = \text{bid}_{\text{sell broker}} - \text{ask}_{\text{buy broker}}
\]

Open a sell on the rich broker and a buy on the cheap broker when divergence ≥ `entry_threshold`, subject to per-broker open-trade limits and lot floors.

**Exit**

Not gap-based. Exit when combined floating P&L exceeds roughly:

`entry_threshold × lot + buffer × lot`

after a minimum hold time (~200s).

**Safety**

- Shared `arb_id` / MT5 magic number pairs both legs  
- Failed pair open → immediately close any orphan leg  
- Missing counter-leg on sync → mark `pending_close` and retry  
- Unknown live positions → force close  

---

## Tech stack

- Python 3.10+  
- [MetaTrader5](https://www.mql5.com/en/docs/python_metatrader5)  
- `multiprocessing` queues for master ↔ worker IPC  
- pandas / numpy for tick matrices  
- PyYAML for asset config  
- requests + optional Telegram Bot API  
- python-dotenv for secrets  

---

## Disclaimer

This software is provided for **educational and portfolio purposes**. Cross-broker CFD trading involves real financial risk: execution risk, slippage, rejects, gap risk, and broker-specific rules. It is **not** risk-free. Use at your own risk; the author assumes no liability for losses.

Also respect each broker’s terms of service. Automated strategies may be restricted on some accounts.

---

## License

No license file is attached yet. If you fork or reuse pieces, ask or treat the code as source-available for learning unless a license is added later.
