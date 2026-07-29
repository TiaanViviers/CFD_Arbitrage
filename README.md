# CFD Arbitrage Trading System

A production-focused system for identifying and trading arbitrage opportunities between multiple CFD brokers via MetaTrader 5 (MT5).  

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Bot](#running-the-bot)
- [Key Concepts](#key-concepts)

---

## Overview

This system monitors real-time price quotes from several CFD brokers, identifies arbitrage opportunities, and automatically executes and manages trades on both legs to capture risk-free profit.  
It features robust error handling, risk controls, Telegram notifications, and a modular architecture for easy extension or modification.

---

## Project Structure

```
├── src/
| ├── main.py               # Entrypoint: launches master process & workers
| ├── master.py             # Core logic for arbitrage orchestration
| ├── worker.py             # MT5 worker process for trade execution, one per broker
| ├── mt5_broker.py         # Wrapper around MetaTrader 5 broker communication
| ├── trade.py              # Trade dataclass for trade info/state
| ├── lim.py                # ("loss injection module") trade logic
| ├── telebot.py            # Minimal Telegram bot for reporting
| ├── io_utils.py           # Config/data I/O utilities (JSON, YAML, CSV)
| ├── trading_schedules.py  # Asset trading time schedule logic
|
├── config/
| ├── asset_config.yml              # per asset runtime configuration
| ├── broker_config.example.json    # template: MT5 paths + symbol map
| ├── broker_config.json            # local only (gitignored)
| ├── fomc.txt                      # high-impact news dates
├── .env.example                    # Telegram credential placeholders
├── .env                            # local only (gitignored)
├── data/
| ├── *{asset}.csv                  # Trade records for future analysis
└── requirements.txt                # Project dependencies
```

---

## How It Works

- **Master process (`main.py`, `master.py`):**
    - Initializes worker processes for each broker.
    - Continuously collects tick data and balances from all brokers.
    - Detects arbitrage and LIM opportunities, triggers trade execution via workers.
    - Monitors open trades and closes them when profit/exit criteria are met.
    - Handles state synchronization and rogue trade cleanup.
    - Sends summary and alert notifications via Telegram.

- **Worker process (`worker.py`):**
    - Interfaces with MT5 for all trade actions (open, close, query positions).
    - Handles its own broker session, risk/timeouts, and robust error handling.

- **Configs (`config/`):**
    - `broker_config.json`: Contains all broker details (name, MT5 path, available symbols).
    - `asset_config.yml`: Asset-level parameters (thresholds, min lot, etc).

---

## Installation

1. **Clone this repo** and ensure you are using Python 3.10 or later.
2. **Install requirements**:

    ```
    pip install -r requirements.txt
    ```
3. **Operating System** At the time of this version release the mt5 terminals are only 
supported on windows. Linux might be supported in the future.

4. **Set up your MetaTrader 5 terminals** for each broker (see config files for paths).

5. **Local secrets / machine config** (required before running):

    ```
    cp .env.example .env
    cp config/broker_config.example.json config/broker_config.json
    ```

    Then edit `.env` with your Telegram credentials and `broker_config.json` with
    your local MT5 `terminal64.exe` paths. Neither file should be committed.

---

## Configuration

- `config/asset_config.yml` — shared asset parameters (thresholds, lots, etc.)
- `config/broker_config.example.json` — template for broker/symbol mapping
- `config/broker_config.json` — your local copy with real MT5 paths (gitignored)
- `.env` — `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` (gitignored)

Customize these files as needed for your environment.

---

## Running the Bot
Run from the project src/ directory:

    ```
    python main.py --asset {asset_name}
    ```
**Supported assets**: BTCUSD, US30, US100, GER40, JP225

**Supported brokers**: IC-markets, Exness, FXTM, Eightcap, XM

---

## Key Concepts
####  - Arbitrage Opportunity:
When bid on one broker > ask on another by at least the entry threshold, system opens simultaneous trades to capture profit.

####  - LIM Trades:
"Loss Injection module" trades for brokers with strong recent win rates; executed more conservatively. To remain under brokers' "arbitrage detection" radars and appear more like day traders.

####  - Resilience:
Brokers can be automatically quarantined (timeout) after repeated trade failures, protecting against market closures or technical downtime. All errors are caught and handled in place or higher up the stack. Various safety precautions have been taken to assure we do not risk single leg exposure, rogue positions, execute trades outside of market open hours, and even if we do, there is logic to mitigate the damage.

####  - Notifications:
Telegram bot integration for real-time alerts and daily summaries.

####  - Parallel Design:
The mt5 Python api only supports one connection per process. Thus there is a worker processes for each broker terminal we wish to connect with through the mt5 api. The master and workers communicate through 2 queues, the command queue(master -> worker) and the response queue(worker -> master). The master accumulates data from all the workers, decides when to take action, then informs the workers what action to execute.

---

