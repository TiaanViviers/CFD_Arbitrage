# nv-capital-ventures/production

**Production Algorithmic Trading Systems**  
This repository contains all *live* and *production-ready* algorithmic trading projects developed and deployed by NV Capital Ventures.

> All code in this repo has been thoroughly tested and battle-hardened through research and simulation.  
> Only strategies and systems that have passed our research pipeline are promoted to production for real capital deployment.

---

## Projects

### 1. `cfd_arb` — CFD Broker Arbitrage System

- **Description:**  
  Arbitrage engine for exploiting pricing inefficiencies between multiple MetaTrader 5 CFD brokers. Automatically finds, executes, and manages arbitrage opportunities, with built-in risk controls and real-time monitoring.
- **Key Features:**
    - Parallel broker management using multiprocessing for true speed and fault tolerance
    - Automatic detection of live arbitrage and “loss insurance” trades (LIM)
    - Production-grade error handling and robust logging
    - Telegram notification integration for real-time status and daily reporting
    - Modular configuration and codebase for rapid extension and new asset support
- **Supported Assets:**
    - Bitcoin / US dollar  (BTCUSD)
    - German 40            (Ger40)
    - Japan 225            (JP225)
    - Dow Jones Industrial (US30)
    - Nasdaq 100           (US100)
- **Supported Brokers:**
    - IC Markets
    - Exness
    - FXTM
    - Eightcap
    - XM

---

*New projects will be listed here as they graduate from the research pipeline.*

---
