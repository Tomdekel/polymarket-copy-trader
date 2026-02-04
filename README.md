# Polymarket Copy Trading Bot

Automatically track and copy @anoin123's trades on Polymarket with proportional position sizing.

## Features
- Real-time wallet tracking via Polymarket API
- Proportional position sizing (maintains same % of portfolio)
- Automated P&L tracking and reporting
- Risk management (max position limits, stop-loss)
- SQLite database for trade history

## Quick Start
```bash
cd polymarket-copy-trader
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python copy_trader.py --wallet anoin123 --budget 10000
```

## Configuration
Edit `config.yaml` to customize:
- Position sizing strategy
- Risk limits
- Markets to exclude
- Logging level

## Architecture
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Polymarket API │────▶│  Wallet Tracker  │────▶│  Position Sizer │
│   (Gamma API)   │     │  (anoin123)      │     │  ($10K budget)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                     │
                                                     ▼
                              ┌──────────────────────────────────────┐
                              │         Copy Trading Engine          │
                              │   - Entry/Exit Detection            │
                              │   - Size Calculation                │
                              │   - Risk Management                 │
                              └──────────────────────────────────────┘
                                                     │
                        ┌──────────────────────────┼──────────────────────────┐
                        ▼                          ▼                          ▼
                 ┌──────────────┐         ┌──────────────┐          ┌──────────────┐
                 │   SQLite DB  │         │   Console    │          │   Alerts     │
                 │  (trades.db) │         │   Output     │          │  (optional)  │
                 └──────────────┘         └──────────────┘          └──────────────┘
```
