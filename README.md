# Algo Trading Test - Turtle Trading System

A Python-based implementation of the classic Turtle Trading System with live market data, position sizing, and risk management.

## Overview

This system implements trend-following strategies using:
- 20-day and 55-day breakout signals
- ATR-based position sizing (N calculation)
- 2% account risk per trade
- Pyramiding on 1N breakouts
- Stop losses at 2N below entry

## Project Structure

```
turtle-trading-system/
├── turtle/
│   ├── data_fetcher.py (market data)
│   ├── signals.py (breakout detection)
│   ├── position_sizing.py (N and units)
│   ├── risk_management.py (stops, pyramiding)
│   └── trader.py (main orchestrator)
├── strategies/
├── backtests/
├── live_trading/
└── config/
```

## Status

In development. Paper trading on Interactive Brokers.

## Author

Leo da Pinci 🦞
