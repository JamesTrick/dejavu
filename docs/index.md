---
icon: lucide/rocket
---

# Dejavu
Dejavu is a high-performance Python backtesting and live trading framework.
It's fully typed, composable, and extensible.

Dejavu currently processes over 3.5 million bars/ticks per second, making it
suitable for high-frequency and large-scale backtesting workloads.

Taking inspiration from the typing philosophy of Pydantic and PydanticAI,
Dejavu is a modern, typed alternative to Backtrader — designed for IDE
completion, composability, and extensibility across a wide range of use cases.

!!! warning
    This package is under active development and the API is subject to change.
    Use with caution in production environments.

## Features

* Multi-asset support. Trade and backtest, Stocks, Cryptocurrencies, Forex, and Options all within a single strategy.
* Supports realistic [commission](brokers/commission.md) structures and slippage
* Supports Margin and cash transactions
* Supports Portfolio management and portfolio level rebalancing
* Support [position sizing](strategy/sizers.md) within a strategy.
