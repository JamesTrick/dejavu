from datetime import datetime

import pytest

from dejavu.portfolio import Portfolio
from dejavu.schemas import (
    AssetClass,
    EventType,
    Instrument,
    MarketEvent,
    Order,
    OrderType,
)


@pytest.fixture
def equity_instrument() -> Instrument:
    return Instrument(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
    )


@pytest.fixture
def market_event(equity_instrument) -> MarketEvent:
    return MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime(2024, 1, 2),
        instrument=equity_instrument,
        open=180.0,
        high=185.0,
        low=175.0,
        close=182.0,
        volume=1_000_000,
    )


@pytest.fixture
def buy_order(equity_instrument) -> Order:
    return Order(
        instrument=equity_instrument,
        quantity=100,
        order_type=OrderType.MARKET,
    )

@pytest.fixture
def sell_order(equity_instrument) -> Order:
    return Order(
        instrument=equity_instrument,
        quantity=-100,
        order_type=OrderType.MARKET,
    )

@pytest.fixture
def limit_buy_order(equity_instrument) -> Order:
    return Order(
        instrument=equity_instrument,
        quantity=100,
        order_type=OrderType.LIMIT,
        limit_price=176.0,  # below market low of 175 — should fill
    )

@pytest.fixture
def portfolio() -> Portfolio:
    return Portfolio(initial_capital=100_000)
