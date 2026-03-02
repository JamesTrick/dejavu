import math
from datetime import datetime

import pytest

from dejavu.portfolio import Portfolio
from dejavu.schemas import FillEvent, AssetClass, MarketEvent, EventType


@pytest.fixture
def portfolio():
    return Portfolio(initial_capital=100_000)

def test_portfolio_initialization(portfolio):
    assert portfolio.initial_capital == 100_000
    assert portfolio.equity == 100_000
    assert portfolio.positions == {}
    assert len(portfolio.positions) == 0

def test_market_updates_updates_equity(portfolio):
    # Simulate a market update for an equity position
    portfolio.apply_fill(FillEvent(type=EventType.FILL, timestamp=datetime.now(), symbol="XYZ", quantity=100, fill_price=10.0, commission=0.0, multiplier=1.0), {"asset_class": AssetClass.EQUITY})
    close_price = 15.0

    portfolio.update_prices(MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        symbol="XYZ",
        open=close_price * .98,
        close=close_price,
        high=close_price * 1.1,
        low=close_price * .9,
        volume=1000,
        asset_class=AssetClass.EQUITY))

    assert portfolio.equity == 100_500.0

    close_price = 5.0
    portfolio.update_prices(MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        symbol="XYZ",
        open=close_price * .98,
        close=close_price,
        high=close_price * 1.1,
        low=close_price * .9,
        volume=1000,
        asset_class=AssetClass.EQUITY))

    assert portfolio.equity == 99_500.0


def test_position_closure(portfolio):
    portfolio.apply_fill(FillEvent(type=EventType.FILL, timestamp=datetime.now(), symbol="TSLA", quantity=100, fill_price=10.0, commission=0.0, multiplier=1), {"asset_class": AssetClass.EQUITY})
    portfolio.apply_fill(FillEvent(type=EventType.FILL, timestamp=datetime.now(), symbol="TSLA", quantity=-100, fill_price=10.0, commission=0.0, multiplier=1), {"asset_class": AssetClass.EQUITY})

    assert "TSLA" not in portfolio.positions
    assert portfolio.cash == 100_000.0
    assert portfolio.equity == 100_000.0
    assert portfolio._position_value == 0.0


def test_short_selling(portfolio):
    """Tests that negative quantities properly deduct from equity when prices rise."""
    # Short 10 shares at $100
    portfolio.apply_fill(FillEvent(type=EventType.FILL, timestamp=datetime.now(), symbol="SPY", quantity=-10, fill_price=100.0, commission=0.0, multiplier=1), {"asset_class": AssetClass.EQUITY})

    assert portfolio.cash == 101_000.0

    close_price = 110.0
    portfolio.update_prices(MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        symbol="SPY",
        open=close_price * .98,
        close=close_price,
        high=close_price * 1.1,
        low=close_price * .9,
        volume=1000,
        asset_class=AssetClass.EQUITY))
    # We owe 10 * 110 = 1100. Equity = 101_000 - 1100 = 99,900
    assert portfolio.equity == 99_900.0
