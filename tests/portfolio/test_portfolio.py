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


def test_option_expiration_rust(portfolio):
    """Option position expires at expiry date; intrinsic is paid to cash. Uses Rust backend."""
    from datetime import datetime as dt
    expiry = dt(2024, 6, 21)
    # Short 1 call, strike 100, underlying AAPL
    portfolio.apply_fill(
        FillEvent(
            type=EventType.FILL,
            timestamp=dt(2024, 6, 1),
            symbol="AAPL240621C100",
            quantity=-1.0,
            fill_price=5.0,
            commission=0.0,
            multiplier=100.0,
        ),
        {
            "asset_class": AssetClass.OPTION,
            "underlying": "AAPL",
            "strike": 100.0,
            "expiry": expiry,
            "option_type": "C",
        },
    )
    # Set underlying price to 105 so call intrinsic = 5 * 100 = 500
    portfolio.update_prices(
        MarketEvent(
            type=EventType.MARKET,
            timestamp=dt(2024, 6, 20),
            symbol="AAPL",
            open=104.0,
            high=106.0,
            low=104.0,
            close=105.0,
            volume=1000,
            asset_class=AssetClass.EQUITY,
        )
    )
    assert "AAPL240621C100" in portfolio.positions
    cash_before = portfolio.cash
    # Event on expiry day with OPTION asset_class triggers expiration
    portfolio.update_prices(
        MarketEvent(
            type=EventType.MARKET,
            timestamp=dt(2024, 6, 21, 12, 0, 0),
            symbol="AAPL240621C100",
            open=5.0,
            high=5.0,
            low=5.0,
            close=5.0,
            volume=0,
            asset_class=AssetClass.OPTION,
        )
    )
    assert "AAPL240621C100" not in portfolio.positions
    # Short call: we pay intrinsic to the holder. Intrinsic = (105 - 100) * 100 = 500. Cash decreases by 500.
    assert portfolio.cash == cash_before - 500.0


def test_underlying_view_and_margin_with_rust_portfolio(portfolio):
    """underlying_view() and RealisticRegTModel.calculate_used_margin work with Rust-backed portfolio."""
    from dejavu.execution.margin import RealisticRegTModel

    # Short 1 put, no equity
    portfolio.apply_fill(
        FillEvent(
            type=EventType.FILL,
            timestamp=datetime.now(),
            symbol="SPY_PUT_400",
            quantity=-1.0,
            fill_price=10.0,
            commission=0.0,
            multiplier=100.0,
        ),
        {
            "asset_class": AssetClass.OPTION,
            "underlying": "SPY",
            "strike": 400.0,
            "expiry": datetime(2025, 12, 19),
            "option_type": "P",
        },
    )
    portfolio.update_prices(
        MarketEvent(
            type=EventType.MARKET,
            timestamp=datetime.now(),
            symbol="SPY",
            open=450.0,
            high=451.0,
            low=449.0,
            close=450.0,
            volume=1_000_000,
            asset_class=AssetClass.EQUITY,
        )
    )
    view = portfolio.underlying_view()
    assert "SPY" in view
    assert view["SPY"]["option_symbols"] == ["SPY_PUT_400"]
    margin = RealisticRegTModel().calculate_used_margin(portfolio)
    assert margin > 0
