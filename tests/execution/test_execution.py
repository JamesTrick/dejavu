from datetime import datetime

import pytest

from dejavu.execution.commission import PerContractCommission
from dejavu.execution.orders import CommissionOnlyHandler
from dejavu.portfolio import Portfolio
from dejavu.schemas import (
    EventType,
    FillEvent,
    Instrument,
    MarketEvent,
    Order,
    OrderType,
)


@pytest.fixture
def commission_model() -> PerContractCommission:
    return PerContractCommission(rate=0.5)


def test_commission_only(
    commission_model: PerContractCommission,
    equity_instrument: Instrument,
    portfolio: Portfolio,
):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=equity_instrument,
        quantity=10,
        order_type=OrderType.MARKET,
    )
    me = MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        instrument=equity_instrument,
        open=10,
        close=20,
        low=9,
        high=21,
        volume=100,
    )
    fill_event = executor.execute(order=order, market=me, portfolio=portfolio)

    assert isinstance(fill_event, FillEvent)
    assert fill_event.fill_price == 20
    assert (
        fill_event.commission == 0.5 * 10
    )  # Cost per contract. Or should it be per order, regardless of quantity?
    assert fill_event.instrument.symbol == "SPY"


def test_sell_stop_order_fills_when_market_trades_at_stop(
    commission_model: PerContractCommission,
    equity_instrument: Instrument,
    market_event: MarketEvent,
    portfolio: Portfolio,
):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=equity_instrument,
        quantity=-10,
        order_type=OrderType.STOP,
        stop_price=176.0,
    )

    fill_event = executor.execute(order=order, market=market_event, portfolio=portfolio)

    assert isinstance(fill_event, FillEvent)
    assert fill_event.fill_price == 176.0
    assert fill_event.quantity == -10


def test_sell_stop_order_stays_pending_until_stop_is_hit(
    commission_model: PerContractCommission,
    equity_instrument: Instrument,
    portfolio: Portfolio,
):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=equity_instrument,
        quantity=-10,
        order_type=OrderType.STOP,
        stop_price=170.0,
    )
    me = MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        instrument=equity_instrument,
        open=180.0,
        close=182.0,
        low=175.0,
        high=185.0,
        volume=100,
    )

    fill_event = executor.execute(order=order, market=me, portfolio=portfolio)

    assert fill_event is None


def test_buy_stop_order_fills_when_market_trades_at_stop(
    commission_model: PerContractCommission,
    equity_instrument: Instrument,
    market_event: MarketEvent,
    portfolio: Portfolio,
):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=equity_instrument,
        quantity=10,
        order_type=OrderType.STOP,
        stop_price=184.0,
    )

    fill_event = executor.execute(order=order, market=market_event, portfolio=portfolio)

    assert isinstance(fill_event, FillEvent)
    assert fill_event.fill_price == 184.0
    assert fill_event.quantity == 10


def test_stop_order_missing_stop_price_does_not_fill(
    caplog: pytest.LogCaptureFixture,
    commission_model: PerContractCommission,
    equity_instrument: Instrument,
    market_event: MarketEvent,
    portfolio: Portfolio,
):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=equity_instrument,
        quantity=-10,
        order_type=OrderType.STOP,
    )

    fill_event = executor.execute(order=order, market=market_event, portfolio=portfolio)

    assert fill_event is None
    assert "Stop order missing price" in caplog.text
