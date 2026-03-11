from datetime import datetime

import pytest

from dejavu.execution.commission import PerContractCommission
from dejavu.execution.orders import CommissionOnlyHandler
from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    Instrument,
    MarketEvent,
    Order,
    OrderType,
)


@pytest.fixture
def instrument() -> Instrument:
    return Instrument(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
    )

@pytest.fixture
def commission_model() -> PerContractCommission:
    return PerContractCommission(rate=0.5)


def test_commission_only(commission_model: PerContractCommission, instrument: Instrument):
    executor = CommissionOnlyHandler(commission_model)
    order = Order(
        instrument=instrument,
        quantity=10,
        order_type=OrderType.MARKET,
    )
    me = MarketEvent(
        type=EventType.MARKET,
        timestamp=datetime.now(),
        instrument=instrument,
        open=10,
        close=20,
        low=9,
        high=21,
        volume=100,
    )
    fill_event = executor.execute(order=order, market=me)

    assert isinstance(fill_event, FillEvent)
    assert fill_event.fill_price == 20
    assert fill_event.commission == 0.5 * 10 # Cost per contract. Or should it be per order, regardless of quantity?
    assert fill_event.multiplier == 1
    assert fill_event.instrument.symbol == "SPY"
