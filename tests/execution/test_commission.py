import pytest

from dejavu.execution.commission import (
    PercentageOfNotionalCommission,
    PerContractCommission,
)
from dejavu.schemas import AssetClass, Instrument, Order, OrderType


@pytest.fixture
def btc() -> Instrument:
    return Instrument(
        symbol="BTC",
        asset_class=AssetClass.CRYPTO,
    )


def test_per_contract(btc: Instrument):
    commission = PerContractCommission(0.65)
    order = Order(
        instrument=btc,
        quantity=10,
        order_type=OrderType.MARKET,
    )
    commission = commission.calculate(order=order, fill_price=20, multiplier=1)

    assert isinstance(commission, float)
    assert commission == (0.65 * 10)


def test_per_order_value(btc: Instrument):
    commission = PercentageOfNotionalCommission(0.65)
    order = Order(
        instrument=btc,
        quantity=10,
        order_type=OrderType.MARKET,
    )
    commission = commission.calculate(order=order, fill_price=20, multiplier=1)

    assert isinstance(commission, float)
    assert commission == (0.65 * (10 * 20))
