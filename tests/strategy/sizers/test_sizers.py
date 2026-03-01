import pytest

from dejavu.portfolio import Portfolio
from dejavu.strategy.sizers import FixedDollar, FixedUnits, PercentRisk


@pytest.fixture
def portfolio() -> Portfolio:
    return Portfolio(
        initial_capital=100_000
    )


def test_fixed_units(portfolio: Portfolio):
    sizer = FixedUnits(units=50)
    size = sizer.size(symbol="TEST", price=200, portfolio=portfolio)
    assert size == 50


def test_fixed_dollar(portfolio: Portfolio):
    sizer = FixedDollar(dollar_amount=200)
    size = sizer.size(symbol="TEST", price=50, portfolio=portfolio)
    assert size == 4

def test_risk_based(portfolio: Portfolio):
    # PercentRisk with 1% risk and a $10 stop distance
    percent_sizer = PercentRisk(risk_pct=0.01)
    size = percent_sizer.size(symbol="TEST", price=100, portfolio=portfolio, stop_distance=10)
    assert size == 100  # (100,000 * 0.01) / 10

