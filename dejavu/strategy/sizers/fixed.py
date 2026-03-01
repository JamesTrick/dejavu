from dejavu.strategy.sizers import PositionSizer


class FixedUnits(PositionSizer):
    """Always trade N units. Simplest possible sizer."""
    def __init__(self, units: float = 100):
        self.units = units

    def size(self, symbol, price, portfolio, **kwargs) -> float:
        return self.units


class FixedDollar(PositionSizer):
    """Allocate a fixed dollar amount per trade."""
    def __init__(self, dollar_amount: float = 5_000):
        self.dollar_amount = dollar_amount

    def size(self, symbol, price, portfolio, **kwargs) -> float:
        return self.dollar_amount / price
