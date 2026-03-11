from dejavu.strategy.sizers import PositionSizer


class FixedUnits(PositionSizer):
    """This is the most basic position sizer, and it simply allocates a fixed number of units per trade, regardless
    of price or portfolio size.
    """
    def __init__(self, units: float = 100):
        """

        Args:
            units: Number of units to trade. Defaults to 100.
        """
        self.units = units

    def size(self, symbol, price, portfolio, **kwargs) -> float:
        return self.units


class FixedDollar(PositionSizer):
    """Similar to `FixedUnits`, this is a basic position sizer, and it simply allocates how much capital (money) to
     allocate to a single trade.
    """
    def __init__(self, dollar_amount: float = 5_000):
        """

        Args:
            dollar_amount: Number of dollars to allocate to the trade. Defaults to $5,000.
        """
        self.dollar_amount = dollar_amount

    def size(self, symbol, price, portfolio, **kwargs) -> float:
        return self.dollar_amount / price
