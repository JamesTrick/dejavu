from dejavu.portfolio import Portfolio
from dejavu.strategy.sizers import PositionSizer


class PercentRisk(PositionSizer):
    """Risk a fixed % of equity per trade.
    Requires a stop_distance kwarg (e.g. 1 ATR or a fixed $ stop).

    qty = (equity * risk_pct) / stop_distance
    """

    def __init__(self, risk_pct: float = 0.01):
        """

        Args:
            risk_pct: percentage of total portfolio equity to risk per trade (e.g. 0.01 for 1%)
        """
        self.risk_pct = risk_pct

    def size(self, symbol: str, price: float, portfolio: Portfolio, **kwargs) -> float:  # noqa: ARG002
        stop_distance = kwargs.get("stop_distance")
        if not stop_distance:
            raise ValueError("PercentRisk requires stop_distance kwarg")
        return (portfolio.equity * self.risk_pct) / stop_distance


class ATRBased(PositionSizer):
    """
    Uses ATR as the stop distance automatically.
    qty = (equity * risk_pct) / (atr * atr_multiplier)
    """

    def __init__(self, risk_pct: float = 0.01, atr_multiplier: float = 2.0):
        self.risk_pct = risk_pct
        self.atr_multiplier = atr_multiplier

    def size(self, symbol: str, price: float, portfolio: Portfolio, **kwargs) -> float:  # noqa: ARG002
        atr = kwargs.get("atr")
        if not atr:
            raise ValueError("ATRBased requires atr kwarg")
        stop_distance = atr * self.atr_multiplier
        return (portfolio.equity * self.risk_pct) / stop_distance


class KellyCriterion(PositionSizer):
    """
    f = (win_rate * avg_win - loss_rate * avg_loss) / avg_win
    Sizes position as a fraction of equity. Half-Kelly by default.
    """

    def __init__(
        self,
        win_rate: float,
        avg_win: float,
        avg_loss: float,
        fraction: float = 0.5,  # half-Kelly is more conservative
    ):
        self.fraction = fraction
        raw_kelly = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
        self.kelly_f = max(0.0, raw_kelly * fraction)  # never short via Kelly

    def size(self, symbol: str, price: float, portfolio: Portfolio, **kwargs) -> float:  # noqa: ARG002
        dollar_allocation = portfolio.equity * self.kelly_f
        return dollar_allocation / price
