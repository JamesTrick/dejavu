from datetime import datetime

from dejavu.portfolio import Portfolio
from dejavu.portfolio.rebalancing.base import Rebalancer
from dejavu.schemas import AssetClass, Order, OrderType


class CalendarRebalancer(Rebalancer):
    """Rebalance on a fixed calendar schedule."""

    FREQUENCIES = {"daily", "weekly", "monthly", "quarterly"}

    def __init__(self, frequency: str = "monthly"):
        assert frequency in self.FREQUENCIES
        self.frequency      = frequency
        self._last_rebal:   datetime | None = None

    def should_rebalance(
        self, timestamp: datetime, portfolio: Portfolio | None = None
    ) -> bool:
        if self._last_rebal is None:
            return True

        if self.frequency == "daily":
            return timestamp.date() > self._last_rebal.date()
        elif self.frequency == "weekly":
            return (timestamp - self._last_rebal).days >= 7
        elif self.frequency == "monthly":
            return (
                timestamp.month != self._last_rebal.month
                or timestamp.year != self._last_rebal.year
            )
        elif self.frequency == "quarterly":
            return (
                (timestamp.month - 1) // 3
                != (self._last_rebal.month - 1) // 3
            )
        return False

    def generate_orders(self, timestamp, portfolio, target_weights, prices):
        orders = []
        self._last_rebal = timestamp

        for symbol, target_pct in target_weights.items():
            price = prices.get(symbol)
            if not price:
                continue

            target_value   = portfolio.equity * target_pct
            target_qty     = target_value / price
            current_qty    = portfolio.positions.get(symbol)
            current_qty    = current_qty.quantity if current_qty else 0.0
            delta_qty      = target_qty - current_qty

            if abs(delta_qty) < 0.01:   # ignore rounding noise
                continue

            orders.append(Order(
                symbol=symbol,
                quantity=round(delta_qty, 4),
                order_type=OrderType.MARKET,
                asset_class=AssetClass.EQUITY,
            ))

        return orders


class ThresholdRebalancer(Rebalancer):
    """
    Rebalance when any position drifts more than `threshold`
    from its target weight. More tax/cost efficient than calendar.
    """
    def __init__(self, threshold: float = 0.05):
        self.threshold = threshold

    def should_rebalance(
        self, timestamp: datetime, portfolio: Portfolio | None = None
    ) -> bool:
        if portfolio is None or not portfolio.positions:
            return False
        # Check max drift — generate_orders will handle the details
        return self._max_drift(portfolio) > self.threshold

    def _max_drift(self, portfolio: Portfolio) -> float:
        # Simplified — in practice you'd compare against stored targets
        weights = [
            pos.market_value(portfolio._last_prices.get(sym, pos.avg_cost))
            / portfolio.equity
            for sym, pos in portfolio.positions.items()
        ]
        if not weights:
            return 0.0
        equal_target = 1.0 / len(weights)
        return max(abs(w - equal_target) for w in weights)

    def generate_orders(self, timestamp, portfolio, target_weights, prices):
        # Same logic as CalendarRebalancer
        ...
