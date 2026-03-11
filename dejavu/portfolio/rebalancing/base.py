from abc import ABC, abstractmethod
from datetime import datetime

from dejavu.portfolio import Portfolio
from dejavu.schemas import Order


class Rebalancer(ABC):
    @abstractmethod
    def should_rebalance(
        self, timestamp: datetime, portfolio: Portfolio | None = None
    ) -> bool:
        """Called every bar — returns True when rebalance is due. Portfolio is optional for schedule-based rebalancers."""
        ...

    @abstractmethod
    def generate_orders(
        self,
        timestamp:      datetime,
        portfolio:      Portfolio,
        target_weights: dict[str, float],  # symbol -> target % of equity
        prices:         dict[str, float],
    ) -> list[Order]:
        """Generate the corrective orders to hit target weights."""
        ...
