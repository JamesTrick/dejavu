from abc import ABC, abstractmethod

from dejavu.portfolio import Portfolio


class PositionSizer(ABC):
    @abstractmethod
    def size(
        self,
        symbol:    str,
        price:     float,
        portfolio: Portfolio,
        **kwargs,  # signal metadata — strength, confidence, ATR, etc.
    ) -> float:
        """Returns the quantity to trade. Positive = long, negative = short."""
        ...
