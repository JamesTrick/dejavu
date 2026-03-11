from abc import ABC, abstractmethod
from collections import deque


class SequentialIndicator(ABC):
    """
    Stateful indicator — feed it prices one at a time via update(),
    read the current value via value.
    """

    def __init__(self, period: int):
        self.period  = period
        self._buffer = deque(maxlen=period)
        self._value: float | None = None

    def update(self, price: float) -> float | None:
        self._buffer.append(price)
        if len(self._buffer) >= self.period:
            self._value = self._compute(list(self._buffer))
        return self._value

    @abstractmethod
    def _compute(self, bars: list[float]) -> float:
        ...

    @property
    def value(self) -> float | None:
        return self._value

    @property
    def ready(self) -> bool:
        return self._value is not None

    def __gt__(self, other: "SequentialIndicator") -> bool:
        if not self.ready or not other.ready:
            return False
        return self.value > other.value

    def __lt__(self, other: "SequentialIndicator") -> bool:
        if not self.ready or not other.ready:
            return False
        return self.value < other.value

    def __ge__(self, other: "SequentialIndicator") -> bool:
        if not self.ready or not other.ready:
            return False
        return self.value >= other.value

    def __le__(self, other: "SequentialIndicator") -> bool:
        if not self.ready or not other.ready:
            return False
        return self.value <= other.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, "SequentialIndicator"):
            return NotImplemented
        if not self.ready or not other.ready:
            return False
        return self.value == other.value

    def __repr__(self):
        return f"{self.__class__.__name__}(period={self.period}, value={self._value})"
