from collections import deque

from dejavu.indicators.base import SequentialIndicator


class SMA(SequentialIndicator):
    """Simple moving average. Used in trend following strategies, and also as the basis for more complex indicators
    like EMA and Bollinger Bands."""
    def __init__(self, period: int):
        super().__init__(period)
        self._sum = 0.0

    def update(self, price: float):
        if len(self._buffer) == self.period:
            self._sum -= self._buffer[0]

        self._buffer.append(price)
        self._sum += price

        if len(self._buffer) == self.period:
            self._value = self._sum / self.period
        return self._value

    def _compute(self, bars: list[float]) -> float:
        # No longer used in the hot path, but kept for API compatibility
        return sum(bars) / len(bars)


class EMA(SequentialIndicator):

    def __init__(self, period: int):
        """Exponential moving average. More responsive to recent price changes than SMA, making it popular for short-term
            trading and momentum strategies."""
        super().__init__(period)
        self._ema: float | None = None
        self._k = 2 / (period + 1)

    def update(self, price: float) -> float | None:
        self._buffer.append(price)
        if len(self._buffer) == self.period and self._ema is None:
            # Seed with SMA of first `period` bars
            self._ema = sum(self._buffer) / self.period
        elif self._ema is not None:
            self._ema = price * self._k + self._ema * (1 - self._k)
        self._value = self._ema
        return self._value

    def _compute(self, bars): # noqa: ARG002
        return self._ema   # handled in update()





class BollingerBands:
    def __init__(self, period: int = 20, num_std: float = 2.0):
        self._sma     = SMA(period)
        self._period  = period
        self._num_std = num_std
        self._buffer  = deque(maxlen=period)
        self.upper:  float | None = None
        self.middle: float | None = None
        self.lower:  float | None = None

    def update(self, price: float):
        self._buffer.append(price)
        mid = self._sma.update(price)
        if mid is None:
            return self
        bars = list(self._buffer)
        std  = (sum((x - mid) ** 2 for x in bars) / len(bars)) ** 0.5
        self.middle = mid
        self.upper  = mid + self._num_std * std
        self.lower  = mid - self._num_std * std
        return self

    @property
    def ready(self) -> bool:
        return self.middle is not None

    @property
    def bandwidth(self) -> float | None:
        if not self.ready:
            return None
        return (self.upper - self.lower) / self.middle

    @property
    def percent_b(self, price: float) -> float | None:
        """Where is price within the bands? 0=lower, 1=upper."""
        if not self.ready:
            return None
        return (price - self.lower) / (self.upper - self.lower)
