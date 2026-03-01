from collections import deque
from typing import Optional

from dejavu.indicators.base import SequentialIndicator


class SMA(SequentialIndicator):
    """Simple moving average. Used in trend following strategies, and also as the basis for more complex indicators
    like EMA and Bollinger Bands."""
    def _compute(self, bars: list[float]) -> float:
        return sum(bars) / len(bars)


class EMA(SequentialIndicator):
    """Exponential moving average. More responsive to recent price changes than SMA, making it popular for short-term
    trading and momentum strategies."""
    def __init__(self, period: int):
        super().__init__(period)
        self._ema: Optional[float] = None
        self._k = 2 / (period + 1)

    def update(self, price: float) -> Optional[float]:
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


class MACD:
    """Not a single-period indicator so doesn't extend base."""

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self._fast   = EMA(fast)
        self._slow   = EMA(slow)
        self._signal = EMA(signal)
        self.macd_line:   Optional[float] = None
        self.signal_line: Optional[float] = None
        self.histogram:   Optional[float] = None

    def update(self, price: float) -> Optional[float]:
        fast = self._fast.update(price)
        slow = self._slow.update(price)

        if fast is None or slow is None:
            return None

        self.macd_line = fast - slow
        sig = self._signal.update(self.macd_line)

        if sig is None:
            return None

        self.signal_line = sig
        self.histogram   = self.macd_line - self.signal_line
        return self.histogram

    @property
    def ready(self) -> bool:
        return self.histogram is not None


class BollingerBands:
    def __init__(self, period: int = 20, num_std: float = 2.0):
        self._sma     = SMA(period)
        self._period  = period
        self._num_std = num_std
        self._buffer  = deque(maxlen=period)
        self.upper:  Optional[float] = None
        self.middle: Optional[float] = None
        self.lower:  Optional[float] = None

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
    def bandwidth(self) -> Optional[float]:
        if not self.ready:
            return None
        return (self.upper - self.lower) / self.middle

    @property
    def percent_b(self, price: float) -> Optional[float]:
        """Where is price within the bands? 0=lower, 1=upper."""
        if not self.ready:
            return None
        return (price - self.lower) / (self.upper - self.lower)
