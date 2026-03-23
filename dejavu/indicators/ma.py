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
        if len(self._buffer) < self.period:
            return None
        if self._value is None:
            self._value = sum(self._buffer) / self.period
        else:
            self._value = price * self._k + self._value * (1.0 - self._k)
        return self._value

    def _compute(self, bars: list[float]) -> float:  # noqa: ARG002
        return sum(bars) / len(bars)


class BollingerBands:
    def __init__(self, period: int = 20, num_std: float = 2.0):
        self._sma = SMA(period)
        self._num_std = num_std
        self.upper: float | None = None
        self.middle: float | None = None
        self.lower: float | None = None

    def update(self, price: float) -> "BollingerBands":
        mid = self._sma.update(price)
        if mid is None:
            return self
        bars = list(self._sma._buffer)
        std = (sum((x - mid) ** 2 for x in bars) / len(bars)) ** 0.5
        self.middle = mid
        self.upper = mid + self._num_std * std
        self.lower = mid - self._num_std * std
        return self

    @property
    def ready(self) -> bool:
        return self.middle is not None

    @property
    def bandwidth(self) -> float | None:
        if self.upper is None or self.lower is None or self.middle is None:
            return None
        return (self.upper - self.lower) / self.middle

    def percent_b(self, price: float) -> float | None:
        """Where is price within the bands? 0 = lower band, 1 = upper band."""
        if self.upper is None or self.lower is None:
            return None
        band_width = self.upper - self.lower
        if band_width == 0:
            return None
        return (price - self.lower) / band_width
