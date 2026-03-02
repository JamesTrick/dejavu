from typing import Optional

from .base import SequentialIndicator
from .ma import EMA


class MACD(SequentialIndicator):
    """
    Moving Average Convergence Divergence (MACD).

    A trend-following momentum indicator that shows the relationship between two
    exponential moving averages (EMAs) of a security’s price. The MACD is
    calculated by subtracting the 'slow' EMA from the 'fast' EMA.

    The result is a momentum oscillator that fluctuates above and below a zero line.
    It is used to identify trend direction, momentum shifts, and potential
    reversal points.
    """
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        """Initializes the MACD with standard (12, 26, 9) parameters.

        Args:
            fast: Period for the shorter EMA (reacts quickly to price changes).
            slow: Period for the longer EMA (provides the baseline trend).
            signal: Smoothing period for the MACD line itself, used to generate signals.
        """
        super().__init__(slow + signal)
        self.fast_period = fast
        self.slow_period = slow
        self.signal_period = signal

        # Pre-calculate multipliers
        self.fast_k = 2.0 / (fast + 1)
        self.slow_k = 2.0 / (slow + 1)
        self.sig_k = 2.0 / (signal + 1)

        # State tracking
        self._fast_ema: Optional[float] = None
        self._slow_ema: Optional[float] = None
        self._sig_ema: Optional[float] = None

        # Temporary buffers for seeding
        self._fast_buf = []
        self._slow_buf = []
        self._sig_buf = []

    def update(self, price: float) -> Optional[tuple[float, float, float]]:
        # 1. Fast EMA
        if self._fast_ema is None:
            self._fast_buf.append(price)
            if len(self._fast_buf) == self.fast_period:
                self._fast_ema = sum(self._fast_buf) / self.fast_period
        else:
            self._fast_ema = price * self.fast_k + self._fast_ema * (1.0 - self.fast_k)

        # 2. Slow EMA
        if self._slow_ema is None:
            self._slow_buf.append(price)
            if len(self._slow_buf) == self.slow_period:
                self._slow_ema = sum(self._slow_buf) / self.slow_period
        else:
            self._slow_ema = price * self.slow_k + self._slow_ema * (1.0 - self.slow_k)

        # 3. MACD Line & Signal Line
        if self._fast_ema is not None and self._slow_ema is not None:
            macd_line = self._fast_ema - self._slow_ema

            if self._sig_ema is None:
                self._sig_buf.append(macd_line)
                if len(self._sig_buf) == self.signal_period:
                    self._sig_ema = sum(self._sig_buf) / self.signal_period
                    macd_hist = macd_line - self._sig_ema
                    self._value = (macd_line, self._sig_ema, macd_hist)
            else:
                self._sig_ema = macd_line * self.sig_k + self._sig_ema * (1.0 - self.sig_k)
                macd_hist = macd_line - self._sig_ema
                self._value = (macd_line, self._sig_ema, macd_hist)

        return self._value

    def _compute(self, bars: list[float]):
        # Unused in hot loop, here to satisfy ABC
        pass

