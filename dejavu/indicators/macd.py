from dataclasses import dataclass, field


@dataclass
class _EMAState:
    period: int
    k: float = field(init=False)
    value: float | None = None
    _buf: list[float] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self.k = 2.0 / (self.period + 1)

    def push(self, price: float) -> float | None:
        if self.value is None:
            self._buf.append(price)
            if len(self._buf) == self.period:
                self.value = sum(self._buf) / self.period
                self._buf.clear()
        else:
            self.value = price * self.k + self.value * (1.0 - self.k)
        return self.value


class MACD:
    """Moving Average Convergence Divergence."""

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self._fast = _EMAState(fast)
        self._slow = _EMAState(slow)
        self._sig = _EMAState(signal)

    def update(self, price: float) -> tuple[float, float, float] | None:
        fast_val = self._fast.push(price)
        slow_val = self._slow.push(price)

        if fast_val is None or slow_val is None:
            return None

        macd_line = fast_val - slow_val
        sig_val = self._sig.push(macd_line)

        if sig_val is None:
            return None

        self._value = (macd_line, sig_val, macd_line - sig_val)
        return self._value  # type: ignore[return-value]
