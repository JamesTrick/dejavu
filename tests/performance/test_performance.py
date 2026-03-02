import time
from collections.abc import Iterator
from datetime import datetime, timedelta

from dejavu.data.feed import DataFeed
from dejavu.engine import BacktestEngine
from dejavu.execution.orders import SimulatedExecutionHandler, VolumeWeightedSlippage
from dejavu.indicators.ma import EMA, SMA
from dejavu.indicators.macd import MACD
from dejavu.portfolio import Portfolio
from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    MarketEvent,
    OrderType,
)
from dejavu.strategy.base import Strategy


class InMemoryFeed(DataFeed):
    """Feed that yields a pre-built list of events. Used for performance tests without CSV I/O."""

    def __init__(self, events: list[MarketEvent]):
        self._events = events

    def stream(self) -> Iterator[MarketEvent]:
        yield from self._events


def make_events(
    symbol: str = "SYM",
    n_bars: int = 10_000,
    start_ts: datetime | None = None,
    base_price: float = 100.0,
) -> list[MarketEvent]:
    """Generate n_bars MarketEvents with simple OHLCV. No randomness for reproducibility."""
    if start_ts is None:
        start_ts = datetime(2024, 1, 1, 9, 30)
    events = []
    for i in range(n_bars):
        ts = start_ts + timedelta(minutes=i)
        open = base_price + i * 0.01
        high = open + 0.5
        low = open - 0.5
        close = open + 0.1
        vol = 1_000_000.0
        events.append(
            MarketEvent(
                type=EventType.MARKET,
                timestamp=ts,
                symbol=symbol,
                open=open,
                high=high,
                low=low,
                close=close,
                volume=vol,
                asset_class=AssetClass.EQUITY,
            )
        )
    return events


class NoOpStrategy(Strategy):
    """Strategy that does nothing. Used to measure engine/portfolio/executor overhead."""

    def on_market(self, event: MarketEvent):
        return []


class SimpleBuySellStrategy(Strategy):
    """Minimal strategy: buy on first bar, sell on last. Generates 2 orders total."""

    def __init__(self, portfolio: Portfolio, symbol: str):
        super().__init__(portfolio)
        self.symbol = symbol
        self.bought = False

    def on_market(self, event: MarketEvent):
        if not self.bought:
            self.bought = True
            return [(self.buy(self.symbol, qty=10, order_type=OrderType.MARKET, asset_class=AssetClass.EQUITY), {"asset_class": AssetClass.EQUITY})]
        return []


def _run_engine_perf(n_bars: int, strategy_factory=lambda p: NoOpStrategy(p), label: str = ""):
    events = make_events(n_bars=n_bars)
    feed = InMemoryFeed(events)
    portfolio = Portfolio(initial_capital=100_000)
    strategy = strategy_factory(portfolio)
    executor = SimulatedExecutionHandler(slippage=VolumeWeightedSlippage(impact_factor=0.0))
    engine = BacktestEngine(feed, strategy, portfolio, executor)

    t0 = time.perf_counter()
    engine.run()
    elapsed = time.perf_counter() - t0

    per_bar_us = (elapsed / n_bars) * 1_000_000
    bars_per_sec = n_bars / elapsed if elapsed > 0 else 0
    return elapsed, per_bar_us, bars_per_sec


def test_perf_engine_run_5k_bars_no_orders(capsys):
    """Time full backtest: 5k bars, no strategy orders (engine + portfolio + feed iteration)."""
    n = 5_000
    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(n, label="no_orders")
    with capsys.disabled():
        print(f"\n  [perf] engine run ({n} bars, no orders): {elapsed:.3f}s total, {per_bar_us:.1f} µs/bar, {bars_per_sec:,.0f} bars/s")
    assert elapsed >= 0
    assert bars_per_sec > 0


def test_perf_engine_run_10k_bars_no_orders(capsys):
    """Time full backtest: 10k bars, no strategy orders."""
    n = 10_000
    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(n, label="no_orders")
    with capsys.disabled():
        print(f"\n  [perf] engine run ({n} bars, no orders): {elapsed:.3f}s total, {per_bar_us:.1f} µs/bar, {bars_per_sec:,.0f} bars/s")
    assert elapsed >= 0
    assert bars_per_sec > 0


def test_perf_engine_run_10k_bars_with_one_buy(capsys):
    """Time full backtest: 10k bars, one buy order (strategy + execution + portfolio apply_fill)."""
    n = 10_000

    def strategy_factory(portfolio):
        return SimpleBuySellStrategy(portfolio, "SYM")

    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(n, strategy_factory=strategy_factory, label="one_buy")
    with capsys.disabled():
        print(f"\n  [perf] engine run ({n} bars, 1 buy): {elapsed:.3f}s total, {per_bar_us:.1f} µs/bar, {bars_per_sec:,.0f} bars/s")
    assert elapsed >= 0
    assert bars_per_sec > 0


def test_perf_indicators_sma_50k_updates(capsys):
    """Time SMA(20) over 50k price updates."""
    sma = SMA(period=20)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        sma.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(f"\n  [perf] SMA(20) {n} updates: {elapsed:.3f}s total, {per_update_us:.2f} µs/update")
    assert elapsed >= 0
    assert sma.ready


def test_perf_indicators_ema_50k_updates(capsys):
    """Time EMA(20) over 50k price updates."""
    ema = EMA(period=20)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        ema.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(f"\n  [perf] EMA(20) {n} updates: {elapsed:.3f}s total, {per_update_us:.2f} µs/update")
    assert elapsed >= 0
    assert ema.ready


def test_perf_indicators_macd_50k_updates(capsys):
    """Time MACD(12,26,9) over 50k price updates."""
    macd = MACD(fast=12, slow=26, signal=9)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        macd.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(f"\n  [perf] MACD(12,26,9) {n} updates: {elapsed:.3f}s total, {per_update_us:.2f} µs/update")
    assert elapsed >= 0
    assert macd.ready


# ----- Feed stream (iteration only) -----


def test_perf_feed_stream_50k_events(capsys):
    """Time iterating 50k events from in-memory feed (no engine)."""
    n = 50_000
    events = make_events(n_bars=n)
    feed = InMemoryFeed(events)
    t0 = time.perf_counter()
    count = 0
    for _ in feed.stream():
        count += 1
    elapsed = time.perf_counter() - t0
    per_event_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(f"\n  [perf] feed stream {n} events: {elapsed:.3f}s total, {per_event_us:.2f} µs/event, count={count}")
    assert count == n
    assert elapsed >= 0


# ----- Portfolio: update_prices + apply_fill -----


def test_perf_portfolio_10k_updates(capsys):
    """Time 10k portfolio.update_prices() calls (no fills)."""
    portfolio = Portfolio(initial_capital=100_000)
    events = make_events(n_bars=10_000)
    t0 = time.perf_counter()
    for ev in events:
        portfolio.update_prices(ev)
    elapsed = time.perf_counter() - t0
    per_us = (elapsed / len(events)) * 1_000_000
    with capsys.disabled():
        print(f"\n  [perf] portfolio update_prices x{len(events)}: {elapsed:.3f}s total, {per_us:.2f} µs/update")
    assert elapsed >= 0


def test_perf_portfolio_1k_fills(capsys):
    """Time 1k apply_fill() calls (buy then sell pattern)."""
    portfolio = Portfolio(initial_capital=1_000_000)
    ts = datetime(2024, 1, 1)
    t0 = time.perf_counter()
    for i in range(1000):
        buy = FillEvent(
            type=EventType.FILL,
            timestamp=ts,
            symbol="SYM",
            quantity=100,
            fill_price=100.0 + i * 0.01,
            commission=1.0,
            multiplier=1,
        )
        portfolio.apply_fill(buy, {"asset_class": AssetClass.EQUITY})
        sell = FillEvent(
            type=EventType.FILL,
            timestamp=ts,
            symbol="SYM",
            quantity=-100,
            fill_price=100.0 + i * 0.01 + 0.1,
            commission=1.0,
            multiplier=1,
        )
        portfolio.apply_fill(sell, {"asset_class": AssetClass.EQUITY})
    elapsed = time.perf_counter() - t0
    per_us = (elapsed / 2000) * 1_000_000  # 2k fills
    with capsys.disabled():
        print(f"\n  [perf] portfolio apply_fill x2000 (1k round-trips): {elapsed:.3f}s total, {per_us:.2f} µs/fill")
    assert elapsed >= 0

from dejavu._core import RustPortfolio


def test_perf_new_portfolio_1k_fills(capsys):
    """Time 1k apply_fill() calls using the Rust backend."""
    # 1. Setup
    initial_capital = 1_000_000.0
    portfolio = RustPortfolio(initial_capital)
    ts = datetime(2024, 1, 1)
    ts_int = int(ts.timestamp())  # Pre-calculate to measure pure logic speed

    # 2. Timing Loop
    # We measure the cost of calling the Rust FFI
    t0 = time.perf_counter()

    for i in range(1000):
        # We simulate the price moving slightly
        buy_price = 100.0 + (i * 0.01)
        sell_price = buy_price + 0.1

        # Call Rust directly
        # Arguments: symbol, qty, price, comm, multiplier, timestamp, position_meta
        portfolio.apply_fill("SYM", 100.0, buy_price, 1.0, 1.0, ts_int, None)
        portfolio.apply_fill("SYM", -100.0, sell_price, 1.0, 1.0, ts_int, None)

    elapsed = time.perf_counter() - t0

    # 3. Reporting
    total_fills = 2000
    per_us = (elapsed / total_fills) * 1_000_000

    with capsys.disabled():
        print(f"\n  [perf] RUST portfolio apply_fill x2000: {elapsed:.4f}s total, {per_us:.3f} µs/fill")

    # 4. Correctness Assertions
    # Verify the math actually happened in Rust
    # Each round trip: Profit = (0.1 * 100) - 2.0 (comm) = 8.0
    # 1000 round trips = 8000.0 profit
    expected_cash = initial_capital + 8000.0
    assert abs(portfolio.cash - expected_cash) < 1e-7
    assert elapsed > 0