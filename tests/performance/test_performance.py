import time
from collections.abc import Iterator
from datetime import datetime, timedelta

from dejavu.data.feed import DataFeed
from dejavu.engine import BacktestEngine
from dejavu.execution.commission import PerContractCommission
from dejavu.execution.orders import SimulatedExecutionHandler, VolumeWeightedSlippage
from dejavu.indicators.ma import EMA, SMA
from dejavu.indicators.macd import MACD
from dejavu.portfolio import Portfolio
from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    FillTiming,
    Instrument,
    MarketEvent,
    Order,
    OrderType,
)
from dejavu.strategy.base import Strategy

# ─────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────

EQUITY_INSTRUMENT = Instrument(
    symbol="SYM",
    asset_class=AssetClass.EQUITY,
    multiplier=1.0,
)


class InMemoryFeed(DataFeed):
    """Feed that yields a pre-built list of events. No CSV I/O."""

    def __init__(self, events: list[MarketEvent]):
        self._events = events

    def stream(self) -> Iterator[MarketEvent]:
        yield from self._events


def make_events(
    symbol: str = "SYM",
    n_bars: int = 10_000,
    start_ts: datetime | None = None,
    base_price: float = 100.0,
    instrument: Instrument | None = None,
) -> list[MarketEvent]:
    """
    Generate n_bars MarketEvents with simple OHLCV.
    No randomness — fully reproducible.
    """
    if start_ts is None:
        start_ts = datetime(2024, 1, 1, 9, 30)
    if instrument is None:
        instrument = Instrument(symbol=symbol, asset_class=AssetClass.EQUITY, multiplier=1.0)

    events = []
    for i in range(n_bars):
        ts = start_ts + timedelta(minutes=i)
        open_ = base_price + i * 0.01
        events.append(
            MarketEvent(
                instrument=instrument,
                type=EventType.MARKET,
                timestamp=ts,
                open=open_,
                high=open_ + 0.5,
                low=open_ - 0.5,
                close=open_ + 0.1,
                volume=1_000_000.0,
            )
        )
    return events


# ─────────────────────────────────────────────
# Strategies
# ─────────────────────────────────────────────


class NoOpStrategy(Strategy):
    """Does nothing. Measures pure engine + portfolio + feed overhead."""

    def on_market(self, event: MarketEvent) -> list:
        return []


class SimpleBuySellStrategy(Strategy):
    """Buys 10 shares on the first bar, never sells. Generates 1 order total."""

    def __init__(self, portfolio: Portfolio, instrument: Instrument):
        super().__init__(portfolio)
        self.instrument = instrument
        self.bought = False

    def on_market(self, event: MarketEvent) -> list:
        if not self.bought and event.instrument.symbol == self.instrument.symbol:
            self.bought = True
            return [
                Order(
                    instrument=self.instrument,
                    quantity=10.0,
                    order_type=OrderType.MARKET,
                )
            ]
        return []


# ─────────────────────────────────────────────
# Engine runner helper
# ─────────────────────────────────────────────


def _build_executor() -> SimulatedExecutionHandler:
    return SimulatedExecutionHandler(
        commission=PerContractCommission(rate=0.65),
        slippage=VolumeWeightedSlippage(impact_factor=0.0),
    )


def _run_engine_perf(
    n_bars: int,
    strategy_factory=None,
    fill_timing: FillTiming = FillTiming.NEXT_BAR,
) -> tuple[float, float, float]:
    """
    Returns (elapsed_seconds, microseconds_per_bar, bars_per_second).
    """
    if strategy_factory is None:
        strategy_factory = lambda p: NoOpStrategy(p)

    events = make_events(n_bars=n_bars)
    portfolio = Portfolio(initial_capital=100_000)
    engine = BacktestEngine(
        feed=InMemoryFeed(events),
        strategy=strategy_factory(portfolio),
        portfolio=portfolio,
        executor=_build_executor(),
        fill_timing=fill_timing,
    )

    t0 = time.perf_counter()
    engine.run()
    elapsed = time.perf_counter() - t0

    per_bar_us = (elapsed / n_bars) * 1_000_000
    bars_per_sec = n_bars / elapsed if elapsed > 0 else 0
    return elapsed, per_bar_us, bars_per_sec


# ─────────────────────────────────────────────
# Engine performance tests
# ─────────────────────────────────────────────


def test_perf_engine_5k_bars_no_orders(capsys):
    """5k bars, no orders — measures engine + portfolio + feed iteration."""
    n = 5_000
    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(n)
    with capsys.disabled():
        print(
            f"\n  [perf] engine ({n} bars, no orders): "
            f"{elapsed:.3f}s | {per_bar_us:.1f} µs/bar | {bars_per_sec:,.0f} bars/s"
        )
    assert bars_per_sec > 0


def test_perf_engine_10k_bars_no_orders(capsys):
    """10k bars, no orders."""
    n = 10_000
    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(n)
    with capsys.disabled():
        print(
            f"\n  [perf] engine ({n} bars, no orders): "
            f"{elapsed:.3f}s | {per_bar_us:.1f} µs/bar | {bars_per_sec:,.0f} bars/s"
        )
    assert bars_per_sec > 0


def test_perf_engine_10k_bars_one_buy_next_bar(capsys):
    """10k bars, one market buy order, NEXT_BAR fill timing."""
    n = 10_000

    def strategy_factory(p):
        return SimpleBuySellStrategy(p, EQUITY_INSTRUMENT)

    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(
        n,
        strategy_factory=strategy_factory,
        fill_timing=FillTiming.NEXT_BAR,
    )
    with capsys.disabled():
        print(
            f"\n  [perf] engine ({n} bars, 1 buy, NEXT_BAR): "
            f"{elapsed:.3f}s | {per_bar_us:.1f} µs/bar | {bars_per_sec:,.0f} bars/s"
        )
    assert bars_per_sec > 0


def test_perf_engine_10k_bars_one_buy_same_bar(capsys):
    """10k bars, one market buy order, SAME_BAR fill timing."""
    n = 10_000

    def strategy_factory(p):
        return SimpleBuySellStrategy(p, EQUITY_INSTRUMENT)

    elapsed, per_bar_us, bars_per_sec = _run_engine_perf(
        n,
        strategy_factory=strategy_factory,
        fill_timing=FillTiming.SAME_BAR,
    )
    with capsys.disabled():
        print(
            f"\n  [perf] engine ({n} bars, 1 buy, SAME_BAR): "
            f"{elapsed:.3f}s | {per_bar_us:.1f} µs/bar | {bars_per_sec:,.0f} bars/s"
        )
    assert bars_per_sec > 0


# ─────────────────────────────────────────────
# Indicator performance tests
# ─────────────────────────────────────────────


def test_perf_indicators_sma_50k_updates(capsys):
    """SMA(20) over 50k price updates."""
    sma = SMA(period=20)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        sma.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] SMA(20) {n} updates: "
            f"{elapsed:.3f}s | {per_update_us:.2f} µs/update"
        )
    assert sma.ready


def test_perf_indicators_ema_50k_updates(capsys):
    """EMA(20) over 50k price updates."""
    ema = EMA(period=20)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        ema.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] EMA(20) {n} updates: "
            f"{elapsed:.3f}s | {per_update_us:.2f} µs/update"
        )
    assert ema.ready


def test_perf_indicators_macd_50k_updates(capsys):
    """MACD(12, 26, 9) over 50k price updates."""
    macd = MACD(fast=12, slow=26, signal=9)
    n = 50_000
    t0 = time.perf_counter()
    for i in range(n):
        macd.update(100.0 + i * 0.01)
    elapsed = time.perf_counter() - t0
    per_update_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] MACD(12,26,9) {n} updates: "
            f"{elapsed:.3f}s | {per_update_us:.2f} µs/update"
        )
    assert macd.ready


# ─────────────────────────────────────────────
# Feed performance tests
# ─────────────────────────────────────────────


def test_perf_feed_stream_50k_events(capsys):
    """Iterate 50k events from in-memory feed — no engine overhead."""
    n = 50_000
    events = make_events(n_bars=n)
    feed = InMemoryFeed(events)

    t0 = time.perf_counter()
    count = sum(1 for _ in feed.stream())
    elapsed = time.perf_counter() - t0

    per_event_us = (elapsed / n) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] feed stream {n} events: "
            f"{elapsed:.3f}s | {per_event_us:.2f} µs/event | count={count}"
        )
    assert count == n


# ─────────────────────────────────────────────
# Portfolio performance tests
# ─────────────────────────────────────────────


def test_perf_portfolio_10k_price_updates(capsys):
    """10k portfolio.update_prices() calls with no fills."""
    portfolio = Portfolio(initial_capital=100_000)
    events = make_events(n_bars=10_000)

    t0 = time.perf_counter()
    for ev in events:
        portfolio.update_prices(ev)
    elapsed = time.perf_counter() - t0

    per_us = (elapsed / len(events)) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] portfolio.update_prices x{len(events)}: "
            f"{elapsed:.3f}s | {per_us:.2f} µs/update"
        )
    assert elapsed >= 0


def test_perf_portfolio_1k_round_trip_fills(capsys):
    """1k buy+sell round trips through portfolio.apply_fill() — 2k fills total."""
    portfolio = Portfolio(initial_capital=1_000_000)
    ts = datetime(2024, 1, 1)
    n_round_trips = 1_000

    t0 = time.perf_counter()
    for i in range(n_round_trips):
        price = 100.0 + i * 0.01

        buy_order = Order(
            instrument=EQUITY_INSTRUMENT,
            quantity=100.0,
            order_type=OrderType.MARKET,
        )
        portfolio.apply_fill(
            FillEvent(
                type=EventType.FILL,
                timestamp=ts,
                order_id=buy_order.order_id,
                instrument=EQUITY_INSTRUMENT,
                quantity=100.0,
                fill_price=price,
                commission=1.0,
                multiplier=1.0,
            )
        )

        sell_order = Order(
            instrument=EQUITY_INSTRUMENT,
            quantity=-100.0,
            order_type=OrderType.MARKET,
        )
        portfolio.apply_fill(
            FillEvent(
                type=EventType.FILL,
                timestamp=ts,
                order_id=sell_order.order_id,
                instrument=EQUITY_INSTRUMENT,
                quantity=-100.0,
                fill_price=price + 0.10,
                commission=1.0,
                multiplier=1.0,
            )
        )

    elapsed = time.perf_counter() - t0
    n_fills = n_round_trips * 2
    per_us = (elapsed / n_fills) * 1_000_000
    with capsys.disabled():
        print(
            f"\n  [perf] portfolio.apply_fill x{n_fills} ({n_round_trips} round-trips): "
            f"{elapsed:.3f}s | {per_us:.2f} µs/fill"
        )
    assert elapsed >= 0
