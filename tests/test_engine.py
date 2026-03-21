from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from dejavu.engine import BacktestEngine
from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    FillTiming,
    Instrument,
    MarketEvent,
    MultiLegOrder,
    Option,
    Order,
    OrderType,
)


def make_instrument(symbol="AAPL", asset_class=AssetClass.EQUITY, multiplier=1.0):
    return Instrument(symbol=symbol, asset_class=asset_class, multiplier=multiplier)


def make_option(symbol="AAPL240119C00150", underlying="AAPL", strike=150.0, multiplier=100.0):
    return Option(
        symbol=symbol,
        asset_class=AssetClass.OPTION,
        underlying=underlying,
        strike=strike,
        expiry=datetime(2024, 1, 19),
        option_type="C",
        multiplier=multiplier,
    )


def make_event(instrument, close=100.0, timestamp=None, volume=1_000_000.0):
    return MarketEvent(
        type=EventType.MARKET,
        timestamp=timestamp or datetime(2024, 1, 2),
        instrument=instrument,
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=volume,
    )


def make_order(instrument, quantity=100.0, order_type=OrderType.MARKET):
    return Order(instrument=instrument, quantity=quantity, order_type=order_type)


def make_fill(order, fill_price=100.0, timestamp=None):
    return FillEvent(
        type=EventType.FILL,
        timestamp=timestamp or datetime(2024, 1, 2),
        order_id=order.order_id,
        instrument=order.instrument,
        quantity=order.quantity,
        fill_price=fill_price,
        commission=1.0,
    )


def make_engine(
    events,
    orders_per_event=None,
    fill_timing=FillTiming.NEXT_BAR,
    executor_fills=None,
):
    """Builds an engine with fully mocked components.

    - events: list of MarketEvents the feed will yield
    - orders_per_event: dict of {event_index: [Order, ...]} the strategy returns
    - executor_fills: dict of {symbol: FillEvent | None} controlling executor behaviour
    """
    feed = MagicMock()
    feed.stream.return_value = iter(events)

    portfolio = MagicMock()
    portfolio.equity = 25_000.0
    portfolio.cash = 25_000.0

    strategy = MagicMock()
    orders_per_event = orders_per_event or {}

    call_count = [-1]

    def on_market_side_effect(event):
        call_count[0] += 1
        return orders_per_event.get(call_count[0], [])

    strategy.on_market.side_effect = on_market_side_effect

    executor = MagicMock()
    executor_fills = executor_fills or {}

    def execute_side_effect(order, market_event, portfolio):
        return executor_fills.get(order.instrument.symbol)

    executor.execute.side_effect = execute_side_effect

    engine = BacktestEngine(
        feed=feed,
        strategy=strategy,
        portfolio=portfolio,
        executor=executor,
        fill_timing=fill_timing,
    )

    return engine, feed, portfolio, strategy, executor


class TestEngineLifecycle:
    def test_run_calls_stream(self):
        inst = make_instrument()
        events = [make_event(inst)]
        engine, feed, *_ = make_engine(events)
        engine.run()
        feed.stream.assert_called_once()

    def test_run_with_no_events_completes(self):
        engine, *_ = make_engine(events=[])
        engine.run()  # should not raise

    def test_update_prices_called_for_every_event(self):
        inst = make_instrument()
        events = [make_event(inst, timestamp=datetime(2024, 1, i)) for i in range(2, 7)]
        engine, _, portfolio, *_ = make_engine(events)
        engine.run()
        assert portfolio.update_prices.call_count == 5

    def test_history_length_matches_event_count(self):
        inst = make_instrument()
        n = 10
        events = [make_event(inst, timestamp=datetime(2024, 1, 2) + timedelta(days=i)) for i in range(n)]

        real_portfolio = MagicMock()
        real_portfolio.equity = 25_000.0
        real_portfolio.cash = 25_000.0

        feed = MagicMock()
        feed.stream.return_value = iter(events)
        strategy = MagicMock()
        strategy.on_market.return_value = []
        executor = MagicMock()

        engine = BacktestEngine(feed, strategy, real_portfolio, executor)
        engine.run()

        history = real_portfolio.history
        assert len(history) == n

    def test_history_timestamps_are_in_order(self):
        inst = make_instrument()
        timestamps = [datetime(2024, 1, 2) + timedelta(days=i) for i in range(5)]
        events = [make_event(inst, timestamp=t) for t in timestamps]

        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        feed = MagicMock()
        feed.stream.return_value = iter(events)
        strategy = MagicMock()
        strategy.on_market.return_value = []
        executor = MagicMock()

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        recorded = [h["timestamp"] for h in portfolio.history]
        assert recorded == timestamps

    def test_strategy_on_market_called_for_every_event(self):
        inst = make_instrument()
        events = [make_event(inst, timestamp=datetime(2024, 1, i)) for i in range(2, 7)]
        engine, _, _, strategy, _ = make_engine(events)
        engine.run()
        assert strategy.on_market.call_count == 5


class TestNextBarFillTiming:
    def test_market_order_not_filled_on_same_bar(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        order = make_order(inst)
        fill = make_fill(order, timestamp=t2)

        engine, _, portfolio, _, executor = make_engine(
            events=[e1, e2],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.NEXT_BAR,
            executor_fills={inst.symbol: fill},
        )
        engine.run()

        # executor should NOT have been called during tick 1 (same bar)
        # It should be called during tick 2 when the pending order is processed
        calls = executor.execute.call_args_list
        assert len(calls) == 1
        _, kwargs = calls[0]
        # The market event passed should be e2 (next bar)
        assert executor.execute.call_args[0][1].timestamp == t2

    def test_fill_applied_on_next_bar(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        order = make_order(inst)
        fill = make_fill(order, timestamp=t2)

        engine, _, portfolio, _, _ = make_engine(
            events=[e1, e2],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.NEXT_BAR,
            executor_fills={inst.symbol: fill},
        )
        engine.run()
        portfolio.apply_fill.assert_called_once_with(fill)

    def test_unfilled_limit_order_stays_pending(self):
        inst = make_instrument()
        events = [
            make_event(inst, close=100.0, timestamp=datetime(2024, 1, i))
            for i in range(2, 6)
        ]
        order = make_order(inst, order_type=OrderType.LIMIT)

        engine, _, portfolio, _, executor = make_engine(
            events=events,
            orders_per_event={0: [order]},
            fill_timing=FillTiming.NEXT_BAR,
            executor_fills={inst.symbol: None},  # never fills
        )
        engine.run()

        # executor should have been tried on bars 2, 3, 4 (3 times)
        assert executor.execute.call_count == 3
        portfolio.apply_fill.assert_not_called()


class TestSameBarFillTiming:
    def test_market_order_filled_immediately(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        event = make_event(inst, timestamp=t1)
        order = make_order(inst)
        fill = make_fill(order, timestamp=t1)

        engine, _, portfolio, _, executor = make_engine(
            events=[event],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.SAME_BAR,
            executor_fills={inst.symbol: fill},
        )
        engine.run()

        executor.execute.assert_called_once()
        portfolio.apply_fill.assert_called_once_with(fill)

    def test_same_bar_fill_uses_current_event(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        event = make_event(inst, close=123.45, timestamp=t1)
        order = make_order(inst)
        fill = make_fill(order, fill_price=123.45, timestamp=t1)

        engine, _, portfolio, _, executor = make_engine(
            events=[event],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.SAME_BAR,
            executor_fills={inst.symbol: fill},
        )
        engine.run()

        call_args = executor.execute.call_args
        assert call_args[0][1].timestamp == t1
        assert call_args[0][1].close == 123.45

    def test_failed_same_bar_fill_queues_for_next_bar(self):
        """If executor returns None on same-bar, order falls back to pending queue."""
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)
        order = make_order(inst)
        fill = make_fill(order, timestamp=t2)

        # First call returns None (same-bar fail), second call returns fill
        executor = MagicMock()
        executor.execute.side_effect = [None, fill]

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order], []]

        engine = BacktestEngine(
            feed, strategy, portfolio, executor,
            fill_timing=FillTiming.SAME_BAR,
        )
        engine.run()

        assert executor.execute.call_count == 2
        portfolio.apply_fill.assert_called_once_with(fill)

    def test_limit_order_always_queued_even_with_same_bar(self):
        """SAME_BAR only short-circuits MARKET orders."""
        inst = make_instrument()
        event = make_event(inst, timestamp=datetime(2024, 1, 2))
        order = make_order(inst, order_type=OrderType.LIMIT)
        fill = make_fill(order)

        engine, _, portfolio, _, executor = make_engine(
            events=[event],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.SAME_BAR,
            executor_fills={inst.symbol: fill},
        )
        engine.run()

        # Limit order queued, but no next bar arrives — should not be filled
        executor.execute.assert_not_called()
        portfolio.apply_fill.assert_not_called()

    def test_same_bar_only_applies_to_matching_symbol(self):
        """A SAME_BAR order for symbol B should not fill when event is for symbol A."""
        inst_a = make_instrument("AAPL")
        inst_b = make_instrument("MSFT")
        event_a = make_event(inst_a, timestamp=datetime(2024, 1, 2))
        order_b = make_order(inst_b)
        fill_b = make_fill(order_b)

        engine, _, portfolio, _, executor = make_engine(
            events=[event_a],
            orders_per_event={0: [order_b]},
            fill_timing=FillTiming.SAME_BAR,
            executor_fills={inst_b.symbol: fill_b},
        )
        engine.run()

        # No MSFT event ever arrived, so order_b stays pending and is never tried
        executor.execute.assert_not_called()
        portfolio.apply_fill.assert_not_called()


# ─────────────────────────────────────────────
# 4. Multi-Leg Orders
# ─────────────────────────────────────────────

class TestMultiLegOrders:
    def test_each_leg_routed_by_its_own_symbol(self):
        inst_a = make_instrument("AAPL")
        inst_b = make_instrument("MSFT")

        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)

        # Both legs submitted on tick 0 (AAPL event)
        # AAPL fills on tick 1, MSFT fills on tick 1 (MSFT event)
        e_aapl_1 = make_event(inst_a, timestamp=t1)
        e_aapl_2 = make_event(inst_a, timestamp=t2)
        e_msft_2 = make_event(inst_b, timestamp=t2)

        leg_a = make_order(inst_a)
        leg_b = make_order(inst_b)
        multi = MultiLegOrder(legs=[leg_a, leg_b], strategy_type="spread")

        fill_a = make_fill(leg_a, timestamp=t2)
        fill_b = make_fill(leg_b, timestamp=t2)

        executor = MagicMock()
        executor.execute.side_effect = lambda order, event, portfolio: (
            fill_a if order.instrument.symbol == "AAPL" else fill_b
        )

        feed = MagicMock()
        feed.stream.return_value = iter([e_aapl_1, e_aapl_2, e_msft_2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[multi], [], []]

        engine = BacktestEngine(
            feed, strategy, portfolio, executor,
            fill_timing=FillTiming.NEXT_BAR,
        )
        engine.run()

        assert portfolio.apply_fill.call_count == 2

    def test_multi_leg_individual_legs_can_fill_independently(self):
        inst_a = make_instrument("AAPL")
        inst_b = make_instrument("MSFT")
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)

        e1 = make_event(inst_a, timestamp=t1)
        e2_aapl = make_event(inst_a, timestamp=t2)

        leg_a = make_order(inst_a)
        leg_b = make_order(inst_b)
        multi = MultiLegOrder(legs=[leg_a, leg_b], strategy_type="spread")
        fill_a = make_fill(leg_a, timestamp=t2)

        executor = MagicMock()
        # Only AAPL fills; MSFT never gets an event
        executor.execute.side_effect = lambda order, event, portfolio: (
            fill_a if order.instrument.symbol == "AAPL" else None
        )

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2_aapl])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[multi], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        # Only AAPL leg filled
        portfolio.apply_fill.assert_called_once_with(fill_a)


# ─────────────────────────────────────────────
# 5. Pending Order Queue Behaviour
# ─────────────────────────────────────────────

class TestPendingOrders:
    def test_order_retried_each_bar_until_filled(self):
        inst = make_instrument()
        events = [
            make_event(inst, timestamp=datetime(2024, 1, i)) for i in range(2, 6)
        ]
        order = make_order(inst)
        fill = make_fill(order)

        # Fails twice, then fills on the third attempt
        executor = MagicMock()
        executor.execute.side_effect = [None, None, fill]

        feed = MagicMock()
        feed.stream.return_value = iter(events)
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order], [], [], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        assert executor.execute.call_count == 3
        portfolio.apply_fill.assert_called_once_with(fill)

    def test_filled_order_removed_from_pending_queue(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        t3 = datetime(2024, 1, 4)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)
        e3 = make_event(inst, timestamp=t3)

        order = make_order(inst)
        fill = make_fill(order, timestamp=t2)

        executor = MagicMock()
        executor.execute.side_effect = [fill, None]  # fills on first pending check

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2, e3])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order], [], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        # executor only called once: at t2. Not again at t3 (order already gone)
        assert executor.execute.call_count == 1

    def test_multiple_pending_orders_same_symbol(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        order_1 = make_order(inst, quantity=100)
        order_2 = make_order(inst, quantity=200)
        fill_1 = make_fill(order_1)
        fill_2 = make_fill(order_2)

        executor = MagicMock()
        executor.execute.side_effect = [fill_1, fill_2]

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order_1, order_2], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        assert portfolio.apply_fill.call_count == 2

    def test_orders_for_different_symbols_dont_interfere(self):
        inst_a = make_instrument("AAPL")
        inst_b = make_instrument("MSFT")
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)

        e1 = make_event(inst_a, timestamp=t1)
        e2_a = make_event(inst_a, timestamp=t2)
        e2_b = make_event(inst_b, timestamp=t2)

        order_a = make_order(inst_a)
        order_b = make_order(inst_b)
        fill_a = make_fill(order_a)
        fill_b = make_fill(order_b)

        executor = MagicMock()
        executor.execute.side_effect = lambda order, event, portfolio: (
            fill_a if order.instrument.symbol == "AAPL" else fill_b
        )

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2_a, e2_b])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order_a, order_b], [], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        assert portfolio.apply_fill.call_count == 2


# ─────────────────────────────────────────────
# 6. Rebalancer Integration
# ─────────────────────────────────────────────

class TestRebalancer:
    def test_rebalancer_orders_are_queued(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        rebal_order = make_order(inst, quantity=50)
        fill = make_fill(rebal_order, timestamp=t2)

        rebalancer = MagicMock()
        rebalancer.should_rebalance.return_value = True
        rebalancer.generate_orders.return_value = [rebal_order]

        executor = MagicMock()
        executor.execute.return_value = fill

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        portfolio.prices = {}
        strategy = MagicMock()
        strategy.on_market.return_value = []

        engine = BacktestEngine(
            feed, strategy, portfolio, executor, rebalancer=rebalancer
        )
        engine.run()

        portfolio.apply_fill.assert_called_once_with(fill)

    def test_rebalancer_not_triggered_when_should_rebalance_false(self):
        inst = make_instrument()
        event = make_event(inst)

        rebalancer = MagicMock()
        rebalancer.should_rebalance.return_value = False

        feed = MagicMock()
        feed.stream.return_value = iter([event])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.return_value = []
        executor = MagicMock()

        engine = BacktestEngine(
            feed, strategy, portfolio, executor, rebalancer=rebalancer
        )
        engine.run()

        rebalancer.generate_orders.assert_not_called()


# ─────────────────────────────────────────────
# 7. Executor Crash Resilience
# ─────────────────────────────────────────────

class TestExecutorResilience:
    def test_executor_returning_none_does_not_crash_engine(self):
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        order = make_order(inst)

        engine, _, portfolio, _, _ = make_engine(
            events=[e1, e2],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.NEXT_BAR,
            executor_fills={inst.symbol: None},
        )
        engine.run()  # must not raise
        portfolio.apply_fill.assert_not_called()

    def test_executor_exception_does_not_crash_engine(self):
        """Engine should survive if executor.execute() raises."""
        inst = make_instrument()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(inst, timestamp=t1)
        e2 = make_event(inst, timestamp=t2)

        order = make_order(inst)

        executor = MagicMock()
        executor.execute.side_effect = RuntimeError("Simulated executor crash")

        feed = MagicMock()
        feed.stream.return_value = iter([e1, e2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[order], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)

        # Depending on your design this is either swallowed or re-raised.
        # Adjust the assertion to match your engine's contract.
        with pytest.raises(RuntimeError):
            engine.run()


# ─────────────────────────────────────────────
# 8. Options-Specific Behaviour
# ─────────────────────────────────────────────

class TestOptionsOrders:
    def test_option_order_uses_multiplier_in_fill(self):
        opt = make_option()
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        e1 = make_event(opt, close=3.50, timestamp=t1)
        e2 = make_event(opt, close=3.50, timestamp=t2)

        order = make_order(opt, quantity=-1.0)
        fill = make_fill(order, fill_price=3.50, timestamp=t2)

        engine, _, portfolio, _, executor = make_engine(
            events=[e1, e2],
            orders_per_event={0: [order]},
            fill_timing=FillTiming.NEXT_BAR,
            executor_fills={opt.symbol: fill},
        )
        engine.run()

        portfolio.apply_fill.assert_called_once_with(fill)
        assert fill.instrument.multiplier == 100.0

    def test_option_and_equity_orders_routed_correctly(self):
        equity = make_instrument("AAPL")
        option = make_option("AAPL240119C00150", underlying="AAPL")

        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)

        e_eq_1 = make_event(equity, timestamp=t1)
        e_eq_2 = make_event(equity, timestamp=t2)
        e_op_2 = make_event(option, timestamp=t2)

        eq_order = make_order(equity, quantity=100)
        op_order = make_order(option, quantity=-1)
        eq_fill = make_fill(eq_order, timestamp=t2)
        op_fill = make_fill(op_order, timestamp=t2)

        executor = MagicMock()
        executor.execute.side_effect = lambda order, event, portfolio: (
            eq_fill if order.instrument.symbol == "AAPL" else op_fill
        )

        feed = MagicMock()
        feed.stream.return_value = iter([e_eq_1, e_eq_2, e_op_2])
        portfolio = MagicMock()
        portfolio.equity = 25_000.0
        portfolio.cash = 25_000.0
        strategy = MagicMock()
        strategy.on_market.side_effect = [[eq_order, op_order], [], []]

        engine = BacktestEngine(feed, strategy, portfolio, executor)
        engine.run()

        assert portfolio.apply_fill.call_count == 2
