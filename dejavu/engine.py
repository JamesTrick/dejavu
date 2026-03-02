from collections import defaultdict
from datetime import datetime

from dejavu.data.feed import DataFeed
from dejavu.execution.orders import SimulatedExecutionHandler
from dejavu.portfolio import Portfolio
from dejavu.portfolio.rebalancing.base import Rebalancer
from dejavu.schemas import MarketEvent, MultiLegOrder, Order
from dejavu.strategy.base import Strategy


class _RustEngineDriver:
    """Driver for RustBacktestEngine: provides next_event, get_pending, execute_order, etc."""

    def __init__(self, feed, strategy, portfolio, executor, rebalancer):
        self._feed_iter = iter(feed.stream())
        self.strategy = strategy
        self.portfolio = portfolio
        self.executor = executor
        self.rebalancer = rebalancer
        self.pending = defaultdict(list)

    def next_event(self):
        try:
            return next(self._feed_iter)
        except StopIteration:
            return None

    def get_pending(self, symbol):
        return self.pending.get(symbol, [])

    def execute_order(self, order, meta, event):
        fill = self.executor.execute(order, event)
        if fill:
            sym = order.symbol
            for i, (o, m) in enumerate(self.pending[sym]):
                if o is order and m is meta:
                    del self.pending[sym][i]
                    break
        return fill

    def should_rebalance(self, event):
        if not self.rebalancer:
            return False
        return self.rebalancer.should_rebalance(event.timestamp, self.portfolio)

    def get_rebalance_orders(self, event):
        if not self.rebalancer:
            return []
        return self.rebalancer.generate_orders(
            timestamp=event.timestamp,
            portfolio=self.portfolio,
            target_weights={},
            prices=self.portfolio.prices,
        )

    def get_strategy_orders(self, event):
        result = []
        for order, meta in self.strategy.on_market(event):
            if type(order) is MultiLegOrder:
                legs_meta = meta.get("legs_meta") or [{}] * len(order.legs)
                result.extend(
                    (leg, leg_meta) for leg, leg_meta in zip(order.legs, legs_meta)
                )
            else:
                result.append((order, meta))
        return result

    def add_pending(self, symbol, order, meta):
        self.pending[symbol].append((order, meta))

    def sync_portfolio(self, rust_portfolio):
        """Sync the wrapper's _rust to the engine's portfolio so rebalance/strategy see current state."""
        self.portfolio._rust = rust_portfolio
        self.portfolio.cash = rust_portfolio.cash

    def set_history(self, ts, equity, cash):
        self.portfolio.history = [
            {"timestamp": datetime.fromtimestamp(t), "equity": e, "cash": c}
            for t, e, c in zip(ts, equity, cash)
        ]


class BacktestEngine:
    def __init__(
        self,
        feed:      DataFeed,
        strategy:  Strategy,
        portfolio: Portfolio,
        executor:  SimulatedExecutionHandler,
        rebalancer: Rebalancer | None = None,
    ):
        self.feed      = feed
        self.strategy  = strategy
        self.portfolio = portfolio
        self.executor  = executor
        self.rebalancer = rebalancer

    def run(self, use_rust_engine: bool = True):
        portfolio = self.portfolio
        if use_rust_engine and getattr(portfolio, "_rust", None) is not None:
            try:
                from dejavu._core import RustBacktestEngine
                engine = RustBacktestEngine(portfolio.initial_capital)
                driver = _RustEngineDriver(
                    self.feed,
                    self.strategy,
                    portfolio,
                    self.executor,
                    self.rebalancer,
                )
                engine.run(driver)
                portfolio._rust = engine.portfolio
                portfolio.cash = engine.portfolio.cash
                return
            except ImportError:
                pass

        pending_by_symbol = defaultdict(list)

        rebalancer = self.rebalancer
        executor = self.executor
        strategy = self.strategy

        update_prices = portfolio.update_prices
        apply_fill = portfolio.apply_fill

        hist_timestamps = []
        hist_equity = []
        hist_cash = []

        for event in self.feed.stream():
            update_prices(event)
            sym = event.symbol

            # Process Pending Orders
            if pending_by_symbol[sym]:
                still_pending = []
                for order, meta, _ in pending_by_symbol[sym]:
                    fill = executor.execute(order, event)
                    if fill:
                        apply_fill(fill, meta)
                    else:
                        still_pending.append((order, meta, event))

                if still_pending:
                    pending_by_symbol[sym] = still_pending
                else:
                    del pending_by_symbol[sym] # Keep dict lean

            # Rebalancing
            if rebalancer and rebalancer.should_rebalance(event.timestamp, portfolio):
                for order in rebalancer.generate_orders(
                    timestamp=event.timestamp,
                    portfolio=portfolio,
                    target_weights={},
                    prices=portfolio.prices,
                ):
                    pending_by_symbol[order.symbol].append((order, {}, event))

            # Strategy reacts
            # STRONGLY RECOMMENDED: Refactor strategy to return flat lists of simple Orders
            for order, meta in strategy.on_market(event):
                # If you must keep the MultiLeg logic, do it here, but it's better removed
                if type(order) is MultiLegOrder: # type() is slightly faster than isinstance()
                    legs_meta = meta.get("legs_meta") or [{}] * len(order.legs)
                    for leg, leg_meta in zip(order.legs, legs_meta):
                        pending_by_symbol[leg.symbol].append((leg, leg_meta, event))
                else:
                    pending_by_symbol[order.symbol].append((order, meta, event))

            # Fast History Tracking (Append to flat lists, zip at the end if needed)
            hist_timestamps.append(event.timestamp)
            hist_equity.append(portfolio.equity) # (Still a bottleneck if equity is calculated dynamically)
            hist_cash.append(portfolio.cash)

        # Reconstruct history format at the very end
        portfolio.history = [
            {"timestamp": t, "equity": e, "cash": c}
            for t, e, c in zip(hist_timestamps, hist_equity, hist_cash)
        ]
