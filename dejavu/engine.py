from collections import defaultdict

from dejavu.data.feed import DataFeed
from dejavu.execution.orders import SimulatedExecutionHandler
from dejavu.portfolio import Portfolio
from dejavu.portfolio.rebalancing.base import Rebalancer
from dejavu.schemas import MarketEvent, MultiLegOrder, Order
from dejavu.strategy.base import Strategy


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

    def run(self):
        pending_by_symbol = defaultdict(list)

        rebalancer = self.rebalancer
        portfolio = self.portfolio
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
