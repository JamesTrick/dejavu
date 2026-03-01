from typing import Optional

from dejavu.data.feed import DataFeed
from dejavu.execution.orders import SimulatedExecutionHandler
from dejavu.portfolio import Portfolio
from dejavu.portfolio.rebalancing.base import Rebalancer
from dejavu.schemas import MarketEvent, Order
from dejavu.strategy.base import Strategy


class BacktestEngine:
    def __init__(
        self,
        feed:      DataFeed,
        strategy:  Strategy,
        portfolio: Portfolio,
        executor:  SimulatedExecutionHandler,
        rebalancer: Optional[Rebalancer] = None,
    ):
        self.feed      = feed
        self.strategy  = strategy
        self.portfolio = portfolio
        self.executor  = executor
        self.rebalancer = rebalancer

    def run(self):
        pending_by_symbol: dict[str, list[tuple[Order, dict, MarketEvent]]] = {}

        for event in self.feed.stream():
            self.portfolio.update_prices(event)

            if event.symbol in pending_by_symbol:
                still_pending = []
                for order, meta, _ in pending_by_symbol[event.symbol]:
                    fill = self.executor.execute(order, event)
                    if fill:
                        self.portfolio.apply_fill(fill)
                    else:
                        still_pending.append((order, meta, event))
                pending_by_symbol[event.symbol] = still_pending

            # Rebalancing
            if self.rebalancer and self.rebalancer.should_rebalance(event.timestamp, self.portfolio):
                rebalancing_orders = self.rebalancer.generate_orders(
                    timestamp=event.timestamp,
                    portfolio=self.portfolio,
                    target_weights={},
                    prices=self.portfolio.prices,
                )
                for order in rebalancing_orders:
                    if order.symbol not in pending_by_symbol:
                        pending_by_symbol[order.symbol] = []
                    pending_by_symbol[order.symbol].append((order, {}, event))

            # Strategy reacts
            new_orders = self.strategy.on_market(event)
            for order, meta in new_orders:
                if order.symbol not in pending_by_symbol:
                    pending_by_symbol[order.symbol] = []
                pending_by_symbol[order.symbol].append((order, meta, event))

            self.portfolio.history.append({
                "timestamp": event.timestamp,
                "equity":    self.portfolio.equity,
                "cash":      self.portfolio.cash,
            })
