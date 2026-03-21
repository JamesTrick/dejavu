from collections import defaultdict

from dejavu.data.feed import DataFeed
from dejavu.execution.orders import ExecutionHandler
from dejavu.portfolio import Portfolio
from dejavu.portfolio.rebalancing.base import Rebalancer
from dejavu.schemas import FillTiming, MultiLegOrder, OrderType
from dejavu.strategy.base import Strategy


class BacktestEngine:
    """The Backtesting Engine is arguably the heart of Dejavu. It's incredibly performant, processing over 3.5 million
    bars per second. Given the speed, Dejavu can handle all sorts of data, making it quite flexible across the board.
    """
    def __init__(
        self,
        feed:      DataFeed,
        strategy:  Strategy,
        portfolio: Portfolio,
        executor:  ExecutionHandler,
        rebalancer: Rebalancer | None = None,
        fill_timing: FillTiming = FillTiming.NEXT_BAR
    ):
        """

        Args:
            feed:
            strategy:
            portfolio:
            executor:
            rebalancer:
            fill_timing: When you want the bars filled. Defaults to NextBar, but can be switched to SAME_BAR if
                dealing with intraday data.
        """
        self.feed      = feed
        self.strategy  = strategy
        self.portfolio = portfolio
        self.executor  = executor
        self.rebalancer = rebalancer
        self.fill_timing = fill_timing

    def run(self):
        portfolio = self.portfolio
        # Pending orders now just store (order, originating_event)
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
            sym = event.instrument.symbol
            # 1. Process Pending Orders
            pending = pending_by_symbol.get(sym)
            if pending:
                still_pending = []
                for order, originating_event in pending:
                    fill = executor.execute(order, event, portfolio)
                    if fill:
                        apply_fill(fill)
                    else:
                        still_pending.append((order, originating_event))

                if still_pending:
                    pending_by_symbol[sym] = still_pending
                else:
                    del pending_by_symbol[sym]

            # 2. Rebalancing
            if rebalancer and rebalancer.should_rebalance(event.timestamp, portfolio):
                for order in rebalancer.generate_orders(
                    timestamp=event.timestamp,
                    portfolio=portfolio,
                    target_weights={},
                    prices=portfolio.prices,
                ):
                    # Route by instrument.symbol
                    pending_by_symbol[order.instrument.symbol].append((order, event))

            # 3. Strategy reacts
            # We assume Strategy.on_market now just yields Orders directly, no meta dicts!
            for order in strategy.on_market(event):
                legs = order.legs if isinstance(order, MultiLegOrder) else [order]
                for o in legs:
                    if (
                            self.fill_timing == FillTiming.SAME_BAR
                            and o.order_type == OrderType.MARKET
                            and o.instrument.symbol == sym
                    ):
                        fill = executor.execute(o, event, portfolio)
                        if fill:
                            apply_fill(fill)
                        else:
                            # Couldn't fill even same-bar, queue it
                            pending_by_symbol[o.instrument.symbol].append((o, event))
                    else:
                        pending_by_symbol[o.instrument.symbol].append((o, event))

            # 4. Record History
            hist_timestamps.append(event.timestamp)
            hist_equity.append(portfolio.equity)
            hist_cash.append(portfolio.cash)


        portfolio.history = [
            {"timestamp": t, "equity": e, "cash": c}
            for t, e, c in zip(hist_timestamps, hist_equity, hist_cash, strict=True)
        ]
