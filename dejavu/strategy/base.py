from abc import ABC, abstractmethod

from dejavu.portfolio import Portfolio
from dejavu.schemas import MarketEvent, MultiLegOrder, Order, OrderType


class Strategy(ABC):
    """
    Strategies are the core of the Dejavu framework. They encapsulate the trading logic and are responsible for
    generating orders based on market events.

    A `Portfolio` can have any number of strategies.

    The main method to implement is `on_market` which takes a `MarketEvent` (a new datapoint) and returns a list of
    (order, meta) tuples to execute. Order can be a single Order or a MultiLegOrder (e.g. strangle, spread); for
    MultiLegOrder use meta['legs_meta'] = [dict, ...] for per-leg option meta. Meta can be {} for equity-only;
    for options include asset_class, underlying"""
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.orders: list[Order] = []

    @abstractmethod
    def on_market(self, event: MarketEvent) -> list[tuple[Order | MultiLegOrder, dict]]:
        """On market is the core event, it's what drives the strategy. Given a MarketEvent (datapoint),
        return a list of (order, meta) tuples to execute. Order can be a single Order or a MultiLegOrder
        (e.g. strangle, spread); for MultiLegOrder use meta['legs_meta'] = [dict, ...] for per-leg option meta.
        Meta can be {} for equity-only; for options include asset_class, underlying, strike, expiry, option_type.
        The engine will take care of filling and portfolio updates."""
        ...

    def buy(self, symbol, qty=1, order_type=OrderType.MARKET, **kwargs) -> Order:
        return Order(symbol=symbol, quantity=qty, order_type=order_type, **kwargs)

    def sell(self, symbol, qty=1, order_type=OrderType.MARKET, **kwargs) -> Order:
        return Order(symbol=symbol, quantity=-qty, order_type=order_type, **kwargs)

    def close(self, symbol, order_type=OrderType.MARKET, **kwargs) -> Order:
        position = self.portfolio.positions.get(symbol)
        if not position:
            raise ValueError(f"No open position for {symbol}")
        return Order(symbol=symbol, quantity=-position.quantity, order_type=order_type, **kwargs)

