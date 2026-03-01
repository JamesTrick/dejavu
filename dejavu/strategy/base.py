from abc import ABC, abstractmethod

from dejavu.portfolio import Portfolio
from dejavu.schemas import MarketEvent, Order, OrderType


class Strategy(ABC):
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.orders: list[Order] = []

    @abstractmethod
    def on_market(self, event: MarketEvent) -> list[Order]:
        """On market is the core event, it's what drives the strategy. Given a MarketEvent (datapoint), we return a list
        of orders to execute. The engine will take care of the rest (filling, portfolio updates, etc)."""
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
