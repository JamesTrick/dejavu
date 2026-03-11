from abc import ABC, abstractmethod

from dejavu.portfolio import Portfolio
from dejavu.schemas import Instrument, MarketEvent, MultiLegOrder, Order, OrderType


class Strategy(ABC):
    """
    Strategies are the core of the Dejavu framework. They encapsulate the trading logic and are responsible for
    generating orders based on market events.

    A `Portfolio` can have any number of strategies.
    """
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.orders: list[Order] = []

    @abstractmethod
    def on_market(self, event: MarketEvent) -> list[Order | MultiLegOrder]:
        """On market is the core event, it's what drives the strategy. Given a MarketEvent (datapoint),
        return a list of orders to execute. Order can be a single Order or a MultiLegOrder.

        The engine will take care of filling and portfolio updates based on the Order's Instrument.

        Args:
            event: The MarketEvent, or new data coming into the strategy.
        """
        ...

    def buy(self, instrument: Instrument, qty: float = 1.0, order_type=OrderType.MARKET, **kwargs) -> Order:
        """This creates a buy order. Used to go long on an underlying asset."""
        return Order(instrument=instrument, quantity=qty, order_type=order_type, **kwargs)

    def sell(self, instrument: Instrument, qty: float = 1.0, order_type=OrderType.MARKET, **kwargs) -> Order:
        """This creates a sell order. Used to go short on an underlying asset."""
        return Order(instrument=instrument, quantity=-qty, order_type=order_type, **kwargs)

    def close(self, instrument: Instrument, order_type=OrderType.MARKET, **kwargs) -> Order:
        """Given a position, create an order to close the position."""
        position = self.portfolio.positions.get(instrument.symbol)
        if not position:
            raise ValueError(f"No open position for {instrument.symbol}")
        return Order(instrument=instrument, quantity=-position.quantity, order_type=order_type, **kwargs)
