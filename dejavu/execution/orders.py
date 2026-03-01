from abc import ABC, abstractmethod
from typing import Optional

from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    MarketEvent,
    Order,
    OrderType,
)


class ExecutionHandler(ABC):
    @abstractmethod
    def execute(self, order: Order, market: MarketEvent) -> Optional[FillEvent]:
        ...


class SlippageModel(ABC):
    @abstractmethod
    def apply(self, price: float, quantity: float, volume: float) -> float:
        ...


class VolumeWeightedSlippage(SlippageModel):
    def __init__(self, impact_factor: float = 0.1):
        self.impact_factor = impact_factor

    def apply(self, price: float, quantity: float, volume: float) -> float:
        participation = abs(quantity) / max(volume, 1)
        slippage = price * self.impact_factor * participation
        return price + slippage if quantity > 0 else price - slippage


class SimulatedExecutionHandler(ExecutionHandler):
    def __init__(
        self,
        slippage: SlippageModel,
        commission_per_contract: float = 0.65,
    ):
        self.slippage = slippage
        self.commission = commission_per_contract

    def execute(self, order: Order, market: MarketEvent) -> Optional[FillEvent]:
        if order.order_type == OrderType.MARKET:
            fill_price = self.slippage.apply(
                market.close, order.quantity, market.volume
            )
        elif order.order_type == OrderType.LIMIT:
            if order.quantity > 0 and market.low <= order.limit_price:
                fill_price = order.limit_price
            elif order.quantity < 0 and market.high >= order.limit_price:
                fill_price = order.limit_price
            else:
                return None   # not filled
        else:
            return None

        multiplier = 100 if order.asset_class == AssetClass.OPTION else 1
        commission = abs(order.quantity) * self.commission

        return FillEvent(
            type=EventType.FILL,
            timestamp=market.timestamp,
            symbol=order.symbol,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=commission,
            multiplier=multiplier,
        )
