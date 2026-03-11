from abc import ABC, abstractmethod

from dejavu.execution.commission import CommissionModel
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
    def execute(self, order: Order, market: MarketEvent) -> FillEvent | None:
        ...


class SlippageModel(ABC):
    @abstractmethod
    def apply(self, order: Order, price: float, market: MarketEvent) -> float:
        ...


class VolumeWeightedSlippage(SlippageModel):
    def __init__(self, impact_factor: float = 0.1):
        self.impact_factor = impact_factor

    def apply(self, order: Order, price: float, market: MarketEvent) -> float:
        # Safeguard against 0 volume to prevent math errors
        participation = abs(order.quantity) / max(market.volume, 1)
        slippage = price * self.impact_factor * participation
        return price + slippage if order.quantity > 0 else price - slippage


class NoSlippage(SlippageModel):
    """Simple slippage that assumes no slippage occurs."""
    def apply(self, order: Order, price: float, market: MarketEvent) -> float:
        return price


class AssetClassSlippage(SlippageModel):
    def __init__(
        self,
        models: dict[AssetClass, SlippageModel],
        default: SlippageModel | None = None,
    ):
        self.models = models
        self.default = default

    def apply(self, order: Order, price: float, market: MarketEvent) -> float:
        # FIX: Look up the asset class via the instrument!
        model = self.models.get(order.instrument.asset_class, self.default)
        if model is None:
            raise ValueError(
                f"No slippage model for asset class {order.instrument.asset_class} "
                "and no default was set."
            )
        return model.apply(order, price, market)


class CommissionOnlyHandler(ExecutionHandler):
    def __init__(self, commission: CommissionModel):
        self.commission = commission

    def execute(self, order: Order, market: MarketEvent) -> FillEvent | None:
        if order.order_type == OrderType.MARKET:
            fill_price = market.close
        elif order.order_type == OrderType.LIMIT:
            if order.quantity > 0 and market.low <= order.limit_price:
                fill_price = order.limit_price
            elif order.quantity < 0 and market.high >= order.limit_price:
                fill_price = order.limit_price
            else:
                return None   # not filled
        else:
            return None

        # FIX: The instrument already knows its multiplier!
        multiplier = order.instrument.multiplier
        commission = self.commission.calculate(order, fill_price, multiplier)

        # FIX: Construct the new FillEvent format
        return FillEvent(
            type=EventType.FILL,
            timestamp=market.timestamp,
            order_id=order.order_id,
            instrument=order.instrument,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=commission,
            multiplier=multiplier,
        )


class SimulatedExecutionHandler(ExecutionHandler):
    def __init__(
            self,
            slippage: SlippageModel,
            commission: CommissionModel,
    ):
        self.slippage = slippage
        self.commission = commission

    def execute(self, order: Order, market: MarketEvent) -> FillEvent | None:
        try:
            if order.instrument.symbol != market.instrument.symbol:
                return None

            if order.order_type == OrderType.MARKET:
                # Ensure market.close is a clean float (Pandas can sometimes sneak in numpy floats)
                clean_price = float(market.close)
                fill_price = self.slippage.apply(order, clean_price, market)

            elif order.order_type == OrderType.LIMIT:
                if order.limit_price is None:
                    print(f"ERROR: Limit order missing price for {order.instrument.symbol}")
                    return None

                if order.quantity > 0 and market.low <= order.limit_price:
                    fill_price = order.limit_price
                elif order.quantity < 0 and market.high >= order.limit_price:
                    fill_price = order.limit_price
                else:
                    return None  # Limit condition not met yet
            else:
                print(f"ERROR: Unknown order type {order.order_type}")
                return None

            multiplier = order.instrument.multiplier
            commission = self.commission.calculate(order, fill_price, multiplier)

            return FillEvent(
                type=EventType.FILL,
                timestamp=market.timestamp,
                order_id=order.order_id,
                instrument=order.instrument,
                quantity=float(order.quantity),
                fill_price=float(fill_price),
                commission=float(commission),
                multiplier=multiplier,
            )

        except Exception as e:
            print(f"\n[EXECUTOR CRASH] Failed to execute {order.instrument.symbol}!")
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None
