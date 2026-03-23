import logging
from abc import ABC, abstractmethod

from dejavu.execution.commission import CommissionModel
from dejavu.execution.margin import RealisticRegTModel
from dejavu.execution.validators import MarginValidator, OrderValidator
from dejavu.portfolio import Portfolio
from dejavu.schemas import (
    AssetClass,
    EventType,
    FillEvent,
    MarketEvent,
    Order,
    OrderType,
)

logger = logging.getLogger(__name__)


class ExecutionHandler(ABC):
    def _get_fill_price(self, order: Order, market: MarketEvent) -> float | None:
        """Shared price discovery logic for all handlers."""
        match order.order_type:
            case OrderType.MARKET:
                return float(market.close)
            case OrderType.LIMIT:
                if order.limit_price is None:
                    logger.warning(
                        f"Limit order missing price for {order.instrument.symbol}"
                    )
                    return None
                if order.quantity > 0 and market.low <= order.limit_price:
                    return float(order.limit_price)
                elif order.quantity < 0 and market.high >= order.limit_price:
                    return float(order.limit_price)
                return None  # condition not met
            case _:
                raise ValueError(f"Unsupported order type {order.order_type}")

    @abstractmethod
    def execute(
        self, order: Order, market: MarketEvent, portfolio: Portfolio
    ) -> FillEvent | None: ...


class SlippageModel(ABC):
    @abstractmethod
    def apply(self, order: Order, price: float, market: MarketEvent) -> float: ...


class VolumeWeightedSlippage(SlippageModel):
    """Volume-weighted slippage model.

    Args:
        SlippageModel (_type_): _description_
    """

    def __init__(self, impact_factor: float = 0.1):
        """_summary_

        Args:
            impact_factor (float, optional): The impact factor for the slippage calculation. Defaults to 0.1.
        """
        self.impact_factor = impact_factor

    def apply(self, order: Order, price: float, market: MarketEvent) -> float:
        volume = market.volume if market.volume is not None else 0
        participation = abs(order.quantity) / max(volume, 1)
        slippage = price * self.impact_factor * participation
        return price + slippage if order.quantity > 0 else price - slippage


class NoSlippage(SlippageModel):
    """Simple slippage that assumes no slippage occurs. In other words, the market price is the fill price."""

    def apply(self, order: Order, price: float, market: MarketEvent) -> float:  # noqa: ARG002
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
    def __init__(
        self,
        commission: CommissionModel,
        validators: list[OrderValidator] | None = None,
    ):
        self.commission = commission
        self.validators = validators or []

    def execute(
        self,
        order: Order,
        market: MarketEvent,
        portfolio: Portfolio,  # noqa: ARG002
    ) -> FillEvent | None:
        fill_price = self._get_fill_price(order, market)
        if fill_price is None:
            return None
        commission = self.commission.calculate(
            order, fill_price, order.instrument.multiplier
        )

        return FillEvent(
            type=EventType.FILL,
            timestamp=market.timestamp,
            order_id=order.order_id,
            instrument=order.instrument,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=commission,
        )


class SimulatedExecutionHandler(ExecutionHandler):
    def __init__(
        self,
        slippage: SlippageModel,
        commission: CommissionModel,
        validators: list[OrderValidator] | None = None,
    ):
        self.slippage = slippage
        self.commission = commission
        self.validators = validators or []

    def execute(
        self, order: Order, market: MarketEvent, portfolio: Portfolio
    ) -> FillEvent | None:
        try:
            fill_price = self._get_fill_price(order, market)
            if fill_price is None:
                return None
            fill_price = self.slippage.apply(order, fill_price, market)

            for validator in self.validators:
                valid, reason = validator.validate(order, fill_price, portfolio)
                if not valid:
                    logger.warning(f"Order rejected: {reason}")
                    return None

            commission = self.commission.calculate(
                order, fill_price, order.instrument.multiplier
            )

            return FillEvent(
                type=EventType.FILL,
                timestamp=market.timestamp,
                order_id=order.order_id,
                instrument=order.instrument,
                quantity=float(order.quantity),
                fill_price=float(fill_price),
                commission=float(commission),
            )

        except Exception as e:
            print(f"\n[EXECUTOR CRASH] Failed to execute {order.instrument.symbol}!")
            print(f"Error: {e}")
            import traceback

            traceback.print_exc()
            return None


class MarginAwareExecutionHandler(ExecutionHandler):
    def __init__(
        self,
        slippage: SlippageModel,
        commission: CommissionModel,
        margin_model: RealisticRegTModel,
        validators: list[OrderValidator] | None = None,
    ):
        self.slippage = slippage
        self.commission = commission
        self.margin_model = margin_model
        # Always include margin validator, plus any extras
        self.validators = [MarginValidator(margin_model)] + (validators or [])

    def execute(
        self, order: Order, market: MarketEvent, portfolio: Portfolio
    ) -> FillEvent | None:
        try:
            fill_price = self._get_fill_price(order, market)
            if fill_price is None:
                return None

            fill_price = self.slippage.apply(order, fill_price, market)

            for validator in self.validators:
                valid, reason = validator.validate(order, fill_price, portfolio)
                if not valid:
                    logger.warning(f"Order rejected: {reason}")
                    return None

            commission = self.commission.calculate(
                order, fill_price, order.instrument.multiplier
            )
            return FillEvent(
                type=EventType.FILL,
                timestamp=market.timestamp,
                order_id=order.order_id,
                instrument=order.instrument,
                quantity=float(order.quantity),
                fill_price=float(fill_price),
                commission=float(commission),
            )
        except Exception as e:
            logger.error(
                f"[EXECUTOR CRASH] {order.instrument.symbol}: {e}", exc_info=True
            )
            return None
