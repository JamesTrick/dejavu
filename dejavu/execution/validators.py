from abc import ABC, abstractmethod

from dejavu.execution.margin import RealisticRegTModel
from dejavu.portfolio import Portfolio
from dejavu.schemas import Order


class OrderValidator(ABC):
    @abstractmethod
    def validate(
        self, order: Order, fill_price: float, portfolio: Portfolio
    ) -> tuple[bool, str | None]:
        """
        Returns (is_valid, reason_if_rejected)
        """
        ...


class CashValidator(OrderValidator):
    def validate(
        self, order: Order, fill_price: float, portfolio: Portfolio
    ) -> tuple[bool, str | None]:
        if order.quantity <= 0:
            return True, None  # sells/closes don't need cash check

        cost = fill_price * order.quantity * order.instrument.multiplier
        if cost > portfolio.cash:
            return False, (
                f"Insufficient cash for {order.instrument.symbol}: "
                f"need ${cost:,.2f}, have ${portfolio.cash:,.2f}"
            )
        return True, None


class MarginValidator(OrderValidator):
    def __init__(self, margin_model: RealisticRegTModel):
        self.margin_model = margin_model

    def validate(
        self, order: Order, fill_price: float, portfolio: Portfolio
    ) -> tuple[bool, str | None]:
        used = self.margin_model.calculate_used_margin(portfolio)
        required = self.margin_model.calculate_order_margin(order, fill_price)
        available = portfolio.cash - used

        if required > available:
            return False, (
                f"Insufficient margin for {order.instrument.symbol}: "
                f"need ${required:,.2f}, available ${available:,.2f}"
            )
        return True, None


class ShortValidator(OrderValidator):
    """Prevents shorting if not explicitly allowed."""

    def __init__(self, allow_short: bool = False):
        self.allow_short = allow_short

    def validate(
        self,
        order: Order,
        fill_price: float,  # noqa: ARG002
        portfolio: Portfolio,  # noqa: ARG002
    ) -> tuple[bool, str | None]:
        if order.quantity < 0 and not self.allow_short:
            existing = portfolio.positions.get(order.instrument.symbol)
            is_closing = existing and existing.quantity > 0
            if not is_closing:
                return (
                    False,
                    f"Short selling not permitted for {order.instrument.symbol}",
                )
        return True, None
