from abc import ABC, abstractmethod

from dejavu.schemas import AssetClass, Order


class CommissionModel(ABC):
    @abstractmethod
    def calculate(
        self, order: Order, fill_price: float, multiplier: float  # noqa: ARG002
    ) -> float: ...


class PerContractCommission(CommissionModel):
    """This commission scheme takes a rate, percentage and applies it the order quantity. Most commonly would be used
    for Options contracts or derivatives.
    """
    def __init__(self, rate: float = 0.65):
        """

        Args:
            rate:
        """
        self.rate = rate

    def calculate(self, order: Order, fill_price: float, multiplier: float) -> float:
        return abs(order.quantity) * self.rate


class PercentageOfNotionalCommission(CommissionModel):
    """This commission structure takes a flat % of the total order value (pre-commission).
    """
    def __init__(self, rate: float = 0.001):
        self.rate = rate

    def calculate(self, order: Order, fill_price: float, multiplier: float) -> float:
        notional = abs(order.quantity) * fill_price * multiplier
        return notional * self.rate


class TieredPerShareCommission(CommissionModel):
    """This one models brokers such as Interactive Brokers, where there is a flat-fee or minimum, that is capped at a
    certain percentage of the trade's value.
    """

    def __init__(
        self,
        rate: float = 0.005,
        minimum: float = 1.00,
        max_pct_notional: float = 0.01,
    ):
        self.rate = rate
        self.minimum = minimum
        self.max_pct_notional = max_pct_notional

    def calculate(self, order: Order, fill_price: float, multiplier: float) -> float:
        notional = abs(order.quantity) * fill_price * multiplier
        per_share = abs(order.quantity) * self.rate
        cap = notional * self.max_pct_notional
        return max(self.minimum, min(per_share, cap))


class AssetClassCommission(CommissionModel):
    """This wrapper allows you to route different commission structures based on the asset classes.

    The usage of it is relatively straight-froward:

    ```python
    commission_model = AssetClassCommission(
        models={
            AssetClass.EQUITY: TieredPerShareCommission(
                rate=0.005,
                minimum=1.00,
                max_pct_notional=0.01,
            ),
            AssetClass.OPTION: PerContractCommission(rate=0.65),
        },
        default=PerContractCommission(rate=0.65),
    )
    ```
    """
    def __init__(self, models: dict[AssetClass, CommissionModel], default: CommissionModel | None = None):
        self.models = models
        self.default = default

    def calculate(self, order: Order, fill_price: float, multiplier: float) -> float:
        model = self.models.get(order.instrument.asset_class, self.default)
        if model is None:
            raise ValueError(
                f"No commission model for asset class {order.instrument.asset_class} "
                "and no default was set."
            )
        return model.calculate(order, fill_price, multiplier)


class SymbolCommission(CommissionModel):
    """Per-symbol overrides, falls back to a default model."""

    def __init__(self, overrides: dict[str, CommissionModel], default: CommissionModel):
        self.overrides = overrides
        self.default = default

    def calculate(self, order: Order, fill_price: float, multiplier: float) -> float:
        model = self.overrides.get(order.instrument.symbol, self.default)
        return model.calculate(order, fill_price, multiplier)
