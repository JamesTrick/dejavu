from dejavu.portfolio import Portfolio
from dejavu.schemas import AssetClass, MarketEvent, Option, Order, OrderType
from dejavu.strategy.base import Strategy


class CoveredCallStrategy(Strategy):

    def __init__(self, portfolio: Portfolio, underlying: str):
        super().__init__(portfolio)
        self.underlying = underlying
        self.bought_stock = False
        self.short_call: str | None = None
        # Track pending orders so we don't double-queue before fills land
        self._pending_call: str | None = None

    def _has_open_short_call(self) -> bool:
        # Check both filled positions AND pending-but-unfilled orders
        in_portfolio = (
                self.short_call is not None
                and self.short_call in self.portfolio.positions
        )
        return in_portfolio or self._pending_call is not None

    def on_market(self, event: MarketEvent) -> list[Order]:
        orders = []

        # Clear pending tracker once the fill has landed in the portfolio
        if (
            self._pending_call is not None
            and self._pending_call in self.portfolio.positions
        ):
            self._pending_call = None

        inst = event.instrument

        # ── Buy stock once ────────────────────────────────────────────────
        if (
            not self.bought_stock
            and inst.asset_class == AssetClass.EQUITY
            and inst.symbol == self.underlying
        ):
            orders.append(
                Order(
                    instrument=inst,
                    quantity=100.0,
                    order_type=OrderType.MARKET,
                )
            )
            self.bought_stock = True
            print(f"  [BUY STOCK] {self.underlying} @ ~{event.close:.2f}")

        # ── Sell OTM call if none open or pending ─────────────────────────
        if (
                self.bought_stock
                and not self._has_open_short_call()
                and isinstance(inst, Option)
                and inst.option_type == "C"
                and inst.underlying == self.underlying
                and 0.20 <= (event.delta or 0.0) <= 0.40
                and event.close >= 0.50  # pre-slippage price floor
        ):
            self._pending_call = inst.symbol
            self.short_call = inst.symbol

            orders.append(
                Order(
                    instrument=inst,
                    quantity=-1.0,  # Sell 1 contract
                    order_type=OrderType.MARKET,
                )
            )
            print(
                f"  [SELL CALL] {inst.symbol} | strike={inst.strike} "
                f"delta={event.delta:.2f} price={event.close:.2f} "
                f"expiry={inst.expiry.date()}"
            )

        return orders
