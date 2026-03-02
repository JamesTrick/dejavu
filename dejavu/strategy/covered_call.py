
from dejavu.portfolio import Portfolio
from dejavu.schemas import AssetClass, MarketEvent, OptionMarketEvent, Order, OrderType
from dejavu.strategy.base import Strategy


class CoveredCallStrategy(Strategy):

    def __init__(self, portfolio: Portfolio, underlying: str):
        """A simple implementation of a covered call strategy. Covered calls involve holding a long position in an underlying
        asset class, whilst simultaneously selling (writing) call options on that same asset.

        The goal is to generate income from the option premiums, which can help to boost returns or provide a cushion
        against downside risk. However, the tradeoff is that the upside potential is capped, since the short call may be
        exercised if the underlying price rises above the strike price.

        Args:
            portfolio:
            underlying:
        """
        super().__init__(portfolio)
        self.underlying    = underlying
        self.bought_stock  = False
        self.short_call:   str | None = None
        # Track pending orders so we don't double-queue before fills land
        self._pending_call: str | None = None

    def _has_open_short_call(self) -> bool:
        # Check both filled positions AND pending-but-unfilled orders
        in_portfolio = (
            self.short_call is not None
            and self.short_call in self.portfolio.positions
        )
        return in_portfolio or self._pending_call is not None

    def on_market(self, event: MarketEvent) -> list[tuple[Order, dict]]:
        orders = []

        # Clear pending tracker once the fill has landed in the portfolio
        if (
            self._pending_call is not None
            and self._pending_call in self.portfolio.positions
        ):
            self._pending_call = None

        # ── Buy stock once ────────────────────────────────────────────────
        if (
            not self.bought_stock
            and event.asset_class == AssetClass.EQUITY
            and event.symbol == self.underlying
        ):
            orders.append((
                Order(
                    symbol=self.underlying,
                    quantity=100,
                    order_type=OrderType.MARKET,
                    asset_class=AssetClass.EQUITY,
                ),
                {"asset_class": AssetClass.EQUITY},
            ))
            self.bought_stock = True
            print(f"  [BUY STOCK] {self.underlying} @ ~{event.close:.2f}")

        # ── Sell OTM call if none open or pending ─────────────────────────
        if (
            self.bought_stock
            and not self._has_open_short_call()
            and isinstance(event, OptionMarketEvent)
            and event.option_type == "C"
            and event.underlying == self.underlying
            and 0.20 <= event.delta <= 0.40
            and event.close >= 0.50          # pre-slippage price floor
        ):
            self._pending_call = event.symbol
            self.short_call    = event.symbol
            orders.append((
                Order(
                    symbol=event.symbol,
                    quantity=-1,
                    order_type=OrderType.MARKET,
                    asset_class=AssetClass.OPTION,
                ),
                {
                    "asset_class": AssetClass.OPTION,
                    "underlying":  event.underlying,
                    "strike":      event.strike,
                    "expiry":      event.expiry,
                    "option_type": event.option_type,
                },
            ))
            print(
                f"  [SELL CALL] {event.symbol} | strike={event.strike} "
                f"delta={event.delta:.2f} price={event.close:.2f} "
                f"expiry={event.expiry.date()}"
            )

        return orders
