from datetime import datetime
from typing import Optional

import pandas as pd

from ..schemas import AssetClass, FillEvent, MarketEvent, OptionMarketEvent, Position


class Portfolio:
    """Manages your portfolio state, including cash, positions, and equity. It also handles applying
    fills and updating market prices.
    """
    def __init__(self, initial_capital: float):
        """

        Args:
            initial_capital: Initial Capital to start with.
        """
        self.cash             = initial_capital
        self.initial_capital  = initial_capital
        self.positions:       dict[str, Position] = {}
        self._last_prices:    dict[str, float]    = {}
        self.history:         list[dict]          = []
        self.trades:          list[dict]          = []

    @property
    def equity(self) -> float:
        """Calculate total equity = cash + market value of positions."""
        mv = sum(
            pos.market_value(self._last_prices.get(sym, pos.avg_cost))
            for sym, pos in self.positions.items()
        )
        return self.cash + mv

    @property
    def trade_journal(self) -> pd.DataFrame:
        """Returns a DataFrame of all trades."""
        return pd.DataFrame(self.trades)

    def update_prices(self, event: MarketEvent):
        self._last_prices[event.symbol] = event.close
        if event.asset_class == AssetClass.OPTION:
            assert isinstance(event, OptionMarketEvent)
            self._expire_options(event.timestamp)

    def _expire_options(self, now: datetime):
        expired = [
            sym for sym, pos in self.positions.items()
            if pos.asset_class == AssetClass.OPTION
            and pos.expiry is not None
            and pos.expiry.date() <= now.date()
        ]
        for sym in expired:
            pos = self.positions.pop(sym)
            underlying_price = self._last_prices.get(pos.underlying or "", 0)
            if pos.option_type == "C":
                intrinsic = max(0, underlying_price - (pos.strike or 0))
            else:
                intrinsic = max(0, (pos.strike or 0) - underlying_price)
            payout = intrinsic * pos.quantity * pos.multiplier
            self.cash += payout
            print(
                f"  [EXPIRY] {sym} expired | "
                f"intrinsic={intrinsic:.2f} payout={payout:.2f}"
            )

    def apply_fill(self, fill: FillEvent, position_meta: Optional[dict] = None):
        self.cash -= fill.quantity * fill.fill_price * fill.multiplier
        self.cash -= fill.commission

        if fill.symbol in self.positions:
            pos = self.positions[fill.symbol]
            new_qty = pos.quantity + fill.quantity
            if abs(new_qty) < 1e-9:
                del self.positions[fill.symbol]
            else:
                # Update average cost
                if (pos.quantity > 0) == (fill.quantity > 0):
                    pos.avg_cost = (
                        (pos.quantity * pos.avg_cost + fill.quantity * fill.fill_price)
                        / new_qty
                    )
                pos.quantity = new_qty
        else:
            meta = position_meta or {}
            self.positions[fill.symbol] = Position(
                symbol=fill.symbol,
                quantity=fill.quantity,
                avg_cost=fill.fill_price,
                asset_class=meta.get("asset_class", AssetClass.EQUITY),
                underlying=meta.get("underlying"),
                strike=meta.get("strike"),
                expiry=meta.get("expiry"),
                option_type=meta.get("option_type"),
            )

        self.trades.append({
            "timestamp":  fill.timestamp,
            "symbol":     fill.symbol,
            "quantity":   fill.quantity,
            "fill_price": fill.fill_price,
            "commission": fill.commission,
        })

