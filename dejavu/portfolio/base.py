from datetime import datetime, date
from typing import Any

import pandas as pd

from ..schemas import AssetClass, FillEvent, MarketEvent, OptionMarketEvent, Position


def _rust_portfolio(initial_capital: float):
    """Return RustPortfolio if the extension is available."""
    try:
        from dejavu._core import RustPortfolio
        return RustPortfolio(initial_capital)
    except ImportError:
        return None


class Portfolio:
    """Manages your portfolio state, including cash, positions, and equity. It also handles applying
    fills and updating market prices.
    """
    def __init__(self, initial_capital: float, use_rust: bool = True):
        """

        Args:
            initial_capital: Initial Capital to start with.
            use_rust: If True and the Rust extension is available, use the Rust backend for performance.
        """
        self._use_rust = use_rust
        self._rust = _rust_portfolio(initial_capital) if use_rust else None

        if self._rust is not None:
            self.cash = initial_capital
            self.initial_capital = initial_capital
            self._last_prices = {}
            self._position_value = 0.0
            self._last_expiry_check = None
            self._trade_timestamps = []
            self._trade_symbols = []
            self._trade_qtys = []
            self._trade_prices = []
            self._trade_comms = []
            self.history = []
            return

        self.cash             = initial_capital
        self.initial_capital  = initial_capital
        self._positions:      dict[str, Position] = {}
        self._last_prices:    dict[str, float]    = {}

        self._position_value = 0.0
        self._last_expiry_check: date | None = None

        self._trade_timestamps = []
        self._trade_symbols = []
        self._trade_qtys = []
        self._trade_prices = []
        self._trade_comms = []
        self.history = []

    @property
    def positions(self) -> dict[str, Any]:
        """Current positions (symbol -> Position). When using Rust, this is a live view from the backend."""
        if self._rust is not None:
            return self._rust.get_positions()
        return self._positions

    @property
    def equity(self) -> float:
        """Calculate total equity = cash + market value of positions."""
        if self._rust is not None:
            return self._rust.equity
        mv = sum(
            pos.market_value(self._last_prices.get(sym, pos.avg_cost))
            for sym, pos in self.positions.items()
        )
        return self.cash + mv

    @property
    def prices(self) -> dict[str, float]:
        """Read-only view of last known price per symbol (for rebalancers and reporting)."""
        if self._rust is not None:
            return dict(self._rust.prices())
        return self._last_prices

    @property
    def trade_journal(self) -> pd.DataFrame:
        """Returns a DataFrame of all trades."""
        if self._rust is not None:
            d = self._rust.trade_journal()
            return pd.DataFrame({
                "timestamp": d["timestamp"],
                "symbol": d["symbol"],
                "quantity": d["quantity"],
                "fill_price": d["fill_price"],
                "commission": d["commission"],
            })
        return pd.DataFrame({
            "timestamp": self._trade_timestamps,
            "symbol": self._trade_symbols,
            "quantity": self._trade_qtys,
            "fill_price": self._trade_prices,
            "commission": self._trade_comms,
        })

    def underlying_view(self) -> dict[str, dict[str, Any]]:
        """Minimal view of underlyings for reporting and risk: last price and option positions per underlying.
        Keys are underlying symbols (equity symbols appear as their own underlying; option positions
        are grouped by their underlying). Each value has 'price' (last known), 'option_symbols' (list of
        option position symbols on this underlying), and 'equity_position' (Position if we hold equity, else None).
        """
        if self._rust is not None:
            return dict(self._rust.underlying_view())
        view: dict[str, dict[str, Any]] = {}
        for sym, pos in self.positions.items():
            if pos.asset_class == AssetClass.EQUITY:
                view.setdefault(sym, {"price": self._last_prices.get(sym, pos.avg_cost), "option_symbols": [], "equity_position": None})
                view[sym]["equity_position"] = pos
            else:
                u = pos.underlying or ""
                view.setdefault(u, {"price": self._last_prices.get(u, 0.0), "option_symbols": [], "equity_position": self._positions.get(u) if self._positions.get(u) and self._positions.get(u).asset_class == AssetClass.EQUITY else None})
                view[u]["option_symbols"].append(sym)
        for u, data in view.items():
            if data["equity_position"] is None and u in self._positions and self._positions[u].asset_class == AssetClass.EQUITY:
                data["equity_position"] = self._positions[u]
        return view

    def update_prices(self, event: MarketEvent):
        if self._rust is not None:
            self._rust.update_prices_from_event(event)
            self.cash = self._rust.cash
            return
        sym = event.symbol
        new_price = event.close

        for sym in self._positions:
            pos = self._positions[sym]
            old_price = self._last_prices.get(sym, pos.avg_cost)
            multiplier = getattr(pos, 'multiplier', 1.0)
            self._position_value += ((new_price - old_price) * pos.quantity) * multiplier

        self._last_prices[sym] = new_price

        if event.asset_class == AssetClass.OPTION:
            current_date = event.timestamp.date()
            if current_date != self._last_expiry_check:
                self._expire_options(current_date)
                self._last_expiry_check = current_date

    def _expire_options(self, current_date: date):
        expired = []
        for sym, pos in self._positions.items():
            if pos.asset_class == AssetClass.OPTION and pos.expiry and pos.expiry.date() <= current_date:
                expired.append((sym, pos))

        for sym, pos in expired:
            del self._positions[sym]
            underlying_price = self._last_prices.get(pos.underlying or "", 0.0)
            self._position_value -= pos.quantity * self._last_prices.get(sym, pos.avg_cost) * getattr(pos, 'multiplier', 1.0)
            if pos.option_type == "C":
                intrinsic = max(0.0, underlying_price - (pos.strike or 0.0))
            else:
                intrinsic = max(0.0, (pos.strike or 0.0) - underlying_price)
            payout = intrinsic * pos.quantity * getattr(pos, 'multiplier', 1.0)
            self.cash += payout

    def apply_fill(self, fill: FillEvent, position_meta: dict | None = None):
        if self._rust is not None:
            meta = dict(position_meta) if position_meta else {}
            if "expiry" in meta and "expiry_days" not in meta:
                from datetime import datetime as dt
                ex = meta["expiry"]
                d = ex.date() if hasattr(ex, "date") else ex
                meta = {**meta, "expiry_days": (d - dt(1970, 1, 1).date()).days}
            if "asset_class" in meta and hasattr(meta["asset_class"], "name"):
                meta = {**meta, "asset_class": meta["asset_class"].name}
            self._rust.apply_fill_from_python(fill, meta if meta else None)
            self.cash = self._rust.cash
            return
        cost = fill.quantity * fill.fill_price * fill.multiplier
        self.cash -= (cost + fill.commission)

        sym = fill.symbol
        market_price = self._last_prices.get(sym, fill.fill_price)

        if sym in self._positions:
            pos = self._positions[sym]
            self._position_value -= pos.quantity * market_price * pos.multiplier
            new_qty = pos.quantity + fill.quantity
            if abs(new_qty) < 1e-9:
                del self._positions[sym]
            else:
                if (pos.quantity > 0) == (fill.quantity > 0):
                    pos.avg_cost = ((pos.quantity * pos.avg_cost) + (fill.quantity * fill.fill_price)) / new_qty
                pos.quantity = new_qty
                self._position_value += pos.quantity * market_price * pos.multiplier
        else:
            meta = position_meta or {}
            new_pos = Position(
                symbol=sym,
                quantity=fill.quantity,
                avg_cost=fill.fill_price,
                asset_class=meta.get("asset_class", AssetClass.EQUITY),
                underlying=meta.get("underlying"),
                strike=meta.get("strike"),
                expiry=meta.get("expiry"),
                option_type=meta.get("option_type"),
            )
            self._positions[sym] = new_pos
            self._position_value += new_pos.quantity * market_price * new_pos.multiplier

        self._trade_timestamps.append(fill.timestamp)
        self._trade_symbols.append(fill.symbol)
        self._trade_qtys.append(fill.quantity)
        self._trade_prices.append(fill.fill_price)
        self._trade_comms.append(fill.commission)
