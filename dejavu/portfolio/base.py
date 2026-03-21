from datetime import date
from typing import Any

import pandas as pd

from dejavu.schemas import AssetClass, FillEvent, MarketEvent, Option, Position


class Portfolio:
    def __init__(self, initial_capital: float):
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self._positions: dict[str, Position] = {}
        self._last_prices: dict[str, float] = {}

        # Caching position value for speed
        self._position_value = 0.0
        self._last_expiry_check: date | None = None

        # Trade Journal arrays
        self._trade_timestamps: list = []
        self._trade_symbols: list = []
        self._trade_qtys: list = []
        self._trade_prices: list = []
        self._trade_comms: list = []
        self.history: list = []

    @property
    def positions(self) -> dict[str, Any]:
        return self._positions

    @property
    def equity(self) -> float:
        return self.cash + self._position_value

    @property
    def prices(self) -> dict[str, float]:
        return self._last_prices

    @property
    def trade_journal(self) -> pd.DataFrame:
        return pd.DataFrame({
            "timestamp": self._trade_timestamps,
            "symbol": self._trade_symbols,
            "quantity": self._trade_qtys,
            "fill_price": self._trade_prices,
            "commission": self._trade_comms,
        })

    def underlying_view(self) -> dict[str, dict[str, Any]]:
        view: dict[str, dict[str, Any]] = {}
        for sym, pos in self._positions.items():
            inst = pos.instrument
            if inst.asset_class == AssetClass.EQUITY:
                view.setdefault(sym, {"price": self._last_prices.get(sym, pos.avg_cost), "option_symbols": [],
                                      "equity_position": None})
                view[sym]["equity_position"] = pos
            elif inst.asset_class == AssetClass.OPTION:
                u = getattr(inst, "underlying", None)

                if u is None:
                    continue

                view.setdefault(
                    u,
                    {
                        "price": self._last_prices.get(u, 0.0),
                        "option_symbols": [],
                        "equity_position": self._positions.get(u) if self._positions.get(u) and self._positions.get(
                            u).instrument.asset_class == AssetClass.EQUITY else None})
                view[u]["option_symbols"].append(sym)

        for u, data in view.items():
            if data["equity_position"] is None:
                eq_pos = self._positions.get(u)
                if eq_pos and eq_pos.instrument.asset_class == AssetClass.EQUITY:
                    data["equity_position"] = eq_pos

        return view

    def update_prices(self, event: MarketEvent):
        sym = event.instrument.symbol
        new_price = event.close

        if sym in self._positions:
            pos = self._positions[sym]
            old_price = self._last_prices.get(sym, pos.avg_cost)
            # Use the instrument's multiplier
            self._position_value += (new_price - old_price) * pos.quantity * pos.instrument.multiplier

        self._last_prices[sym] = new_price

        if event.instrument.asset_class == AssetClass.OPTION:
            current_date = event.timestamp.date()
            if current_date != self._last_expiry_check:
                self._expire_options(current_date)
                self._last_expiry_check = current_date

    def _expire_options(self, current_date: date):
        expired = []
        for sym, pos in self._positions.items():
            inst = pos.instrument
            if isinstance(inst, Option) and inst.expiry.date() <= current_date:
                expired.append((sym, pos))

        for sym, pos in expired:
            inst = pos.instrument  # We know this is an Option now
            assert isinstance(inst, Option)
            del self._positions[sym]

            underlying_price = self._last_prices.get(inst.underlying, 0.0)
            self._position_value -= pos.quantity * self._last_prices.get(sym, pos.avg_cost) * inst.multiplier

            if inst.option_type == "C":
                intrinsic = max(0.0, underlying_price - inst.strike)
            else:
                intrinsic = max(0.0, inst.strike - underlying_price)

            payout = intrinsic * pos.quantity * inst.multiplier
            self.cash += payout

    def apply_fill(self, fill: FillEvent):
        # Cost is negative if selling, positive if buying.
        # Commission is always positive.
        cost = fill.quantity * fill.fill_price * fill.instrument.multiplier
        self.cash -= (cost + fill.commission)

        sym = fill.instrument.symbol
        market_price = self._last_prices.get(sym, fill.fill_price)

        if sym in self._positions:
            pos = self._positions[sym]

            self._position_value -= pos.quantity * market_price * pos.instrument.multiplier

            new_qty = pos.quantity + fill.quantity

            if abs(new_qty) < 1e-9:
                del self._positions[sym]
            else:
                if (pos.quantity > 0) == (fill.quantity > 0):
                    pos.avg_cost = ((pos.quantity * pos.avg_cost) + (fill.quantity * fill.fill_price)) / new_qty
                elif (pos.quantity > 0) != (new_qty > 0):
                    # We crossed zero (e.g. Long -> Short)
                    pos.avg_cost = fill.fill_price

                pos.quantity = new_qty
                self._position_value += pos.quantity * market_price * pos.instrument.multiplier
        else:
            new_pos = Position(
                instrument=fill.instrument,
                quantity=fill.quantity,
                avg_cost=fill.fill_price,
            )
            self._positions[sym] = new_pos
            self._position_value += new_pos.quantity * market_price * new_pos.instrument.multiplier

        self._trade_timestamps.append(fill.timestamp)
        self._trade_symbols.append(fill.instrument.symbol)
        self._trade_qtys.append(fill.quantity)
        self._trade_prices.append(fill.fill_price)
        self._trade_comms.append(fill.commission)
