from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from ..schemas import AssetClass, EventType, MarketEvent, OptionMarketEvent


@dataclass
class BarData:
    """Normalized bar — every feed outputs this."""
    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    asset_class: AssetClass
    # Options fields (None for non-options)
    underlying: Optional[str] = None
    strike: Optional[float] = None
    expiry: Optional[datetime] = None
    option_type: Optional[str] = None  # "C" or "P"
    iv: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None


class DataFeed(ABC):
    @abstractmethod
    def stream(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> Iterator[BarData]:
        ...


class CSVDataFeed(DataFeed):

    def __init__(self, paths: dict[str, str], asset_classes: dict[str, AssetClass]):
        self.paths = paths  # symbol -> file path
        self.asset_classes = asset_classes

    def stream(self):
        frames = []
        for symbol, path in self.paths.items():
            df = pd.read_csv(path, parse_dates=["timestamp"])
            df["symbol"] = symbol
            frames.append(df)

        combined = pd.concat(frames).sort_values("timestamp")

        for _, row in combined.iterrows():
            yield MarketEvent(
                type=EventType.MARKET,
                timestamp=row["timestamp"],
                symbol=row["symbol"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                asset_class=self.asset_classes[row["symbol"]],
            )


class CombinedDataFeed:
    """Combines equity and options data from separate CSVs, yielding MarketEvent or OptionMarketEvent in timestamp order."""
    def __init__(self, equity_path: str, options_path: Optional[str] = None):
        eq = pd.read_csv(equity_path, parse_dates=["timestamp"])
        eq["asset_class_order"] = 0   # equity streams FIRST each day
        frames = [eq]

        if options_path:
            op = pd.read_csv(
                options_path, parse_dates=["timestamp", "expiry"]
            )
            op["asset_class_order"] = 1   # options stream SECOND
            frames.append(op)

        self.data = (
            pd.concat(frames, ignore_index=True)
            .sort_values(["timestamp", "asset_class_order"])
            .reset_index(drop=True)
        )

    def stream(self):
        for _, row in self.data.iterrows():
            is_option = row.get("asset_class_order", 0) == 1

            if not is_option:
                yield MarketEvent(
                    type=EventType.MARKET,
                    timestamp=row["timestamp"],
                    symbol=row["symbol"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                    asset_class=AssetClass.EQUITY,
                )
            else:
                yield OptionMarketEvent(
                    type=EventType.MARKET,
                    timestamp=row["timestamp"],
                    symbol=row["symbol"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                    asset_class=AssetClass.OPTION,
                    underlying=str(row["underlying"]),
                    strike=float(row["strike"]),
                    expiry=row["expiry"],
                    option_type=str(row["option_type"]),
                    iv=float(row["iv"]),
                    delta=float(row["delta"]),
                    gamma=float(row["gamma"]),
                    theta=float(row["theta"]),
                    vega=float(row["vega"]),
                )
