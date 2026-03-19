from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator

import pandas as pd

from dejavu.schemas import (
    AssetClass,
    EventType,
    Instrument,
    MarketEvent,
    Option,
)


class DataFeed(ABC):
    """Event-based feed: stream() yields market events in time order. No symbols/start/end — feed owns its source."""

    @abstractmethod
    def stream(self) -> Iterator[MarketEvent] | AsyncIterator[MarketEvent]:
        """All feeds must implement a streaming mechanism."""
        pass

    def supports_asset_class(self, asset_class: AssetClass) -> bool:
        """Override if the feed only supports certain asset classes. Default: all supported."""
        return True

class RESTDataFeed(DataFeed, ABC):

    @abstractmethod
    def stream(self) -> Iterator[MarketEvent]:
        ...


class LiveDataFeed(DataFeed, ABC):

    @abstractmethod
    async def stream(self) -> AsyncIterator[MarketEvent]:
        ...

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass


class CSVDataFeed(DataFeed):
    """More often than note, data for back testing is stored within CSV files. This data feed provides a performant way of loading them
    and transforming them into MarketEvent's for your trading strategies. Supports EQUITY and CRYPTO"""

    def __init__(self, paths: dict[str, str], asset_classes: dict[str, AssetClass]):
        """_summary_

        Args:
            paths (dict["symbol", "path"]): A mapping of symbol to its corresponding CSV file path.
            The CSVs should have columns: timestamp, open, high, low, close, volume.
            asset_classes (dict[str, AssetClass]): A mapping of symbol to its corresponding asset class.
        """
        self.paths = paths  # symbol -> file path
        self.asset_classes = asset_classes

    def stream(self) -> Iterator[MarketEvent]:
        frames = []
        instruments = {}
        for symbol, path in self.paths.items():
            df = pd.read_csv(path, parse_dates=["timestamp"])
            df["symbol"] = symbol
            frames.append(df)

        combined = pd.concat(frames).sort_values("timestamp")

        for row in combined.itertuples(index=False):
            sym = row.symbol

            instruments[sym] = Instrument(
                symbol=sym,
                asset_class=self.asset_classes.get(sym, AssetClass.EQUITY),
                multiplier=1.0,
            )

            yield MarketEvent(
                type=EventType.MARKET,
                timestamp=row.timestamp,
                instrument=instruments[sym],
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume)
            )


class CombinedDataFeed(DataFeed):
    """Combines equity and options data from separate CSVs, yielding MarketEvent or OptionMarketEvent in timestamp order."""

    def __init__(self, equity_path: str, options_path: str | None = None):
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

    def stream(self) -> Iterator[MarketEvent]:
        instruments = {}

        for row in self.data.itertuples(index=False):
            sym = row.symbol

            if sym not in instruments:
                is_option = row.asset_class_order == 1
                if is_option:
                    instruments[sym] = Option(
                        symbol=sym,
                        asset_class=AssetClass.OPTION,
                        underlying=row.underlying,
                        strike=float(row.strike),
                        expiry=row.expiry,
                        option_type=row.option_type,
                        multiplier=100.0,
                    )
                else:
                    instruments[sym] = Instrument(
                        symbol=sym,
                        asset_class=AssetClass.EQUITY,
                        multiplier=1.0,
                    )

            yield MarketEvent(
                type=EventType.MARKET,
                timestamp=row.timestamp,
                instrument=instruments[sym],
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                iv=float(row.iv) if row.iv else None,
                delta=float(row.delta) if row.delta else None,
                gamma=float(row.gamma) if row.gamma else None,
            )
