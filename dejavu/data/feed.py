import asyncio
import csv
import heapq
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from datetime import datetime

from dejavu.schemas import (
    AssetClass,
    EventType,
    Instrument,
    MarketEvent,
    Option,
)


def _parse_float(val: str) -> float | None:
    if not val or val.strip() == "":
        return None
    return float(val)


def _parse_timestamp(val: str) -> datetime:
    return datetime.fromisoformat(val)


class DataFeed(ABC):
    """Event-based feed: stream() yields market events in time order. No symbols/start/end — feed owns its source."""

    @abstractmethod
    def stream(self) -> Iterator[MarketEvent] | AsyncIterator[MarketEvent]:
        """All feeds must implement a streaming mechanism."""
        pass

    def supports_asset_class(self, asset_class: AssetClass) -> bool:  # noqa: ARG002
        """Override if the feed only supports certain asset classes. Default: all supported."""
        return True


class RESTDataFeed(DataFeed, ABC):
    """Base class for REST data feeds. We use async to handle multiple symbols from the one feed.
    """
    @abstractmethod
    def stream(self) -> AsyncIterator[MarketEvent]:
        ...


class LiveDataFeed(DataFeed, ABC):

    @abstractmethod
    def stream(self) -> AsyncIterator[MarketEvent]:
        ...

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass


class CSVDataFeed(DataFeed):
    def __init__(self, path: str, asset_class: AssetClass = AssetClass.EQUITY):
        self.path = path
        self.asset_class = asset_class

    def stream(self) -> Iterator[MarketEvent]:
        instruments: dict[str, Instrument] = {}
        ts_cache: dict[str, datetime] = {}

        with open(self.path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            idx = {name: i for i, name in enumerate(header)}

            for row in reader:
                raw_ts = row[idx["timestamp"]]
                if raw_ts not in ts_cache:
                    ts_cache[raw_ts] = datetime.fromisoformat(raw_ts)

                sym = row[idx["symbol"]]
                if sym not in instruments:
                    instruments[sym] = self._build_instrument_from_row(row, idx, sym)

                yield MarketEvent(
                    type=EventType.MARKET,
                    timestamp=ts_cache[raw_ts],
                    instrument=instruments[sym],
                    open=float(row[idx["open"]]),
                    high=float(row[idx["high"]]),
                    low=float(row[idx["low"]]),
                    close=float(row[idx["close"]]),
                    volume=float(row[idx["volume"]]),
                    iv=_parse_float(row[idx["iv"]] if "iv" in idx else ""),
                    delta=_parse_float(row[idx["delta"]] if "delta" in idx else ""),
                    gamma=_parse_float(row[idx["gamma"]] if "gamma" in idx else ""),
                )

    def _build_instrument_from_row(
            self, row: list[str], idx: dict[str, int], sym: str
    ) -> Instrument:
        if self.asset_class == AssetClass.OPTION:
            return Option(
                symbol=sym,
                asset_class=AssetClass.OPTION,
                underlying=row[idx["underlying"]],
                strike=float(row[idx["strike"]]),
                expiry=datetime.fromisoformat(row[idx["expiry"]]),
                option_type=row[idx["option_type"]],
                multiplier=100.0,
            )
        return Instrument(
            symbol=sym,
            asset_class=self.asset_class,
            multiplier=1.0,
        )

async def _collect_async(ait: AsyncIterator[MarketEvent]) -> list[MarketEvent]:
    return [event async for event in ait]


class CombinedDataFeed(DataFeed):
    """This merges data across any number of DataFeeds. For example, you may have a AlphaVantage Data feed and a CSV
    data feed.

    Example:

    ```python
    feed = CombinedDataFeed(
        CSVDataFeed("equity.csv", "options.csv"),
        AlphaVantageRestFeed(api_key="...", symbols=["AAPL"], ...),
    )
    ```
    """

    def __init__(self, *feeds: DataFeed):
        self.feeds = feeds

    def stream(self) -> Iterator[MarketEvent]:
        def iter_feed(feed: DataFeed) -> Iterator[MarketEvent]:
            result = feed.stream()
            if isinstance(result, AsyncIterator):
                yield from asyncio.run(_collect_async(result))
            else:
                yield from result

        iterators = [iter_feed(f) for f in self.feeds]
        heap = []

        def push(it, idx):
            try:
                event = next(it)
                heapq.heappush(heap, (event.timestamp, idx, event))
            except StopIteration:
                pass

        for idx, it in enumerate(iterators):
            push(it, idx)

        while heap:
            ts, idx, event = heapq.heappop(heap)
            yield event
            push(iterators[idx], idx)
