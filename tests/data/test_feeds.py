from collections.abc import AsyncIterator, Iterator
from datetime import datetime

from dejavu.data.feed import CombinedDataFeed, DataFeed
from dejavu.schemas import AssetClass, EventType, Instrument, MarketEvent


def make_instrument(symbol: str) -> Instrument:
    return Instrument(symbol=symbol, asset_class=AssetClass.EQUITY, multiplier=1.0)


def make_event(symbol: str, timestamp: datetime, close: float = 100.0) -> MarketEvent:
    return MarketEvent(
        type=EventType.MARKET,
        timestamp=timestamp,
        instrument=make_instrument(symbol),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1000.0,
    )


class SyncFeed(DataFeed):
    def __init__(self, events: list[MarketEvent]):
        self._events = events

    def stream(self) -> Iterator[MarketEvent]:
        yield from self._events


class AsyncFeed(DataFeed):
    def __init__(self, events: list[MarketEvent]):
        self._events = events

    async def stream(self) -> AsyncIterator[MarketEvent]:
        for event in self._events:
            yield event


class EmptyFeed(DataFeed):
    def stream(self) -> Iterator[MarketEvent]:
        return iter([])


class EmptyAsyncFeed(DataFeed):
    async def stream(self) -> AsyncIterator[MarketEvent]:
        return
        yield  # make it an async generator


class TestCombinedDataFeed:
    def test_single_feed_passthrough(self):
        events = [
            make_event("AAPL", datetime(2024, 1, 1, 9, 30)),
            make_event("AAPL", datetime(2024, 1, 1, 9, 31)),
            make_event("AAPL", datetime(2024, 1, 1, 9, 32)),
        ]
        feed = CombinedDataFeed(SyncFeed(events))
        result = list(feed.stream())

        assert len(result) == 3
        assert result == events

    def test_two_sync_feeds_merged_in_timestamp_order(self):
        feed_a = SyncFeed([
            make_event("AAPL", datetime(2024, 1, 1, 9, 30)),
            make_event("AAPL", datetime(2024, 1, 1, 9, 32)),
        ])
        feed_b = SyncFeed([
            make_event("MSFT", datetime(2024, 1, 1, 9, 31)),
            make_event("MSFT", datetime(2024, 1, 1, 9, 33)),
        ])

        result = list(CombinedDataFeed(feed_a, feed_b).stream())
        timestamps = [e.timestamp for e in result]

        assert timestamps == sorted(timestamps)
        assert len(result) == 4
        assert [e.instrument.symbol for e in result] == ["AAPL", "MSFT", "AAPL", "MSFT"]

    def test_async_feed_is_drained_and_merged(self):
        sync_events = [
            make_event("AAPL", datetime(2024, 1, 1, 9, 30)),
            make_event("AAPL", datetime(2024, 1, 1, 9, 32)),
        ]
        async_events = [
            make_event("MSFT", datetime(2024, 1, 1, 9, 31)),
            make_event("MSFT", datetime(2024, 1, 1, 9, 33)),
        ]

        result = list(
            CombinedDataFeed(SyncFeed(sync_events), AsyncFeed(async_events)).stream()
        )
        timestamps = [e.timestamp for e in result]

        assert timestamps == sorted(timestamps)
        assert len(result) == 4

    def test_overlapping_timestamps_preserves_all_events(self):
        ts = datetime(2024, 1, 1, 9, 30)
        feed_a = SyncFeed([make_event("AAPL", ts)])
        feed_b = SyncFeed([make_event("MSFT", ts)])

        result = list(CombinedDataFeed(feed_a, feed_b).stream())

        assert len(result) == 2
        symbols = {e.instrument.symbol for e in result}
        assert symbols == {"AAPL", "MSFT"}

    def test_empty_feed_alongside_populated_feed(self):
        events = [
            make_event("AAPL", datetime(2024, 1, 1, 9, 30)),
            make_event("AAPL", datetime(2024, 1, 1, 9, 31)),
        ]

        result = list(CombinedDataFeed(SyncFeed(events), EmptyFeed()).stream())

        assert len(result) == 2
        assert result == events

    def test_all_empty_feeds_yields_nothing(self):
        result = list(CombinedDataFeed(EmptyFeed(), EmptyFeed()).stream())
        assert result == []

    def test_empty_async_feed(self):
        events = [make_event("AAPL", datetime(2024, 1, 1, 9, 30))]
        result = list(CombinedDataFeed(SyncFeed(events), EmptyAsyncFeed()).stream())

        assert len(result) == 1
        assert result[0].instrument.symbol == "AAPL"

    def test_no_feeds_yields_nothing(self):
        result = list(CombinedDataFeed().stream())
        assert result == []

    def test_output_is_globally_sorted_across_many_feeds(self):
        feed_a = SyncFeed([make_event("A", datetime(2024, 1, 1, 9, 30))])
        feed_b = SyncFeed([make_event("B", datetime(2024, 1, 1, 9, 28))])
        feed_c = SyncFeed([make_event("C", datetime(2024, 1, 1, 9, 29))])

        result = list(CombinedDataFeed(feed_a, feed_b, feed_c).stream())
        timestamps = [e.timestamp for e in result]

        assert timestamps == sorted(timestamps)
        assert [e.instrument.symbol for e in result] == ["B", "C", "A"]

    def test_close_values_are_preserved(self):
        events = [
            make_event("AAPL", datetime(2024, 1, 1, 9, 30), close=150.0),
            make_event("AAPL", datetime(2024, 1, 1, 9, 31), close=151.5),
        ]
        result = list(CombinedDataFeed(SyncFeed(events)).stream())

        assert result[0].close == 150.0
        assert result[1].close == 151.5
