import csv
import time
from datetime import datetime, timedelta

import pytest

from dejavu.data.feed import CombinedDataFeed, CSVDataFeed
from dejavu.schemas import AssetClass


def write_equity_csv(path: str, rows: int) -> None:
    base = datetime(2024, 1, 2, 9, 30)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        )
        for i in range(rows):
            ts = base + timedelta(minutes=i)
            writer.writerow(
                [
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "AAPL",
                    180.0,
                    181.0,
                    179.0,
                    180.5,
                    1000,
                ]
            )


def write_options_csv(path: str, rows: int) -> None:
    base = datetime(2024, 1, 2, 9, 30)
    expiry = datetime(2024, 3, 15)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "symbol",
                "underlying",
                "strike",
                "expiry",
                "option_type",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "iv",
                "delta",
                "gamma",
            ]
        )
        for i in range(rows):
            ts = base + timedelta(minutes=i)
            writer.writerow(
                [
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "AAPL240315C00180000",
                    "AAPL",
                    180.0,
                    expiry.strftime("%Y-%m-%d"),
                    "call",
                    2.5,
                    2.6,
                    2.4,
                    2.5,
                    100,
                    0.25,
                    0.45,
                    0.02,
                ]
            )


@pytest.fixture
def equity_csv_small(tmp_path):
    path = str(tmp_path / "equity_small.csv")
    write_equity_csv(path, rows=1_000)
    return path


@pytest.fixture
def equity_csv_large(tmp_path):
    path = str(tmp_path / "equity_large.csv")
    write_equity_csv(path, rows=100_000)
    return path


@pytest.fixture
def options_csv_large(tmp_path):
    path = str(tmp_path / "options_large.csv")
    write_options_csv(path, rows=100_000)
    return path


class TestCSVDataFeedPerformance:
    def test_stream_1k_rows(self, benchmark, equity_csv_small):
        def run():
            return list(CSVDataFeed(equity_csv_small, AssetClass.EQUITY).stream())

        result = benchmark(run)
        assert len(result) == 1_000

    def test_stream_100k_rows(self, benchmark, equity_csv_large):
        def run():
            return list(CSVDataFeed(equity_csv_large, AssetClass.EQUITY).stream())

        result = benchmark(run)
        assert len(result) == 100_000

    def test_stream_100k_option_rows(self, benchmark, options_csv_large):
        def run():
            return list(CSVDataFeed(options_csv_large, AssetClass.OPTION).stream())

        result = benchmark(run)
        assert len(result) == 100_000

    def test_stream_is_lazy(self, equity_csv_large):
        """Stream should not load all rows upfront — first event should arrive quickly."""
        feed = CSVDataFeed(equity_csv_large, AssetClass.EQUITY)
        gen = feed.stream()

        start = time.perf_counter()
        next(gen)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.01, f"First event took {elapsed:.3f}s — feed may not be lazy"


class TestCombinedDataFeedPerformance:
    def test_merge_two_feeds_100k_each(
        self, benchmark, equity_csv_large, options_csv_large
    ):
        def run():
            feed = CombinedDataFeed(
                CSVDataFeed(equity_csv_large, AssetClass.EQUITY),
                CSVDataFeed(options_csv_large, AssetClass.OPTION),
            )
            return list(feed.stream())

        result = benchmark(run)
        assert len(result) == 200_000

    def test_merge_preserves_timestamp_order(self, equity_csv_large, options_csv_large):
        feed = CombinedDataFeed(
            CSVDataFeed(equity_csv_large, AssetClass.EQUITY),
            CSVDataFeed(options_csv_large, AssetClass.OPTION),
        )
        events = list(feed.stream())
        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_merge_five_feeds(self, benchmark, tmp_path):
        """Heap merge should scale well across many feeds."""
        paths = []
        for i in range(5):
            path = str(tmp_path / f"equity_{i}.csv")
            write_equity_csv(path, rows=20_000)
            paths.append(path)

        def run():
            feeds = [CSVDataFeed(p, AssetClass.EQUITY) for p in paths]
            return list(CombinedDataFeed(*feeds).stream())

        result = benchmark(run)
        assert len(result) == 100_000
