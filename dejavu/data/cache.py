import hashlib
import pickle
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from ..schemas import AssetClass
from .feed import BarData, DataFeed


class CachedDataFeed(DataFeed):
    """
    Wraps any DataFeed and caches results to disk.
    Cache is keyed by (feed_name, symbols, start, end).
    """

    def __init__(self, feed: DataFeed, cache_dir: Path = Path("data/processed")):
        self.feed = feed
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, symbols: list[str], start: datetime, end: datetime) -> Path:
        raw = f"{type(self.feed).__name__}:{sorted(symbols)}:{start}:{end}"
        digest = hashlib.md5(raw.encode()).hexdigest()
        return self.cache_dir / f"{digest}.pkl"

    def stream(self, symbols, start, end) -> Iterator[BarData]:
        cache_file = self._cache_key(symbols, start, end)

        if cache_file.exists():
            with open(cache_file, "rb") as f:
                bars: list[BarData] = pickle.load(f)
            yield from bars
            return

        bars = list(self.feed.stream(symbols, start, end))

        with open(cache_file, "wb") as f:
            pickle.dump(bars, f)

        yield from bars

    def supports_asset_class(self, asset_class: AssetClass) -> bool:
        return self.feed.supports_asset_class(asset_class)
