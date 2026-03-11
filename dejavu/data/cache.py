import hashlib
import pickle
from collections.abc import Iterator
from pathlib import Path

from ..schemas import AssetClass, MarketEvent
from .feed import DataFeed


class CachedDataFeed(DataFeed):
    """
    Wraps any DataFeed and caches the event stream to disk.
    Cache is keyed by feed_id (if provided) or by feed type + config hash.
    """

    def __init__(
        self,
        feed: DataFeed,
        cache_dir: Path = Path("data/processed"),
        feed_id: str | None = None,
    ):
        self.feed = feed
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.feed_id = feed_id

    def _cache_key(self) -> Path:
        if self.feed_id is not None:
            raw = self.feed_id
        else:
            # Derive from feed type and its config (paths, etc.)
            config = getattr(self.feed, "paths", None) or getattr(
                self.feed, "data", None
            )
            raw = f"{type(self.feed).__name__}:{config!r}"
        digest = hashlib.md5(raw.encode()).hexdigest()
        return self.cache_dir / f"{digest}.pkl"

    def stream(self) -> Iterator[MarketEvent]:
        cache_file = self._cache_key()

        if cache_file.exists():
            with open(cache_file, "rb") as f:
                events: list[MarketEvent] = pickle.load(f)
            yield from events
            return

        events = list(self.feed.stream())
        with open(cache_file, "wb") as f:
            pickle.dump(events, f)
        yield from events

    def supports_asset_class(self, asset_class: AssetClass) -> bool:
        return self.feed.supports_asset_class(asset_class)
