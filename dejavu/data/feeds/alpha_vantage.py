import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from enum import StrEnum

import httpx

from dejavu.data.feed import RESTDataFeed
from dejavu.schemas import AssetClass, EventType, Instrument, MarketEvent

# Developer note: Alpha vantage is a bit of a tricky API as the URL changes between asset classes,
# and time-frame. This is why I've broken out the different timeframes into enums for URL building
# within the feed itself


class AlphaVantageIntradayInterval(StrEnum):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min"
    FIFTEEN_MINUTES = "15min"
    THIRTY_MINUTES = "30min"
    SIXTY_MINUTES = "60min"


class AlphaVantagePeriodicInterval(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


AlphaVantageInterval = AlphaVantageIntradayInterval | AlphaVantagePeriodicInterval


_AV_INTRADAY_CONFIG: dict[AssetClass, tuple[str, str, str]] = {
    AssetClass.EQUITY: ("TIME_SERIES_INTRADAY", "symbol", "Time Series"),
    AssetClass.FX: ("FX_INTRADAY", "from_symbol", "Time Series FX"),
    AssetClass.CRYPTO: ("CRYPTO_INTRADAY", "symbol", "Crypto Intraday"),
}

# (av_function, series_key_prefix) per asset class per periodic interval
_AV_PERIODIC_CONFIG: dict[
    AssetClass, dict[AlphaVantagePeriodicInterval, tuple[str, str]]
] = {
    AssetClass.EQUITY: {
        AlphaVantagePeriodicInterval.DAILY: (
            "TIME_SERIES_DAILY",
            "Time Series (Daily)",
        ),
        AlphaVantagePeriodicInterval.WEEKLY: (
            "TIME_SERIES_WEEKLY",
            "Weekly Time Series",
        ),
        AlphaVantagePeriodicInterval.MONTHLY: (
            "TIME_SERIES_MONTHLY",
            "Monthly Time Series",
        ),
    },
    AssetClass.FX: {
        AlphaVantagePeriodicInterval.DAILY: (
            "FX_DAILY",
            "Time Series FX (Daily)",
        ),
        AlphaVantagePeriodicInterval.WEEKLY: (
            "FX_WEEKLY",
            "Time Series FX (Weekly)",
        ),
        AlphaVantagePeriodicInterval.MONTHLY: (
            "FX_MONTHLY",
            "Time Series FX (Monthly)",
        ),
    },
    AssetClass.CRYPTO: {
        AlphaVantagePeriodicInterval.DAILY: (
            "DIGITAL_CURRENCY_DAILY",
            "Time Series (Digital Currency Daily)",
        ),
        AlphaVantagePeriodicInterval.WEEKLY: (
            "DIGITAL_CURRENCY_WEEKLY",
            "Time Series (Digital Currency Weekly)",
        ),
        AlphaVantagePeriodicInterval.MONTHLY: (
            "DIGITAL_CURRENCY_MONTHLY",
            "Time Series (Digital Currency Monthly)",
        ),
    },
}

# Crypto periodic responses use different OHLCV keys
_CRYPTO_PERIODIC_OHLCV_KEYS = (
    "1a. open (USD)",
    "2a. high (USD)",
    "3a. low (USD)",
    "4a. close (USD)",
    "5. volume",
)

_DEFAULT_OHLCV_KEYS = (
    "1. open",
    "2. high",
    "3. low",
    "4. close",
    "5. volume",
)


def _is_intraday(interval: AlphaVantageInterval) -> bool:
    return isinstance(interval, AlphaVantageIntradayInterval)


class AlphaVantageRESTFeed(RESTDataFeed):
    """Alpha Vantage REST API feed.

    Supports intraday (1/5/15/30/60 min) and periodic (daily/weekly/monthly)
    intervals for equities, FX, and crypto.

    Note: Alpha Vantage has strict rate limits (5 calls/minute, 500 calls/day
    for the free tier). Intraday endpoints typically require a paid
    subscription — check the Alpha Vantage docs to confirm your API key has
    the necessary permissions.

    Example:
    ```python
    equity_feed = AlphaVantageRESTFeed(
        api_key="YOUR_KEY",
        symbols=["IBM", "AAPL"],
        asset_class=AssetClass.EQUITY,
        interval=AlphaVantageIntradayInterval.FIVE_MINUTES,
    )
    forex_feed = AlphaVantageRESTFeed(
        api_key="YOUR_KEY",
        symbols=["EUR/USD"],
        asset_class=AssetClass.FX,
        interval=AlphaVantagePeriodicInterval.WEEKLY,
    )
    feed = CombinedDataFeed(equity_feed, forex_feed)
    ```
    """

    def __init__(
        self,
        api_key: str,
        symbols: list[str],
        asset_class: AssetClass,
        interval: AlphaVantageInterval,
        total_limit: int = 2000,
    ):
        """

        Args:
            api_key: Your Alphavantage API key. Usually stored as an environment variable.
            symbols: The list of symbols you wish to return. Note, they must be within the same asset class.
            asset_class: The asset class of the data you're returning.
            interval: Your time interval for the data, eg. 1min, weekly, etc.
            total_limit:
        """
        self.interval = self._coerce_interval(interval)

        if not self.supports_asset_class(asset_class):
            raise ValueError(
                f"Unsupported asset class '{asset_class}' for "
                f"{'intraday' if _is_intraday(self.interval) else 'periodic'} intervals. "
                f"Supported: {list(_AV_INTRADAY_CONFIG if _is_intraday(self.interval) else _AV_PERIODIC_CONFIG)}"
            )

        self.api_key = api_key
        self.symbols = symbols
        self.asset_class = asset_class
        self.total_limit = total_limit
        self.base_url = "https://www.alphavantage.co/query"

        self._av_function, self._symbol_param, self._series_key_prefix = (
            self._resolve_config(asset_class, self.interval)
        )
        self._ohlcv_keys = (
            _CRYPTO_PERIODIC_OHLCV_KEYS
            if asset_class == AssetClass.CRYPTO and not _is_intraday(interval)
            else _DEFAULT_OHLCV_KEYS
        )

    @staticmethod
    def _coerce_interval(interval: AlphaVantageInterval | str) -> AlphaVantageInterval:
        if isinstance(
            interval, (AlphaVantageIntradayInterval, AlphaVantagePeriodicInterval)
        ):
            return interval
        for enum_cls in (AlphaVantageIntradayInterval, AlphaVantagePeriodicInterval):
            try:
                return enum_cls(interval)
            except ValueError:
                continue
        raise ValueError(
            f"Unrecognised interval '{interval}'. "
            f"Valid intraday: {list(AlphaVantageIntradayInterval)}. "
            f"Valid periodic: {list(AlphaVantagePeriodicInterval)}."
        )

    @staticmethod
    def _resolve_config(
        asset_class: AssetClass, interval: AlphaVantageInterval
    ) -> tuple[str, str, str]:
        """Return (av_function, symbol_param, series_key_prefix)."""
        if _is_intraday(interval):
            return _AV_INTRADAY_CONFIG[asset_class]

        av_function, series_key_prefix = _AV_PERIODIC_CONFIG[asset_class][
            interval  # type: ignore[index]
        ]
        # Periodic FX uses from_symbol; all others use symbol
        symbol_param = "from_symbol" if asset_class == AssetClass.FX else "symbol"
        return av_function, symbol_param, series_key_prefix

    def supports_asset_class(self, asset_class: AssetClass) -> bool:
        config = (
            _AV_INTRADAY_CONFIG if _is_intraday(self.interval) else _AV_PERIODIC_CONFIG
        )
        return asset_class in config

    def _parse_forex_symbol(self, symbol: str) -> tuple[str, str]:
        if "/" not in symbol:
            raise ValueError(
                f"Forex symbol '{symbol}' must be in 'FROM/TO' format, e.g. 'EUR/USD'."
            )
        from_ccy, to_ccy = symbol.split("/", maxsplit=1)
        if len(from_ccy) != 3 or len(to_ccy) != 3:
            raise ValueError(
                f"Forex symbol '{symbol}' contains non-standard currency codes. "
                "Expected 3-character ISO 4217 codes, e.g. 'EUR/USD'."
            )
        return from_ccy.upper(), to_ccy.upper()

    def _build_params(self, symbol: str) -> dict:
        params = {
            "function": self._av_function,
            "outputsize": "full",
            "apikey": self.api_key,
        }
        if _is_intraday(self.interval):
            params["interval"] = self.interval

        if self.asset_class == AssetClass.FX:
            from_ccy, to_ccy = self._parse_forex_symbol(symbol)
            params["from_symbol"] = from_ccy
            params["to_symbol"] = to_ccy
        else:
            params[self._symbol_param] = symbol

        return params

    def _find_time_series(self, data: dict, symbol: str) -> dict:
        for key in data:
            if key.startswith(self._series_key_prefix):
                return data[key]

        error = data.get("Information") or data.get("Note") or data.get("Error Message")
        raise ValueError(
            f"Could not find time series data for '{symbol}'. "
            + (
                f"Alpha Vantage says: {error}"
                if error
                else f"Response keys: {list(data.keys())}"
            )
        )

    async def _fetch_symbol(
        self, client: httpx.AsyncClient, symbol: str
    ) -> tuple[str, dict]:
        params = self._build_params(symbol)
        response = await client.get(self.base_url, params=params)
        response.raise_for_status()
        data = response.json()
        time_series = self._find_time_series(data, symbol)
        return symbol, time_series

    async def stream_async(self) -> AsyncIterator[MarketEvent]:
        open_key, high_key, low_key, close_key, volume_key = self._ohlcv_keys

        async with httpx.AsyncClient() as client:
            for i, symbol in enumerate(self.symbols):
                if i > 0:
                    await asyncio.sleep(12)  # ~5 calls/min safe default

                symbol, time_series = await self._fetch_symbol(client, symbol)
                inst = Instrument(symbol=symbol, asset_class=self.asset_class)

                entries = list(reversed(list(time_series.items())))
                entries = entries[: self.total_limit]

                for timestamp_str, ohlcv in entries:
                    yield MarketEvent(
                        type=EventType.MARKET,
                        timestamp=datetime.fromisoformat(timestamp_str),
                        instrument=inst,
                        open=float(ohlcv[open_key]),
                        high=float(ohlcv[high_key]),
                        low=float(ohlcv[low_key]),
                        close=float(ohlcv[close_key]),
                        volume=float(ohlcv[volume_key])
                        if volume_key in ohlcv
                        else None,
                    )
