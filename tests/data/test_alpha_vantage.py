import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from dejavu.data.feeds.alpha_vantage import (
    AlphaVantageIntradayInterval,
    AlphaVantagePeriodicInterval,
    AlphaVantageRESTFeed,
)
from dejavu.schemas import AssetClass, EventType


def make_feed(**kwargs) -> AlphaVantageRESTFeed:
    defaults = dict(
        api_key="TEST_KEY",
        symbols=["IBM"],
        asset_class=AssetClass.EQUITY,
        interval=AlphaVantageIntradayInterval.FIVE_MINUTES,
        total_limit=3,
    )
    return AlphaVantageRESTFeed(**{**defaults, **kwargs})


def _intraday_response(symbol: str = "IBM") -> dict:
    return {
        "Meta Data": {"2. Symbol": symbol},
        "Time Series (5min)": {
            "2024-01-01 09:35:00": {
                "1. open": "150.00",
                "2. high": "151.00",
                "3. low": "149.50",
                "4. close": "150.75",
                "5. volume": "10000",
            },
            "2024-01-01 09:30:00": {
                "1. open": "148.00",
                "2. high": "150.00",
                "3. low": "147.50",
                "4. close": "150.00",
                "5. volume": "20000",
            },
        },
    }


def _fx_daily_response() -> dict:
    return {
        "Meta Data": {},
        "Time Series FX (Daily)": {
            "2024-01-02": {
                "1. open": "1.0900",
                "2. high": "1.0950",
                "3. low": "1.0880",
                "4. close": "1.0920",
            },
            "2024-01-01": {
                "1. open": "1.0850",
                "2. high": "1.0910",
                "3. low": "1.0840",
                "4. close": "1.0900",
            },
        },
    }


def _crypto_daily_response() -> dict:
    return {
        "Meta Data": {},
        "Time Series (Digital Currency Daily)": {
            "2024-01-02": {
                "1a. open (USD)": "42000.00",
                "2a. high (USD)": "43000.00",
                "3a. low (USD)": "41500.00",
                "4a. close (USD)": "42500.00",
                "5. volume": "1500.00",
            },
            "2024-01-01": {
                "1a. open (USD)": "41000.00",
                "2a. high (USD)": "42000.00",
                "3a. low (USD)": "40500.00",
                "4a. close (USD)": "41800.00",
                "5. volume": "2000.00",
            },
        },
    }


def _error_response(key: str, message: str) -> dict:
    return {key: message}


def _make_mock_response(payload: dict, status_code: int = 200) -> MagicMock:
    mock = MagicMock(spec=httpx.Response)
    mock.status_code = status_code
    mock.json.return_value = payload
    mock.raise_for_status = MagicMock()
    if status_code >= 400:
        mock.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=mock
        )
    return mock



class TestAlphaVantageConstruction:
    def test_valid_equity_intraday(self):
        feed = make_feed()
        assert feed.asset_class == AssetClass.EQUITY
        assert feed.interval == AlphaVantageIntradayInterval.FIVE_MINUTES

    def test_valid_fx_periodic(self):
        feed = make_feed(
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        assert feed.interval == AlphaVantagePeriodicInterval.DAILY

    def test_valid_crypto_intraday(self):
        feed = make_feed(
            symbols=["BTC"],
            asset_class=AssetClass.CRYPTO,
            interval=AlphaVantageIntradayInterval.ONE_MINUTE,
        )
        assert feed.asset_class == AssetClass.CRYPTO

    @pytest.mark.parametrize(
        "interval",
        [
            "1min",
            "5min",
            "15min",
            "30min",
            "60min",
            "daily",
            "weekly",
            "monthly",
        ],
    )
    def test_string_interval_coercion(self, interval: str):
        feed = make_feed(interval=interval)
        assert isinstance(
            feed.interval,
            (AlphaVantageIntradayInterval, AlphaVantagePeriodicInterval),
        )

    def test_invalid_string_interval_raises(self):
        with pytest.raises(ValueError, match="Unrecognised interval"):
            make_feed(interval="hourly")

    def test_unsupported_asset_class_raises(self):
        with pytest.raises(ValueError, match="Unsupported asset class"):
            make_feed(asset_class=AssetClass.COMMODITY)

    def test_default_total_limit(self):
        feed = make_feed()
        assert feed.total_limit == 3

    def test_base_url(self):
        feed = make_feed()
        assert feed.base_url == "https://www.alphavantage.co/query"


class TestSupportsAssetClass:
    def test_equity_intraday_supported(self):
        feed = make_feed(asset_class=AssetClass.EQUITY)
        assert feed.supports_asset_class(AssetClass.EQUITY)

    def test_fx_intraday_supported(self):
        feed = make_feed(
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
        )
        assert feed.supports_asset_class(AssetClass.FX)

    def test_crypto_periodic_supported(self):
        feed = make_feed(
            symbols=["BTC"],
            asset_class=AssetClass.CRYPTO,
            interval=AlphaVantagePeriodicInterval.WEEKLY,
        )
        assert feed.supports_asset_class(AssetClass.CRYPTO)

    def test_unsupported_class_returns_false(self):
        feed = make_feed(asset_class=AssetClass.EQUITY)
        assert not feed.supports_asset_class(AssetClass.COMMODITY)


class TestParseForexSymbol:
    def test_valid_symbol(self):
        feed = make_feed(
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        assert feed._parse_forex_symbol("EUR/USD") == ("EUR", "USD")

    def test_lowercase_normalised(self):
        feed = make_feed(
            symbols=["eur/usd"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        assert feed._parse_forex_symbol("eur/usd") == ("EUR", "USD")

    def test_missing_slash_raises(self):
        feed = make_feed(
            symbols=["EURUSD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        with pytest.raises(ValueError, match="FROM/TO"):
            feed._parse_forex_symbol("EURUSD")

    def test_non_standard_code_raises(self):
        feed = make_feed(
            symbols=["EURO/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        with pytest.raises(ValueError, match="ISO 4217"):
            feed._parse_forex_symbol("EURO/USD")


class TestAlphaVantageBuildParams:
    def test_intraday_includes_interval(self):
        feed = make_feed(interval=AlphaVantageIntradayInterval.FIVE_MINUTES)
        params = feed._build_params("IBM")
        assert params["interval"] == "5min"

    def test_periodic_omits_interval(self):
        feed = make_feed(interval=AlphaVantagePeriodicInterval.DAILY)
        params = feed._build_params("IBM")
        assert "interval" not in params

    def test_equity_uses_symbol_param(self):
        feed = make_feed(asset_class=AssetClass.EQUITY)
        params = feed._build_params("IBM")
        assert params["symbol"] == "IBM"

    def test_fx_splits_symbol(self):
        feed = make_feed(
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
        )
        params = feed._build_params("EUR/USD")
        assert params["from_symbol"] == "EUR"
        assert params["to_symbol"] == "USD"
        assert "symbol" not in params

    def test_api_key_included(self):
        feed = make_feed(api_key="MY_KEY")
        params = feed._build_params("IBM")
        assert params["apikey"] == "MY_KEY"

    def test_outputsize_full(self):
        feed = make_feed()
        params = feed._build_params("IBM")
        assert params["outputsize"] == "full"


class TestAlphaVantageFindTimeSeries:
    def test_finds_matching_key(self):
        feed = make_feed()
        data = {"Time Series (5min)": {"ts": "data"}, "Meta Data": {}}
        assert feed._find_time_series(data, "IBM") == {"ts": "data"}

    def test_raises_on_information_error(self):
        feed = make_feed()
        data = _error_response("Information", "API limit reached")
        with pytest.raises(ValueError, match="API limit reached"):
            feed._find_time_series(data, "IBM")

    def test_raises_on_note(self):
        feed = make_feed()
        data = _error_response("Note", "Thank you for using Alpha Vantage")
        with pytest.raises(ValueError, match="Thank you"):
            feed._find_time_series(data, "IBM")

    def test_raises_on_error_message(self):
        feed = make_feed()
        data = _error_response("Error Message", "Invalid API call")
        with pytest.raises(ValueError, match="Invalid API call"):
            feed._find_time_series(data, "IBM")

    def test_raises_with_response_keys_when_no_error(self):
        feed = make_feed()
        data = {"Unexpected Key": {}}
        with pytest.raises(ValueError, match="Response keys"):
            feed._find_time_series(data, "IBM")


class TestAlphaVantageStream:
    @pytest.mark.asyncio
    async def test_equity_intraday_events(self):
        feed = make_feed(
            symbols=["IBM"],
            asset_class=AssetClass.EQUITY,
            interval=AlphaVantageIntradayInterval.FIVE_MINUTES,
            total_limit=2,
        )
        mock_response = _make_mock_response(_intraday_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            events = [event async for event in feed.stream()]

        assert len(events) == 2
        event = events[0]
        assert event.type == EventType.MARKET
        assert event.instrument.symbol == "IBM"
        assert event.instrument.asset_class == AssetClass.EQUITY
        assert event.open == 148.0
        assert event.close == 150.0
        assert event.volume == 20000.0

    @pytest.mark.asyncio
    async def test_fx_daily_no_volume(self):
        feed = make_feed(
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
            total_limit=2,
        )
        mock_response = _make_mock_response(_fx_daily_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            events = [event async for event in feed.stream()]

        assert len(events) == 2
        assert all(e.volume is None for e in events)
        assert events[0].open == 1.0850

    @pytest.mark.asyncio
    async def test_crypto_periodic_uses_usd_keys(self):
        feed = make_feed(
            symbols=["BTC"],
            asset_class=AssetClass.CRYPTO,
            interval=AlphaVantagePeriodicInterval.DAILY,
            total_limit=2,
        )
        mock_response = _make_mock_response(_crypto_daily_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            events = [event async for event in feed.stream()]

        assert len(events) == 2
        assert events[0].open == 41000.0
        assert events[0].volume == 2000.0

    @pytest.mark.asyncio
    async def test_total_limit_respected(self):
        feed = make_feed(
            symbols=["IBM"],
            total_limit=1,
        )
        mock_response = _make_mock_response(_intraday_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            events = [event async for event in feed.stream()]

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_events_in_chronological_order(self):
        feed = make_feed(symbols=["IBM"], total_limit=2)
        mock_response = _make_mock_response(_intraday_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            events = [event async for event in feed.stream()]

        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    @pytest.mark.asyncio
    async def test_multiple_symbols_sleep_between(self):
        feed = make_feed(symbols=["IBM", "AAPL"], total_limit=2)
        mock_response = _make_mock_response(_intraday_response())

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls, patch(
            "dejavu.data.feeds.alpha_vantage.asyncio.sleep"
        ) as mock_sleep:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_sleep.return_value = None

            events = [event async for event in feed.stream()]

        mock_sleep.assert_called_once_with(12)
        assert len(events) == 4  # 2 symbols × 2 events each

    @pytest.mark.asyncio
    async def test_http_error_raises(self):
        feed = make_feed()
        mock_response = _make_mock_response({}, status_code=403)

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            with pytest.raises(httpx.HTTPStatusError):
                _ = [event async for event in feed.stream()]

    @pytest.mark.asyncio
    async def test_api_error_message_raises_value_error(self):
        feed = make_feed()
        mock_response = _make_mock_response(
            _error_response("Error Message", "Invalid API call")
        )

        with patch(
            "dejavu.data.feeds.alpha_vantage.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)

            with pytest.raises(ValueError, match="Invalid API call"):
                _ = [event async for event in feed.stream()]


@pytest.mark.integration
class TestAlphaVantageIntegration:
    @pytest.mark.asyncio
    async def test_equity_intraday(self):
        feed = AlphaVantageRESTFeed(
            api_key=os.environ["ALPHAVANTAGE_API_KEY"],
            symbols=["IBM"],
            asset_class=AssetClass.EQUITY,
            interval=AlphaVantageIntradayInterval.FIVE_MINUTES,
            total_limit=5,
        )
        events = [event async for event in feed.stream()]
        assert len(events) == 5
        assert all(e.type == EventType.MARKET for e in events)
        assert all(e.volume is not None for e in events)

    @pytest.mark.asyncio
    async def test_fx_daily(self):
        feed = AlphaVantageRESTFeed(
            api_key=os.environ["ALPHAVANTAGE_API_KEY"],
            symbols=["EUR/USD"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.DAILY,
            total_limit=5,
        )
        events = [event async for event in feed.stream()]
        assert len(events) == 5
        assert all(e.volume is None for e in events)

    @pytest.mark.asyncio
    async def test_fx_weekly(self):
        feed = AlphaVantageRESTFeed(
            api_key=os.environ["ALPHAVANTAGE_API_KEY"],
            symbols=["GBP/JPY"],
            asset_class=AssetClass.FX,
            interval=AlphaVantagePeriodicInterval.WEEKLY,
            total_limit=5,
        )
        events = [event async for event in feed.stream()]
        assert len(events) == 5

    @pytest.mark.asyncio
    async def test_crypto_daily(self):
        feed = AlphaVantageRESTFeed(
            api_key=os.environ["ALPHAVANTAGE_API_KEY"],
            symbols=["BTC"],
            asset_class=AssetClass.CRYPTO,
            interval=AlphaVantagePeriodicInterval.DAILY,
            total_limit=5,
        )
        events = [event async for event in feed.stream()]
        assert len(events) == 5
        assert all(e.volume is not None for e in events)

    @pytest.mark.asyncio
    async def test_events_are_chronological(self):
        feed = AlphaVantageRESTFeed(
            api_key=os.environ["ALPHAVANTAGE_API_KEY"],
            symbols=["IBM"],
            asset_class=AssetClass.EQUITY,
            interval=AlphaVantagePeriodicInterval.MONTHLY,
            total_limit=10,
        )
        events = [event async for event in feed.stream()]
        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)
