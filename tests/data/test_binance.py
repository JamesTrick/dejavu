from unittest.mock import AsyncMock, MagicMock

import pytest

from dejavu.data.feeds.binance import BinanceRESTFeed


@pytest.mark.asyncio
@pytest.mark.integration
def test_binance_integration():
    feed = BinanceRESTFeed(symbols=["BTCUSDT"], interval="1m", total_limit=10)
    events = [event for event in feed.stream()]

    assert len(events) == 10
    for event in events:
        assert event.instrument.symbol == "BTCUSDT"
        assert event.open > 0
        assert event.high > 0
        assert event.low > 0
        assert event.close > 0
        assert event.volume > 0


@pytest.mark.asyncio
@pytest.mark.integration
def test_binance_multiple_symbols_integration():
    symbols = ["BTCUSDT", "ETHUSDT"]
    limit = 10
    feed = BinanceRESTFeed(symbols=symbols, interval="1m", total_limit=limit)

    events = []
    for event in feed.stream():
        events.append(event)

    # Total events should be symbols * limit
    assert len(events) == len(symbols) * limit

    # Verify we got data for both
    received_symbols = {e.instrument.symbol for e in events}
    assert received_symbols == set(symbols)


@pytest.mark.asyncio
def test_binance_feed_unit(mocker):
    mock_candle = [1672531200000, "16000.0", "16100.0", "15900.0", "16050.0", "100.5"]
    mock_response = [mock_candle]

    mock_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    mock_res_obj = MagicMock()
    mock_res_obj.json.return_value = mock_response
    mock_res_obj.raise_for_status = MagicMock()
    mock_get.return_value = mock_res_obj

    feed = BinanceRESTFeed(symbols=["BTCUSDT"], interval="1m", total_limit=1)
    events = []
    for event in feed.stream():
        events.append(event)

    assert len(events) == 1
    event = events[0]
    assert event.instrument.symbol == "BTCUSDT"
    assert event.close == 16050.0
    assert event.volume == 100.5

    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert kwargs["params"]["symbol"] == "BTCUSDT"


def test_binance_pagination_unit(mocker):
    first_response = [[1000, "10", "11", "9", "10", "100"]]
    second_response = []

    mock_client = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    mock_client.side_effect = [
        MagicMock(json=lambda: first_response, raise_for_status=lambda: None),
        MagicMock(json=lambda: second_response, raise_for_status=lambda: None),
    ]

    feed = BinanceRESTFeed(symbols=["BTCUSDT"], interval="1m", total_limit=2)
    events = [e for e in feed.stream()]

    assert len(events) == 1
    # Verify the second call used the correct startTime (1000 + 1)
    args, kwargs = mock_client.call_args_list[1]
    assert kwargs["params"]["startTime"] == 1001
