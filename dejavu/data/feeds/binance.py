import asyncio

import httpx
import pandas as pd

from dejavu.data.feed import RESTDataFeed
from dejavu.schemas import AssetClass, EventType, Instrument, MarketEvent


class BinanceRESTFeed(RESTDataFeed):
    """Binance is one the world's largest Crypto market places. Whether you're interested in Crypto or not, Binance can be a helpful starting
    point for getting familiar with Dejavu due to low-cost of data access compared to more traditional entrypoints.

    Binance provide numerous base_urls, these are [documented here](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information)
    If you wish to change the base_url you can do so easily:

    ```python
    binance_feed = BinanceRESTFeed(symbols=['BTCUSDT'], interval='1m')
    binance_feed.base_url = 'https://api4.binance.com'
    ```
    """
    def __init__(self, symbols: list[str], interval: str, total_limit: int = 2000):
        """_summary_

        Args:
            symbols (list[str]): List of Symbols to fetch, e.g. ["BTCUSDT", "ETHUSDT"]
            interval (str): Candle interval, e.g. "1m", "5m", "1h", etc. See Binance docs for supported intervals.
            total_limit (int, optional): Total number of candles to fetch per symbol. Defaults to 2000.
        """
        self.symbols = symbols
        self.interval = interval
        self.total_limit = total_limit
        self.base_url = "https://api.binance.com/api/v3/klines"

    async def _fetch_paginated_symbol(self, client: httpx.AsyncClient, symbol: str):
        all_candles = []
        last_ts = None

        batch_limit = 1000

        while len(all_candles) < self.total_limit:
            remaining = self.total_limit - len(all_candles)
            current_limit = min(batch_limit, remaining)

            params = {
                "symbol": symbol,
                "interval": self.interval,
                "limit": current_limit,
            }
            if last_ts:
                params["startTime"] = last_ts + 1

            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            batch = response.json()

            if not batch:
                break

            all_candles.extend(batch)
            last_ts = batch[-1][0] # Update the anchor timestamp

            if len(all_candles) < self.total_limit:
                await asyncio.sleep(0.1)

        return symbol, all_candles

    async def stream(self):
        async with httpx.AsyncClient() as client:
            tasks = [self._fetch_paginated_symbol(client, s) for s in self.symbols]
            results = await asyncio.gather(*tasks)

            for symbol, candles in results:
                inst = Instrument(symbol=symbol, asset_class=AssetClass.CRYPTO)
                for c in candles:
                    yield MarketEvent(
                        type=EventType.MARKET,
                        timestamp=pd.to_datetime(c[0], unit='ms'),
                        instrument=inst,
                        open=float(c[1]), high=float(c[2]),
                        low=float(c[3]), close=float(c[4]),
                        volume=float(c[5])
                    )
