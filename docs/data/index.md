Data is crucial for backtesting and live-trading. As such, Dejavu provides a few ways to ingest data into your strategies.

## Backtesting

For backtesting, data is stored in Flat-files or ingested through data providers through an API (typically REST).

To get started, we suggest looking at the CSVDataFeed.

::: dejavu.data.feed.CSVDataFeed


## Live-trading

For live trading, we transition from REST or Flat-file datasets to data that is streamed, typically through Websockets for FIX feeds.


