Data is crucial for backtesting and live-trading. As such, Dejavu provides a few ways to ingest data into your strategies.

## Backtesting

For backtesting, data is stored in Flat-files or ingested through data providers through an API (typically REST).

To get started, we suggest looking at the CSVDataFeed.

::: dejavu.data.feed.CSVDataFeed

## Combining Data Sources

At times, it's not possible to get all data from the same place, or organised. Or Perhaps you want to combine Asset
Classes.

To help with this, we've got CombinedDataFeed, that takes any number of `DataFeed` and combines it for use within your
strategy.

::: dejavu.data.feed.CombinedDataFeed

## Live-trading

For live trading, we transition from REST or Flat-file datasets to data that is streamed, typically through Websockets for FIX feeds.


