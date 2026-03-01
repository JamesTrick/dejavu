# Dejavu Backtesting

Dejavu Trading is a python backtesting library that is flexible and composable to support a range of 
asset classes, including stocks, options, futures, and cryptocurrencies.

## Dejavu Model

The model behind Dejavu is hopefully relatively straightforward and makes sense to the real world of trading.

We begin with a portfolio, which is broadly represents a brokerage account.
A portfolio can then have any number of strategies (Strategy), which are the actual trading systems you want.
Each strategy can have different indicators or different assets. Position sizing is done at a strategy level.
Finally, rebalancing can occur at a portfolio level, which allows you to have multiple strategies and rebalance between them.