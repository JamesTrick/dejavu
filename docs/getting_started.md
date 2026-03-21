## Let's build a strategy!

You're here, let's get started. Dejavu is a flexible and powerful tool for backtesting. Let's start by building a nice
straightforward trading strategy to get you a solid foundation.

Ok, so the heart of the trading strategy, is indeed the Strategy itself. But, the stragegy only works if there's data.
So, let's bring in the data.

### Adding data to Dejavu

Dejavu has a few ways to get data, the easiest way would be to use a CSV. Or if you're interested in Crypto,
using our BinanceRESTFeed, which requires no API keys. For now, let's use a CSV.

```python
from dejavu.data.feed import CSVDataFeed

equity_data = CSVDataFeed(path='equity.csv', asset_class='EQUITY')

```

For developers, our DataFeeds are lazy, they don't actually process data until they're called with `stream()` method.

### Building a strategy

Let's start by creating a simple strategy that buys when the price goes up and sells when the price goes down.

```python
from dejavu.strategy import Strategy
from dejavu.portfolio import Portfolio

class SimpleStrategy(Strategy):
    def __init__(self, portfolio: Portfolio):
        super().__init__(portfolio)
        self.previous_price = None

    def on_market(self, event):
        if self.previous_price is None:
            self.previous_price = event.close

        position = self.portfolio.positions.get(event.instrument.symbol)
        has_position = position is not None and position.quantity > 0

        orders = []
        if event.close > self.previous_price and not has_position:
            orders.append(self.buy(instrument=event.instrument, qty=10))
        elif event.close < self.previous_price and has_position:
            orders.append(self.close(instrument=event.instrument))

        self.previous_price = event.close
        return orders
```

1. `on_market` is the method that is called everytime a new datapoint, or more specifically `MarketEvent` is received by the strategy.
2. This says, if the current close is higher than the previous price, we buy.
3. Conversely, this says if closing price is lower the previous price, we sell.  

### Adding Commission

Ok. Now that we have our strategy let's run it! Wait, surely we need to pay someone to trade? Yes, it's highly like we'll
be paying commission.

To add commissions to your strategy, you can use an ExecutionHandler. The basic one and most easiest one to get started
is the `CommissionOnlyHandler()`

```python
from dejavu.execution.commission import PerContractCommission
from dejavu.execution.orders import CommissionOnlyHandler

commission = PerContractCommission(rate=0.65)
executor  = CommissionOnlyHandler(commission)
```

### Creating a portfolio

Now, let's add our capital! We need money to trade.

```python
from dejavu.portfolio import Portfolio
from dejavu.engine import BacktestEngine

portfolio = Portfolio(initial_capital=10_000)

engine = BacktestEngine(equity_data, strategy, portfolio, executor)
engine.run()
```

Of course, there's so much more you can do with Dejavu including:

* Adding indicators to your strategy
* Position sizing
* Portfolio risk and rebalancing
* Adding in Options or other aset classes
* More complex and accurate commission structures
* Modelling slippage, and different types of orders such as Limit.

For now, you have a working strategy! Congrats. Go forth and explore.


```python
from dejavu.portfolio import Portfolio
from dejavu.engine import BacktestEngine
from dejavu.execution.commission import PerContractCommission
from dejavu.execution.orders import CommissionOnlyHandler
from dejavu.strategy import Strategy
from dejavu.data.feed import CSVDataFeed

class SimpleStrategy(Strategy):
    def __init__(self):
        self.previous_price = None
        
    def on_market(self, event): # (1)!
        if self.previous_price is None:
            return []
        
        if event.close > self.previous_price: # (2)!
            self.buy()
        elif event.close < self.previous_price: # (3)!
            self.sell()
        self.previous_price = event.close

equity_data = CSVDataFeed(path='equity.csv', asset_class='EQUITY')
strategy = SimpleStrategy()
commission = PerContractCommission(rate=0.65)
executor  = CommissionOnlyHandler(commission)
portfolio = Portfolio(initial_capital=10_000)
engine = BacktestEngine(equity_data, strategy, portfolio, executor)
engine.run()
```