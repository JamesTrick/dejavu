## Your first Dejavu Strategy

Dejavu is a flexible and powerful tool for backtesting, but it can be a little confusing to begin with.

Let's start by creating a simple strategy that buys when the price goes up and sells when the price goes down.

```python
from dejavu.strategy import Strategy

class SimpleStrategy(Strategy):
    def on_market(self, event):
        if self.previous_price is None:
            return []
        
        if event.close > self.previous_price:
            self.buy()
        elif event < self.previous_price:
            self.sell()
        self.previous_price = event.close
```


Of course, this is a relatively simple strategy, and certainly not reflective of reality. Let's start by addressing the
inveitable. Commissions.

To add commissions to your strategy, you can use an ExecutionHandler. The basic one and most easiest one to get started
is the `CommissionOnlyHandler()`

```python
from dejavu.execution.orders import CommissionOnlyHandler

executor  = CommissionOnlyHandler(commission_per_contract=0.65)
engine    = BacktestEngine(feed, strategy, portfolio, executor)
```

For more advanced or complex trading strategies, you may want to look at our other ExecutionHandlers, or craft your own.

For example SimulatedExecutionHandler handles slippage which becomes critical in large trades or less liquid asset classes.