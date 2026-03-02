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