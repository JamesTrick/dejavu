Validators in Dejavu can be seen as a harness. While they could replicate the logic in your
strategy, we strongly suggest using Validators as a safety harness.

Validators don't stop the signal generation of a trade as logic in your trading Strategy would, but they do stop the
placement of orders if conditions aren't met.

For example, if you had a cash-only long-only strategy, you can also validate this by going:

```python
from dejavu.execution.orders import SimulatedExecutionHandler
from dejavu.execution.validators import CashValidator, ShortValidator

slippage = ...
commission = ...
executor = SimulatedExecutionHandler(slippage, commission, validators=[CashValidator(), ShortValidator(allow_short=False)])
```

This way, if your strategy logic happens to generate a short signal, the signal will still be generated, but the order
will not be executed.

Dejavu provides a few validators out the of the box, but we suggest creating your own to suit your business or
investment needs.

::: dejavu.execution.validators.CashValidator
    options:
        show_root_heading: true
        heading_level: 2

::: dejavu.execution.validators.MarginValidator
    options:
        show_root_heading: true
        heading_level: 2

::: dejavu.execution.validators.ShortValidator
    options:
        show_root_heading: true
        heading_level: 2

## Extending Validators

Validators being composable in nature make them really helpful for maintaining a level of predictability and safety.

They also have access to your `Portfolio`, so you can get quite advanced with the validation you want. Perhaps ensuring
you're not overweight in a stock or asset class for example, or ensuring you're not making too many trades in a day.