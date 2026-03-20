When a market order is placed, the price that you receive is dependent on the other side of the transaction (either buyers or sellers), the
difference between the price you transact at the the fill-price is called Slippage.

Slippage can be modelled directly through looking through order books. Though this is out of scope for Dejavu currently. Instead, Dejavu
provides a few simple slippage models out the box. But, like everything in Dejavu, you can extend and create the perfect slippage model
for your backtest.

::: dejavu.execution.orders.NoSlippage
    options:
        show_root_heading: true
        heading_level: 2


::: dejavu.execution.orders.VolumeWeightedSlippage
    options:
        show_root_heading: true
        heading_level: 2
