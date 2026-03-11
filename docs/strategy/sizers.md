---
title: Position Sizers
---

Positon sizers are crucial for managing risk and optimising your returns in trading. They determine how much capital 
to allocate to each trade based on various factors such as risk tolerance, account size, and market conditions. 

Of course, the naive way to size a position in Dejavu is to use the `buy()` and `sell()` methods without any arguments, 
which will default to buying or selling one unit of the asset. However, this approach does not take into account 
the size of your account or the risk associated with the trade.

Here are some common types of position sizers:

::: dejavu.strategy.sizers.fixed.FixedUnits
    options:
        show_root_heading: true
        heading_level: 2

::: dejavu.strategy.sizers.fixed.FixedDollar
    options:
        show_root_heading: true
        heading_level: 2

::: dejavu.strategy.sizers.risk.PercentRisk
    options:
        show_root_heading: true
        heading_level: 2