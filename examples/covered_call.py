from datetime import datetime

import numpy as np
import pandas as pd

from data.generate_data import generate_equity_csv, generate_options_csv
from dejavu.data.feed import CombinedDataFeed, CSVDataFeed
from dejavu.engine import BacktestEngine
from dejavu.execution.commission import (
    AssetClassCommission,
    PerContractCommission,
    TieredPerShareCommission,
)
from dejavu.execution.orders import SimulatedExecutionHandler, VolumeWeightedSlippage
from dejavu.portfolio import Portfolio
from dejavu.schemas import AssetClass
from dejavu.strategy.covered_call import CoveredCallStrategy


def run_test():
    print("\n" + "=" * 60)
    print("  BACKTEST: Covered Call Strategy")
    print("=" * 60)

    start = datetime(2024, 1, 2)

    # ── Generate data ─────────────────────────────────────────────
    equity_rows = generate_equity_csv(
        path="equity.csv",
        symbol="AAPL",
        start=start,
        days=252,
        start_price=180.0,
    )
    generate_options_csv(
        path="options.csv",
        equity_rows=equity_rows,
        underlying="AAPL",
        expiry_cycles=4,
    )

    # ── Wire up components ────────────────────────────────────────
    portfolio = Portfolio(initial_capital=25_000)
    strategy  = CoveredCallStrategy(portfolio, underlying="AAPL")
    equity_feed = CSVDataFeed("equity.csv", asset_class=AssetClass.EQUITY)
    options_feed = CSVDataFeed("options.csv", asset_class=AssetClass.OPTION)
    feed = CombinedDataFeed(equity_feed, options_feed)
    slippage  = VolumeWeightedSlippage(impact_factor=0.1)
    commission_model = AssetClassCommission(
        models={
            AssetClass.EQUITY: TieredPerShareCommission(
                rate=0.005,
                minimum=1.00,
                max_pct_notional=0.01,
            ),
            AssetClass.OPTION: PerContractCommission(rate=0.65),
        },
        default=PerContractCommission(rate=0.65),
    )
    executor  = SimulatedExecutionHandler(commission=commission_model, slippage=slippage)
    engine    = BacktestEngine(feed, strategy, portfolio, executor)

    # ── Run ───────────────────────────────────────────────────────
    print("\n--- Activity Log ---")
    engine.run()

    # ── Results ───────────────────────────────────────────────────
    history = pd.DataFrame(portfolio.history).drop_duplicates("timestamp").set_index("timestamp")
    returns = history["equity"].pct_change().dropna()

    sharpe = (
        np.sqrt(252) * returns.mean() / returns.std()
        if returns.std() > 0 else 0
    )

    equity = history["equity"]
    peak = equity.cummax()
    max_drawdown = ((equity - peak) / peak).min()

    years = max((equity.index[-1] - equity.index[0]).total_seconds() / (365.25 * 86400), 0.001)
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1

    print("\n--- Trade Log ---")
    trades_df = pd.DataFrame(portfolio.trade_journal)
    print(trades_df.to_string(index=False))

    print("\n--- Performance Summary ---")
    print(f"  Initial Capital : ${portfolio.initial_capital:>10,.2f}")
    print(f"  Final Equity    : ${equity.iloc[-1]:>10,.2f}")
    print(f"  CAGR            : {cagr:>10.2%}")
    print(f"  Sharpe Ratio    : {sharpe:>10.2f}")
    print(f"  Max Drawdown    : {max_drawdown:>10.2%}")
    print(f"  Total Trades    : {len(trades_df):>10}")
    print("=" * 60)


if __name__ == "__main__":
    run_test()
