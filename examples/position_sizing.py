from datetime import datetime

import numpy as np
import pandas as pd

from data.generate_data import generate_equity_csv, generate_options_csv
from dejavu.data.feed import CSVDataFeed
from dejavu.engine import BacktestEngine
from dejavu.execution.orders import SimulatedExecutionHandler, VolumeWeightedSlippage
from dejavu.indicators.ma import SMA
from dejavu.portfolio import Portfolio
from dejavu.schemas import AssetClass, MarketEvent, Order, OrderType
from dejavu.strategy.base import Strategy
from dejavu.strategy.sizers.risk import PercentRisk


class MACrossOver(Strategy):

    def __init__(self, portfolio, underlying: str):
        super().__init__(portfolio)
        self.underlying = underlying

        # Declare indicators — no maths in the strategy itself
        self.fast_ma = SMA(period=20)
        self.slow_ma = SMA(period=50)
        self.sizer = PercentRisk(risk_pct=0.01)

    def on_market(self, event: MarketEvent):
        orders = []

        if event.asset_class == AssetClass.EQUITY:
            self.fast_ma.update(event.close)
            self.slow_ma.update(event.close)

        if not self.fast_ma.ready or not self.slow_ma.ready:
            return orders

        in_position = self.underlying in self.portfolio.positions

        if self.fast_ma > self.slow_ma and not in_position:
            # Golden cross — go long
            qty = self.sizer.size(
                symbol=self.underlying,
                price=event.close,
                portfolio=self.portfolio,
                stop_distance=event.close * 0.98
            )
            orders.append((
                Order(
                    symbol=self.underlying,
                    quantity=qty,
                    order_type=OrderType.MARKET,
                    asset_class=AssetClass.EQUITY,
                ),
                {"asset_class": AssetClass.EQUITY},
            ))
            print(
                f"  [BUY] {event.timestamp.date()} | "
                f"fast={self.fast_ma.value:.2f} slow={self.slow_ma.value:.2f}"
            )

        elif self.fast_ma.value < self.slow_ma.value and in_position:
            # Death cross — exit
            qty = -self.portfolio.positions[self.underlying].quantity
            orders.append((
                Order(
                    symbol=self.underlying,
                    quantity=qty,
                    order_type=OrderType.MARKET,
                    asset_class=AssetClass.EQUITY,
                ),
                {"asset_class": AssetClass.EQUITY},
            ))
            print(
                f"  [SELL] {event.timestamp.date()} | "
                f"fast={self.fast_ma.value:.2f} slow={self.slow_ma.value:.2f}"
            )

        return orders


def run_test():
    print("\n" + "=" * 60)
    print("  BACKTEST: Moving Average Strategy")
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
    strategy  = MACrossOver(portfolio, underlying="AAPL")
    feed      = CSVDataFeed({"AAPL": "equity.csv"}, asset_classes={"AAPL": AssetClass.EQUITY})
    slippage  = VolumeWeightedSlippage(impact_factor=0.1)
    executor  = SimulatedExecutionHandler(commission_per_contract=0.65, slippage=slippage)
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
    equity       = history["equity"]
    peak         = equity.cummax()
    max_drawdown = ((equity - peak) / peak).min()
    years        = (equity.index[-1] - equity.index[0]).days / 365.25
    cagr         = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1

    print("\n--- Trade Log ---")
    trades_df = pd.DataFrame(portfolio.trades)
    print(trades_df.to_string(index=False))

    print("\n--- Performance Summary ---")
    print(f"  Initial Capital : ${portfolio.initial_capital:>10,.2f}")
    print(f"  Final Equity    : ${equity.iloc[-1]:>10,.2f}")
    print(f"  CAGR            : {cagr:>10.2%}")
    print(f"  Sharpe Ratio    : {sharpe:>10.2f}")
    print(f"  Max Drawdown    : {max_drawdown:>10.2%}")
    print(f"  Total Trades    : {len(portfolio.trades):>10}")
    print("=" * 60)


if __name__ == "__main__":
    run_test()
