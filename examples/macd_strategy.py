import numpy as np
import pandas as pd

from dejavu.data.feed import CSVDataFeed
from dejavu.engine import BacktestEngine
from dejavu.execution.commission import AssetClassCommission, TieredPerShareCommission
from dejavu.execution.orders import (
    NoSlippage,
    SimulatedExecutionHandler,
)
from dejavu.execution.validators import ShortValidator
from dejavu.indicators.macd import MACD
from dejavu.portfolio import Portfolio
from dejavu.schemas import AssetClass, MarketEvent, Order, OrderType
from dejavu.strategy.base import Strategy


class MACDStrategy(Strategy):
    def __init__(self, portfolio, underlying: str):
        super().__init__(portfolio)
        self.underlying = underlying
        self.macd = MACD()

    def on_market(self, event: MarketEvent) -> list[Order] | list:
        orders = []

        self.macd.update(event.close)

        if not self.macd.ready:
            return orders

        in_position = self.underlying in self.portfolio.positions

        if self.macd._value > 0 and not in_position:
            # MACD line above signal line — go long
            orders.append(
                self.buy(
                    instrument=event.instrument,
                    qty=50,
                    order_type=OrderType.MARKET,
                )
            )
            print(
                f"  [BUY] {event.timestamp.date()} | "
                f"MACD={self.macd._value:.2f} Signal={self.macd.signal_line:.2f}"
            )
        elif self.macd._value < 0 and in_position:
            # MACD line below signal line — exit
            orders.append(
                self.close(
                    instrument=event.instrument,
                    order_type=OrderType.MARKET,
                )
            )
            print(
                f"  [SELL] {event.timestamp.date()} | "
                f"MACD={self.macd._value:.2f} Signal={self.macd.signal_line:.2f}"
            )

        return orders


def run_test():
    print("\n" + "=" * 60)
    print("  BACKTEST: Moving Average Strategy")
    print("=" * 60)

    # ── Wire up components ────────────────────────────────────────
    portfolio = Portfolio(initial_capital=25_000)
    strategy = MACDStrategy(portfolio, underlying="AAPL")
    feed = CSVDataFeed(path="../data/equity.csv", asset_class=AssetClass.EQUITY)
    commission_model = AssetClassCommission(
        models={
            AssetClass.EQUITY: TieredPerShareCommission(
                rate=0.005,
                minimum=1.00,
                max_pct_notional=0.01,
            )
        }
    )
    executor = SimulatedExecutionHandler(
        commission=commission_model,
        slippage=NoSlippage(),
        validators=[ShortValidator()],
    )
    engine = BacktestEngine(feed, strategy, portfolio, executor)

    # ── Run ───────────────────────────────────────────────────────
    print("\n--- Activity Log ---")
    engine.run()

    # ── Results ───────────────────────────────────────────────────
    history = (
        pd.DataFrame(portfolio.history)
        .drop_duplicates("timestamp")
        .set_index("timestamp")
    )
    returns = history["equity"].pct_change().dropna()

    sharpe = np.sqrt(252) * returns.mean() / returns.std() if returns.std() > 0 else 0
    equity = history["equity"]
    peak = equity.cummax()
    max_drawdown = ((equity - peak) / peak).min()
    years = (equity.index[-1] - equity.index[0]).days / 365.25
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1

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
