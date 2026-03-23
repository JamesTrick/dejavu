from dejavu.execution.validators import CashValidator, ShortValidator
from dejavu.portfolio import Portfolio
from dejavu.schemas import Instrument, Order, OrderType, Position


def test_cash_validator_rejects_when_insufficient(equity_instrument: Instrument):
    validator = CashValidator()
    order = Order(
        instrument=equity_instrument, quantity=100, order_type=OrderType.MARKET
    )
    portfolio = Portfolio(initial_capital=100)

    valid, reason = validator.validate(order, fill_price=200.0, portfolio=portfolio)

    assert not valid
    assert "Insufficient cash" in reason


def test_cash_validator_allows_sell(equity_instrument: Instrument):
    validator = CashValidator()
    order = Order(
        instrument=equity_instrument, quantity=-100, order_type=OrderType.MARKET
    )
    portfolio = Portfolio(initial_capital=0)

    valid, reason = validator.validate(order, fill_price=200.0, portfolio=portfolio)

    assert valid


class TestCashValidator:
    def test_allows_buy_with_sufficient_cash(self, buy_order, portfolio):
        validator = CashValidator()
        valid, reason = validator.validate(buy_order, 182.0, portfolio)
        assert valid
        assert reason is None

    def test_rejects_buy_with_insufficient_cash(self, buy_order):
        from dejavu.portfolio import Portfolio

        poor_portfolio = Portfolio(initial_capital=100)  # can't afford 100 shares @ 182
        validator = CashValidator()
        valid, reason = validator.validate(buy_order, 182.0, poor_portfolio)
        assert not valid
        assert "Insufficient cash" in reason

    def test_allows_sell_regardless_of_cash(self, sell_order):
        from dejavu.portfolio import Portfolio

        empty_portfolio = Portfolio(initial_capital=0)
        validator = CashValidator()
        valid, reason = validator.validate(sell_order, 182.0, empty_portfolio)
        assert valid

    def test_rejection_message_includes_amounts(self, buy_order):
        from dejavu.portfolio import Portfolio

        poor_portfolio = Portfolio(initial_capital=100)
        validator = CashValidator()
        valid, reason = validator.validate(buy_order, 182.0, poor_portfolio)
        assert "$" in reason


class TestShortValidator:
    def test_allows_buy(self, buy_order, portfolio):
        validator = ShortValidator(allow_short=False)
        valid, reason = validator.validate(buy_order, 182.0, portfolio)
        assert valid

    def test_rejects_naked_short_when_not_allowed(self, sell_order, portfolio):
        validator = ShortValidator(allow_short=False)
        valid, reason = validator.validate(sell_order, 182.0, portfolio)
        assert not valid
        assert "Short selling not permitted" in reason

    def test_allows_short_when_enabled(self, sell_order, portfolio):
        validator = ShortValidator(allow_short=True)
        valid, reason = validator.validate(sell_order, 182.0, portfolio)
        assert valid

    def test_allows_closing_long_position(
        self, sell_order, portfolio, equity_instrument
    ):
        # Simulate existing long position
        portfolio._positions[equity_instrument.symbol] = Position(
            instrument=equity_instrument, quantity=100, avg_cost=180.0
        )
        validator = ShortValidator(allow_short=False)
        valid, reason = validator.validate(sell_order, 182.0, portfolio)
        assert valid
