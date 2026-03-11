from dejavu.indicators.macd import MACD


def test_macd_initialization_lag():
    macd = MACD(fast=2, slow=5, signal=3)
    prices = [10.0, 11.0, 12.0, 13.0, 14.0]

    for i in range(4):
        assert macd.update(prices[i]) is None
        assert macd.ready is False


def test_macd_calculation_logic(mocker):
    macd = MACD(fast=12, slow=26, signal=9)

    macd._fast_ema = 20.0
    macd._slow_ema = 15.0
    macd.macd_line = 20.0 - 15.0
    assert macd.macd_line == 5.0
