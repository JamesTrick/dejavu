from dejavu.indicators.ma import SMA


def test_sma():
    sma = SMA(period=3)

    assert not sma.ready
    sma.update(10)
    assert not sma.ready

    sma.update(20)
    assert not sma.ready

    sma.update(30)
    assert sma.value == 20
    assert sma.ready

    # Test rolling behavior
    sma.update(40)
    assert sma.value == 30
