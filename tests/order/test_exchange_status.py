import ccxt

from cross_arbitrage.order.market import check_exchange_status


def test_check_exchange_status():
    binance = ccxt.binanceusdm()
    state = check_exchange_status(binance)
    assert state.ok
    assert state.status == 'ok'
    assert state.msg == ''

    okex = ccxt.okex()
    state = check_exchange_status(okex)
    assert state.ok
    assert state.status == 'ok'
    assert state.msg == ''
