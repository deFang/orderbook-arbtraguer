from os.path import join
from cross_arbitrage.fetch.utils.common import get_project_root
from cross_arbitrage.utils.symbol_mapping import get_common_symbol_from_ccxt, get_common_symbol_from_exchange_symbol, get_exchange_symbol, init_symbol_mapping_from_file


def test_get_exchange_symbol(): 
    init_symbol_mapping_from_file(join(get_project_root(), "configs/common_config.json"))
    assert get_exchange_symbol('BNB/USDT', 'okex').name == 'BNB-USDT-SWAP'
    assert get_exchange_symbol('BNB/USDT', 'binance').name == 'BNBUSDT'
    assert get_exchange_symbol('PEPE/USDT', 'okex').name == 'PEPE-USDT-SWAP'
    assert get_exchange_symbol('PEPE/USDT', 'binance').name == '1000PEPEUSDT'

def test_get_common_symbol_from_ccxt(): 
    init_symbol_mapping_from_file(join(get_project_root(), "configs/common_config.json"))
    assert get_common_symbol_from_ccxt('ETH/USDT:USDT') == 'ETH/USDT'
    assert get_common_symbol_from_ccxt('PEPE/USDT:USDT') == 'PEPE/USDT'
    assert get_common_symbol_from_ccxt('1000PEPE/USDT:USDT') == 'PEPE/USDT'

def test_get_common_symbol_from_exchange_symbol(): 
    init_symbol_mapping_from_file(join(get_project_root(), "configs/common_config.json"))
    assert get_common_symbol_from_exchange_symbol('BNB-USDT-SWAP', 'okex') == 'BNB/USDT'
    assert get_common_symbol_from_exchange_symbol('ETHUSDT', 'binance') == 'ETH/USDT'
    assert get_common_symbol_from_exchange_symbol('PEPE-USDT-SWAP', 'okex') == 'PEPE/USDT'
    assert get_common_symbol_from_exchange_symbol('1000PEPEUSDT', 'binance') == 'PEPE/USDT'
