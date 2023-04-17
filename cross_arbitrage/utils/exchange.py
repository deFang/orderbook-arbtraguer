import time
from typing import Dict
import ccxt
from cross_arbitrage.config.account import AccountConfig
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol


def create_exchange(params: AccountConfig, proxy: dict = None) -> ccxt.Exchange:
    c = {
        'apiKey': params.api_key,
        'secret': params.secret,
    }
    if proxy:
        c['proxies'] = proxy

    match params.exchange_name:
        case 'binance':
            return ccxt.binanceusdm(c)
        case 'okex':
            c['password'] = params.password
            return ccxt.okex(c)
        case _ as x:
            raise Exception(f'unknown exchange: {x}')

def get_symbol_min_amount(exchanges: Dict[str, ccxt.Exchange], symbol:str):
    ccxt_symbol = get_ccxt_symbol(symbol)
    ret = {}
    for ex_name, ex in exchanges.items():
        ex.load_markets()
        symbol_info = ex.market(ccxt_symbol)
        if float(symbol_info['contractSize']) == 1.0:
            ret[ex_name] = 10**(-symbol_info['precision']['amount'])
        else:
            ret[ex_name] = symbol_info['contractSize']
    return max(list(ret.values()))

def get_exchange_name(exchange: ccxt.Exchange) -> str:
    match exchange:
        case ccxt.okex():
            return 'okex'
        case ccxt.binanceusdm():
            return 'binance'
        case _:
            raise Exception(f"get_exchange_name: unsupport ccxt exchange {exchange.name}")
