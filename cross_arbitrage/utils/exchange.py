import time
from typing import Dict
from decimal import Decimal
import ccxt
from cross_arbitrage.config.account import AccountConfig
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_exchange_symbol_from_exchange


def create_exchange(params: AccountConfig, proxy: dict = None) -> ccxt.Exchange:
    c = {}
    if params.api_key and params.secret:
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
            if params.password:
                c['password'] = params.password
            return ccxt.okex(c)
        case _ as x:
            raise Exception(f'unknown exchange: {x}')

def get_symbol_min_amount(exchanges: Dict[str, ccxt.Exchange], symbol:str):
    ret = {}
    for ex_name, ex in exchanges.items():
        ex.load_markets()
        exchange_symbol = get_exchange_symbol_from_exchange(ex, symbol)
        symbol_info = ex.market(exchange_symbol.name)
        match ex:
            case ccxt.okex():
                ret[ex_name] = Decimal(str(symbol_info['contractSize'])) * exchange_symbol.multiplier
            case ccxt.binanceusdm():
                ret[ex_name] = Decimal(str(10**(-symbol_info['precision']['amount']))) * exchange_symbol.multiplier
            case _:
                raise Exception(f"unsupport exchange: {ex_name}")
    return max(list(ret.values()))

def get_exchange_name(exchange: ccxt.Exchange) -> str:
    match exchange:
        case ccxt.okex():
            return 'okex'
        case ccxt.binanceusdm():
            return 'binance'
        case _:
            raise Exception(f"get_exchange_name: unsupport ccxt exchange {exchange.name}")
