import json
from typing import Dict
import ccxt

import pydantic


class ExchangeSymbol(pydantic.BaseModel):
    name: str | list[str]
    multiplier: int = 1


symbol_mapping: Dict[str, Dict[str, ExchangeSymbol]] = {}
_ccxt2common = {}


class SymbolMappingNotFoundError(Exception):
    pass


def init_symbol_mapping_from_file(file_path: str):
    with open(file_path, "r") as f:
        data = json.load(f)
        symbol_mapping = data['symbol_name_datas']
    init_symbol_mapping(symbol_mapping)


def init_symbol_mapping(mapping: Dict[str, Dict[str, str | dict]]):
    global symbol_mapping

    for common, m in mapping.items():
        mp = {}
        for exchange, symbol_info in m.items():
            if isinstance(symbol_info, str):
                mp[exchange] = ExchangeSymbol(name=symbol_info)
            elif isinstance(symbol_info, dict):
                mp[exchange] = ExchangeSymbol(**symbol_info)
            else:
                raise ValueError("invalid symbol mapping type: {}({})".format(
                    type(symbol_info), symbol_info))

        ccxt_symbol = mp.get("ccxt", None)
        if ccxt_symbol:
            if isinstance(ccxt_symbol.name, str):
                ccxt_symbol_names = [ccxt_symbol.name]
            else:
                ccxt_symbol_names = ccxt_symbol.name

            for s in ccxt_symbol_names:
                _ccxt2common[s] = common

        symbol_mapping[common] = mp


def get_ccxt_symbol(common_symbol: str) -> str:
    raise NotImplementedError()
    try:
        return symbol_mapping[common_symbol]["ccxt"].name
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of 'ccxt' symbol not found for '{common_symbol}'"
        )


def get_exchange_symbol(common_symbol: str, exchange_name: str) -> ExchangeSymbol:
    try:
        return symbol_mapping[common_symbol][exchange_name]
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of '{exchange_name}' symbol not found for '{common_symbol}'"
        )


def get_exchange_symbol_from_exchange(exchange: ccxt.Exchange, symbol: str) -> ExchangeSymbol:
    match exchange:
        case ccxt.okex():
            return get_exchange_symbol(symbol, "okex")
        case ccxt.binanceusdm():
            return get_exchange_symbol(symbol, "binance")
        case _:
            raise Exception(
                f"get_exchange_symbol_from_exchange: unsupport ccxt exchange {exchange.name}")


def get_common_symbol_from_ccxt(ccxt_symbol: str) -> str:
    try:
        return _ccxt2common[ccxt_symbol]
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of 'ccxt' symbol not found for '{ccxt_symbol}'"
        )


def get_common_symbol_from_exchange_symbol(
    exchange_symbol: str, exchange_name: str
) -> str:
    try:
        # print(list(symbol_mapping['symbols'].items())[0])
        filtered = list(
            filter(
                lambda v: v[1][exchange_name].name == exchange_symbol,
                symbol_mapping.items(),
            )
        )
        if len(filtered) > 0:
            # print(filtered[0][0])
            return filtered[0][0]
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of '{exchange_name}' symbol not found for '{exchange_symbol}'"
        )
