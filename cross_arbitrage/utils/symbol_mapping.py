import json
from typing import Dict

symbol_mapping: Dict[str, Dict[str, str]] = {}
_ccxt2common = {}


class SymbolMappingNotFoundError(Exception):
    pass


def init_symbol_mapping_from_file(file_path: str):
    with open(file_path, "r") as f:
        data = json.load(f)
        symbol_mapping = data['symbol_name_datas']
    init_symbol_mapping(symbol_mapping)


def init_symbol_mapping(mapping: Dict[str, Dict[str, str]]):
    global symbol_mapping
    symbol_mapping = mapping

    for common, mapping in mapping.items():
        ccxt_symbol = mapping.get("ccxt", None)
        if ccxt_symbol:
            _ccxt2common[ccxt_symbol] = common


def get_ccxt_symbol(common_symbol: str) -> str:
    try:
        return symbol_mapping[common_symbol]["ccxt"]
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of 'ccxt' symbol not found for '{common_symbol}'"
        )


def get_exchange_symbol(common_symbol: str, exchange_name: str) -> str:
    try:
        return symbol_mapping[common_symbol][exchange_name]
    except KeyError:
        raise SymbolMappingNotFoundError(
            f"mapping of '{exchange_name}' symbol not found for '{common_symbol}'"
        )


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
                lambda v: v[1][exchange_name] == exchange_symbol,
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
