from decimal import Decimal
from enum import Enum
import logging
import time
import ccxt
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_common_symbol_from_ccxt
import orjson
import redis

import pydantic


class PositionDirection(str, Enum):
    long = "long"
    short = "short"


class PositionStatus(pydantic.BaseModel):
    direction: PositionDirection
    qty: Decimal


def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


def position_status_key():
    return "order:position_status"


def update_position_status(rc: redis.Redis, exchange_name, symbol, position_status: PositionStatus):
    rc.hset(position_status_key(),
            f'{exchange_name}:{symbol}', orjson.dumps(position_status.dict(), default=_json_default))


def get_position_status(rc: redis.Redis, exchange_name, symbol):
    data = rc.hget(position_status_key(), f'{exchange_name}:{symbol}')
    if data is None:
        return None
    return PositionStatus.parse_obj(orjson.loads(data))


def refresh_position_status(rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list):
    for exchange_name, exchange in exchanges.items():
        try:
            refresh_symbol_position_status(rc, exchange_name, exchange, symbols)
        except Exception as ex:
            logging.error(f'Failed to refresh position status for {exchange_name}:{symbols}: {type(ex)}')
            logging.exception(ex)


def refresh_symbol_position_status(rc: redis.Redis, exchange_name: str, exchange: ccxt.Exchange, symbols: list[str]):
    exchange: ccxt.binanceusdm | ccxt.okex
    ccxt_symbols = list(map(get_ccxt_symbol, symbols))
    positions = []

    match exchange:
        case ccxt.binanceusdm():
            positions = exchange.fetch_positions(ccxt_symbols)
            if len(positions) == 0:
                raise Exception(f'No position found for {symbols}')
        case ccxt.okex():
            for chunk in [ccxt_symbols[i:i + 20] for i in range(0, len(ccxt_symbols), 20)]:
                positions += exchange.fetch_positions(chunk)
    if not positions:
        return None
    for position in positions:
        symbol = get_common_symbol_from_ccxt(position['symbol'])
        contract_size = Decimal(str(position['contractSize']))
        contracts = Decimal(str(position['contracts']))
        qty = contracts * contract_size
        direction = PositionDirection.long if position['side'] == 'long' else PositionDirection.short
        position_status = PositionStatus(direction=direction, qty=qty)
        update_position_status(rc, exchange_name, symbol, position_status)


def refresh_position_loop(ctx: CancelContext, rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list):
    while not ctx.is_canceled():
        start_time = time.time()
        refresh_position_status(rc, exchanges, symbols)
        time.sleep(10 - (time.time() - start_time))
