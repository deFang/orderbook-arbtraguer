from decimal import Decimal
from enum import Enum
import logging
import time
import ccxt
from cross_arbitrage.order.market import market_order
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.exchange import get_symbol_min_amount
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
            refresh_symbol_position_status(
                rc, exchange_name, exchange, symbols)
        except Exception as ex:
            logging.error(
                f'Failed to refresh position status for {exchange_name}:{symbols}: {type(ex)}')
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
        sleep_with_context(ctx, 10 - (time.time() - start_time))


def align_position(rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list):
    order_prefix = 'croTalg'
    unprocessed_symbol_list = []
    for symbol in symbols:
        res = []
        with rc.pipeline() as pipe:
            pipe.multi()
            pipe.sismember('order:signal:processing', symbol)
            pipe.sadd('order:signal:processing', symbol)
            res = pipe.execute()
        if res[0] == True:
            continue
        else:
            unprocessed_symbol_list.append(symbol)

    refresh_position_status(rc, exchanges, unprocessed_symbol_list)
    
    for symbol in unprocessed_symbol_list:
        try:
            positions = []
            for exchange_name in exchanges.keys():
                position = get_position_status(rc, exchange_name, symbol)
                if position == None:
                    position = PositionStatus(
                        direction=PositionDirection.long, qty=Decimal(0))
                positions.append((exchange_name, position))
            delta = positions[0][1].qty - positions[1][1].qty
            min_qty = get_symbol_min_amount(exchanges, symbol)
            if abs(delta) > min_qty:
                logging.info(f"align position: {symbol} {positions}")
            if delta > min_qty:
                exchange = exchanges[positions[0][0]]
                side = 'sell' if positions[0][1].direction == PositionDirection.long else 'buy'
                market_order(exchange, symbol,
                                side, delta,
                                client_id=f"{order_prefix}T{int(time.time() * 1000)}")
            elif delta < -min_qty:
                exchange = exchanges[positions[1][0]]
                side = 'sell' if positions[1][1].direction == PositionDirection.long else 'buy'
                market_order(exchange, symbol,
                                side, -delta,
                                client_id=f"{order_prefix}T{int(time.time() * 1000)}")
        except Exception as ex:
            logging.error(ex)
            logging.exception(ex)
        finally:
            rc.srem('order:signal:processing', symbol)


def align_position_loop(ctx: CancelContext, rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list):
    while not ctx.is_canceled():
        start_time = time.time()
        align_position(rc, exchanges, symbols)
        sleep_with_context(ctx, 30 - (time.time() - start_time))
