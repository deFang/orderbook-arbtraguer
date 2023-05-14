from decimal import Decimal
from enum import Enum
from functools import lru_cache
import logging
import time
from typing import Optional
import ccxt
from cross_arbitrage.config.symbol import SymbolConfig
from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.market import market_order
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.exchange import get_bag_size, get_symbol_min_amount, get_symbol_min_amount_by_exchange
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_common_symbol_from_ccxt, get_exchange_symbol_from_exchange
import orjson
import redis

import pydantic


class PositionDirection(str, Enum):
    long = "long"
    short = "short"

    def buy_or_sell(self):
        return "buy" if self == PositionDirection.long else "sell"


class PositionStatus(pydantic.BaseModel):
    direction: PositionDirection
    qty: Decimal
    avg_price: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None


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

    exchange_symbol_names = list(map(lambda s: get_exchange_symbol_from_exchange(exchange, s).name, symbols))
    positions = []

    match exchange:
        case ccxt.binanceusdm():
            positions = exchange.fetch_positions(exchange_symbol_names)
            if len(positions) == 0:
                raise Exception(f'No position found for {symbols}')
        case ccxt.okex():
            okex_positions = []
            for chunk in [exchange_symbol_names[i:i + 10] for i in range(0, len(exchange_symbol_names), 10)]:
                pos = exchange.fetch_positions(chunk)
                pos = [p for p in pos if p['info']['instId'] in chunk]
                okex_positions += pos
                time.sleep(1)
            positions += [pos for pos in okex_positions if pos['info']['mgnMode']== 'cross']
    if not positions:
        return None
    for position in positions:
        symbol = get_common_symbol_from_ccxt(position['symbol'])
        exchange_symbol = get_exchange_symbol_from_exchange(exchange, symbol)
        bag_size = get_bag_size(exchange, symbol)
        # contract_size = Decimal(str(position['contractSize']))
        contracts = Decimal(str(position['contracts']))
        avg_price = Decimal(str(position['entryPrice'])) / exchange_symbol.multiplier if position['entryPrice'] else None
        mark_price = Decimal(str(position['markPrice'])) / exchange_symbol.multiplier if position['markPrice'] else None
        qty = contracts * bag_size
        direction = PositionDirection.long if position['side'] == 'long' else PositionDirection.short
        position_status = PositionStatus(direction=direction, qty=qty, avg_price=avg_price, mark_price=mark_price)
        update_position_status(rc, exchange_name, symbol, position_status)


def refresh_position_loop(ctx: CancelContext, rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list):
    while not ctx.is_canceled():
        start_time = time.time()
        refresh_position_status(rc, exchanges, symbols)
        sleep_with_context(ctx, 20 - (time.time() - start_time))


def _lock_keys_fn(symbol: str, exchange_names: list[str]):
    return [f'{exchange_name}:{symbol}' for exchange_name in exchange_names]


def align_position(rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list, config: OrderConfig):
    order_prefix = 'croTalg'
    unprocessed_symbol_list = []
    exchange_names = config.exchange_pair_names

    for symbol in symbols:
        res = []
        lock_keys = _lock_keys_fn(symbol, exchange_names)
        with rc.pipeline() as pipe:
            pipe.multi()
            # for lock_key in lock_keys:
            #     pipe.sismember('order:signal:processing', lock_key)
            for lock_key in lock_keys:
                pipe.sadd('order:signal:processing', lock_key)
            res = pipe.execute()

        # lock_failed_list = res[:len(lock_keys)]
        # if any(lock_failed_list):
        #     rc.srem('order:signal:processing', *map(lambda x: x[0], filter(lambda x: not x[1], zip(lock_keys, lock_failed_list))))

        locked_list = res[:len(lock_keys)]
        if not all(locked_list):
            # revert locked keys
            if any(locked_list):
                rc.srem('order:signal:processing', 
                        *map(lambda x: x[0], filter(lambda x: x[1], zip(lock_keys, locked_list))))
        else:
            unprocessed_symbol_list.append(symbol)

    refresh_position_status(rc, exchanges, unprocessed_symbol_list)

    for symbol in unprocessed_symbol_list:
        try:
            positions = []
            for exchange_name in exchanges.keys():
                position = get_position_status(rc, exchange_name, symbol)
                positions.append((exchange_name, position))
            min_qty = get_symbol_min_amount(exchanges, symbol)
            if positions[0][1] is None and positions[1][1] is None:
                continue
            if positions[0][1] is None:
                if positions[1][1].qty < min_qty:
                    continue
                else:
                    delta = -positions[1][1].qty
            elif positions[1][1] is None:
                if positions[0][1].qty < min_qty:
                    continue
                else:
                    delta = positions[0][1].qty
            elif positions[0][1].direction == positions[1][1].direction:
                # both position have the same direction
                pos_0 = positions[0][1]
                pos_1 = positions[1][1]
                side_0 = 'sell' if pos_0.direction == PositionDirection.long else 'buy'
                side_1 = 'sell' if pos_1.direction == PositionDirection.long else 'buy'
                if pos_0.qty >= min_qty:
                    market_order(exchanges[positions[0][0]], symbol,
                                 side_0, pos_0.qty,
                                 client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                                 reduce_only=True)
                if pos_1.qty >= min_qty:
                    market_order(exchanges[positions[1][0]], symbol,
                                 side_1, pos_1.qty,
                                 client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                                 reduce_only=True)
                continue
            else:
                delta = positions[0][1].qty - positions[1][1].qty

            symbol_info: SymbolConfig = config.get_symbol_datas(symbol)[0]

            if abs(delta) == Decimal(0):
                continue
            else:
                logging.info(f"align position: {symbol} {positions} {delta}")

            if delta >= min_qty:
                exchange = exchanges[positions[0][0]]
                pos: PositionStatus = positions[0][1]

                if pos.mark_price * delta > Decimal(str(symbol_info.max_notional_per_order)) * 4:
                    logging.warning('align position: too much money in position, skip: symbol={}, positions={}, min_qty={}'.format(
                        symbol, positions, min_qty))
                    continue

                side = 'sell' if pos.direction == PositionDirection.long else 'buy'
                market_order(exchange, symbol,
                             side, delta,
                             client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                             reduce_only=True)
            elif delta <= -min_qty:
                exchange = exchanges[positions[1][0]]
                pos: PositionStatus = positions[1][1]

                if pos.mark_price * (-delta) > Decimal(str(symbol_info.max_notional_per_order)) * 4:
                    logging.warning('align position: too much money in position, skip: symbol={}, positions={}, min_qty={}'.format(
                        symbol, positions, min_qty))
                    continue

                side = 'sell' if pos.direction == PositionDirection.long else 'buy'
                market_order(exchange, symbol,
                             side, -delta,
                             client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                             reduce_only=True)
            else:
                # abs(delta) < min_qty
                exchange = exchanges[positions[0][0]]
                pos: PositionStatus = positions[0][1]
                min_qty_by_exchange = get_symbol_min_amount_by_exchange(exchange, symbol)
                if abs(min_qty_by_exchange) < abs(delta):
                    reduce_only = True
                    if delta > 0:
                        side = 'sell' if pos.direction == PositionDirection.long else 'buy'
                    else:
                        side = 'buy' if pos.direction == PositionDirection.long else 'sell'
                        reduce_only = False
                    market_order(exchange, symbol,
                                 side, abs(delta),
                                 client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                                 reduce_only=reduce_only)
                else:
                    exchange = exchanges[positions[1][0]]
                    pos: PositionStatus = positions[1][1]
                    reduce_only = True
                    if delta > 0:
                        side = 'buy' if pos.direction == PositionDirection.long else 'sell'
                        reduce_only = False
                    else:
                        side = 'sell' if pos.direction == PositionDirection.long else 'buy'
                    market_order(exchange, symbol,
                                 side, abs(delta),
                                 client_id=f"{order_prefix}T{int(time.time() * 1000)}",
                                 reduce_only=reduce_only)

        except Exception as ex:
            logging.error(ex)
            logging.exception(ex)
        finally:
            lock_keys = _lock_keys_fn(symbol, exchange_names)
            rc.srem('order:signal:processing', *lock_keys)


def align_position_loop(ctx: CancelContext, rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], symbols: list, config: OrderConfig):
    sleep_with_context(ctx, 30)

    while not ctx.is_canceled():
        start_time = time.time()
        try:
            align_position(rc, exchanges, symbols, config)
        except Exception as ex:
            logging.error(ex)
            logging.exception(ex)
        sleep_with_context(ctx, 30 - (time.time() - start_time))
