
from decimal import Decimal
import logging
from typing import Dict, NamedTuple, Optional
import ccxt

import orjson
import redis

from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.order.position_status import PositionDirection, get_position_status, PositionStatus
from cross_arbitrage.utils.cache import expire_cache, ExpireCache
from cross_arbitrage.utils.exchange import get_symbol_min_amount
from .threshold import Threshold


class OrderSignal(NamedTuple):
    symbol: str
    maker_side: str
    maker_exchange: str
    maker_price: Decimal
    maker_qty: Decimal
    taker_side: str
    taker_exchange: str
    taker_price: Decimal
    # taker_expect_price: Decimal
    orderbook_ts: int
    cancel_order_threshold: float
    maker_position: Optional[PositionStatus]
    is_reduce_position: bool = False

_cache = ExpireCache(1)

def get_position(rc: redis.Redis, exchange_name: str, symbol: str):
    key = (exchange_name, symbol)
    ret = _cache.get(key)
    if ret is None:
        ret = get_position_status(rc, exchange_name, symbol)
        _cache.set(key, ret)
    return ret


def fetch_orderbooks_from_redis(ctx: CancelContext,
                                rc: redis.Redis,
                                stream: str,
                                last_id: str,
                                limit: int,
                                block: Optional[int] = None) -> Optional[list]:
    """
    return: [(stream_id (symbol order_books:{exchange => orderbook}))]
    """
    try:
        data = rc.xread({stream: last_id}, count=limit, block=block)
    except redis.RedisError as e:
        logging.warning(f'get a redis error: {type(e)}: {e}')
        return None
    if not data:
        return None

    data = data[0][1]
    ret = []
    for rid, values in data:
        for symbol, ob in values.items():
            symbol: bytes
            ret.append((rid, (symbol.decode(), orjson.loads(ob))))
    return ret


def get_signal_from_orderbooks(rc: redis.Redis, exchanges: dict[str, ccxt.Exchange], 
                               config: OrderConfig, thresholds: dict[str, Threshold], orderbooks: list) -> Dict[str, OrderSignal]:
    """
    return: {symbol: `OrderSignal`}
    """

    ret = {}
    processed_symbols = set()
    symbols = [s.symbol_name for s in config.cross_arbitrage_symbol_datas]
    for symbol, ob in reversed(orderbooks):
        if symbol in processed_symbols:
            continue

        if symbol not in symbols:
            continue

        processed_symbols.add(symbol)

        for symbol_config in config.get_symbol_datas(symbol):
            maker_exchange = symbol_config.makeonly_exchange_name
            taker_exchange = (set(config.exchange_pair_names) - {maker_exchange}).pop()

            threshold = thresholds[maker_exchange].get_symbol_thresholds(symbol)

            maker_ob = ob[maker_exchange]
            taker_ob = ob[taker_exchange]

            # TODO: check position
            maker_symbol_position = get_position(rc, maker_exchange, symbol)
            symbol_minimum_qty = get_symbol_min_amount(exchanges, symbol)
            high_delta = threshold.short_threshold.increase_position_threshold
            high_cancel_threshold = threshold.short_threshold.cancel_increase_position_threshold

            low_delta = threshold.long_threshold.increase_position_threshold
            low_cancel_threshold = threshold.long_threshold.cancel_increase_position_threshold

            position_qty = None
            is_reduce_position = False
            # reduce position
            if maker_symbol_position and maker_symbol_position.qty > symbol_minimum_qty:
                position_qty = maker_symbol_position.qty
                if maker_symbol_position.direction == PositionDirection.long:
                    high_delta = threshold.long_threshold.decrease_position_threshold
                    high_cancel_threshold = threshold.long_threshold.cancel_decrease_position_threshold
                elif maker_symbol_position.direction == PositionDirection.short:
                    low_delta = threshold.short_threshold.decrease_position_threshold
                    low_cancel_threshold = threshold.short_threshold.cancel_decrease_position_threshold

            # if maker exchange price if higher
            if float(maker_ob['asks'][0][0]) > float(taker_ob['asks'][0][0]) * float(1 + high_delta):
                qty = Decimal(taker_ob['asks'][0][1])
                if position_qty is not None:
                    qty = min(qty, position_qty)
                    if maker_symbol_position and maker_symbol_position.direction == PositionDirection.long:
                        is_reduce_position = True
                ret[symbol] = OrderSignal(
                    symbol=symbol,
                    maker_side='sell',
                    maker_exchange=maker_exchange,
                    maker_price=Decimal(maker_ob['asks'][0][0]),
                    maker_qty= qty,
                    taker_side='buy',
                    taker_exchange=taker_exchange,
                    taker_price=Decimal(taker_ob['asks'][0][0]),
                    orderbook_ts=maker_ob['ts'],
                    cancel_order_threshold=float(high_cancel_threshold),
                    maker_position=maker_symbol_position,
                    is_reduce_position = is_reduce_position,
                )
            # else if maker exchange price if lower
            elif float(maker_ob['bids'][0][0]) < float(taker_ob['bids'][0][0]) * float(1 + low_delta):
                qty = Decimal(taker_ob['bids'][0][1])
                if position_qty is not None:
                    qty = min(qty, position_qty)
                    if maker_symbol_position and maker_symbol_position.direction == PositionDirection.short:
                        is_reduce_position = True
                ret[symbol] = OrderSignal(
                    symbol=symbol,
                    maker_side='buy',
                    maker_exchange=maker_exchange,
                    maker_price=Decimal(maker_ob['bids'][0][0]),
                    maker_qty=qty,
                    taker_side='sell',
                    taker_exchange=taker_exchange,
                    taker_price=Decimal(taker_ob['bids'][0][0]),
                    orderbook_ts=maker_ob['ts'],
                    cancel_order_threshold=float(low_cancel_threshold),
                    maker_position=maker_symbol_position,
                    is_reduce_position = is_reduce_position,
                )
    return ret
