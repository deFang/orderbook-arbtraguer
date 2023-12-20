# import json
import logging
import threading
import time
from typing import List

import orjson as json
import redis

from cross_arbitrage.fetch.config import FetchConfig
from cross_arbitrage.fetch.utils.common import now_ms, ts_to_str
from cross_arbitrage.fetch.utils.redis import get_ob_notify_key, get_ob_storage_key
from cross_arbitrage.utils.context import CancelContext


def agg_orderbooks_from_redis(
    rc: redis.Redis,
    notify_exchange: str,
    symbol: str,
    exchanges: List[str],
    output_stream: str,
    stream_size: int,
    ctx: CancelContext,
):
    origin_ob_keys = {ex: get_ob_storage_key(ex, symbol) for ex in exchanges}
    origin_ob_notify_key = get_ob_notify_key(notify_exchange, symbol)

    while not ctx.is_canceled():
        result = None
        try:
            # with rc.pipeline() as pipe:
            #     pipe.multi()
            #     pipe.brpop(origin_ob_notify_key, timeout=1)
            #     pipe.delete(origin_ob_notify_key)
            #     pipe.mget(origin_ob_keys.values())
            #     result = pipe.execute()

            res = rc.brpop(origin_ob_notify_key, timeout=1)
            if res is None:
                continue
            result = rc.mget(origin_ob_keys.values())
        except redis.exceptions.TimeoutError:
            continue

        if result == None:
            continue
        obs = result
        if len(obs) != len(exchanges) or None in obs:
            continue
        ts = now_ms()
        time_str = ts_to_str(ts / 1000)
        order_book = {
            "symbol": symbol,
            "ts": ts,
            "datetime": time_str,
            "exchange": notify_exchange,
        }
        for exchange, ob in zip(origin_ob_keys.keys(), obs):
            order_book[exchange] = json.loads(ob)
        try:
            rc.xadd(
                output_stream,
                {symbol: json.dumps(order_book)},
                maxlen=stream_size,
                approximate=True,
            )
        except Exception as e:
            logging.error(f"get a redis error: {type(e)}: {e}")
            logging.exception(e)


def _loop(
    redis_url: str,
    symbol: str,
    exchanges: List[str],
    notify_exchange: str,
    output_stream: str,
    stream_size: int,
    ctx: CancelContext,
):
    rc = redis.Redis.from_url(redis_url)

    while not ctx.is_canceled():
        try:
            agg_orderbooks_from_redis(
                rc, notify_exchange, symbol, exchanges, output_stream, stream_size, ctx
            )
        except Exception as e:
            logging.error(f"get a redis error: {type(e)}: {e}")
            logging.exception(e)


def agg_orderbook_mainloop(conf: FetchConfig, ctx: CancelContext):
    redis_url = conf.redis.url
    exchanges = list(conf.exchanges.keys())
    threads = {}
    for symbol in conf.cross_arbitrage_symbol_datas:
        for exchange in exchanges:
            t = threading.Thread(
                target=_loop,
                args=(
                    redis_url,
                    symbol,
                    exchanges,
                    exchange,
                    conf.redis.orderbook_stream,
                    conf.redis.orderbook_stream_size,
                    ctx,
                ),
                name=f"agg_ob_{symbol}_{exchange}_thread",
                daemon=True,
            )
            t.start()
            threads[(symbol, exchange)] = t
    while not ctx.is_canceled():
        time.sleep(10)
        for (symbol, exchange), t in threads.items():
            t: threading.Thread
            if not t.is_alive() and not ctx.is_canceled():
                logging.error(f"thread {symbol} {exchange} died, restarting")
                t = threading.Thread(
                    target=_loop,
                    args=(
                        redis_url,
                        symbol,
                        exchanges,
                        exchange,
                        conf.redis.orderbook_stream,
                        conf.redis.orderbook_stream_size,
                        ctx,
                    ),
                    name=f"agg_ob_{symbol}_{exchange}_thread",
                    daemon=True,
                )
                t.start()
                threads[(symbol, exchange)] = t
