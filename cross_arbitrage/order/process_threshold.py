from functools import lru_cache
import logging
from decimal import Decimal
from typing import List

import orjson
import redis

from cross_arbitrage.fetch.fetch_funding_rate import get_funding_rate_key
from cross_arbitrage.fetch.utils.common import now_s
from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.position_status import PositionDirection, PositionStatus, get_position_status
from cross_arbitrage.order.threshold import SymbolConfig, ThresholdConfig
from cross_arbitrage.order.config import SymbolConfig as OrderSymbolConfig
from cross_arbitrage.utils.context import CancelContext, sleep_with_context


def get_threshold_key(exchange):
    return f"order:thresholds:{exchange}"

def init_symbol_config(symbol_info: OrderSymbolConfig) -> SymbolConfig:
    return SymbolConfig(
        short_threshold=ThresholdConfig(
            increase_position_threshold=Decimal(
                str(
                    symbol_info.short_threshold_data.increase_position_threshold
                )
            ),
            decrease_position_threshold=Decimal(
                str(
                    symbol_info.short_threshold_data.decrease_position_threshold
                )
            ),
            cancel_increase_position_threshold=Decimal(
                str(
                    symbol_info.short_threshold_data.cancel_increase_position_threshold
                )
            ),
            cancel_decrease_position_threshold=Decimal(
                str(
                    symbol_info.short_threshold_data.cancel_decrease_position_threshold
                )
            ),
        ),
        long_threshold=ThresholdConfig(
            increase_position_threshold=Decimal(
                str(
                    symbol_info.long_threshold_data.increase_position_threshold
                )
            ),
            decrease_position_threshold=Decimal(
                str(
                    symbol_info.long_threshold_data.decrease_position_threshold
                )
            ),
            cancel_increase_position_threshold=Decimal(
                str(
                    symbol_info.long_threshold_data.cancel_increase_position_threshold
                )
            ),
            cancel_decrease_position_threshold=Decimal(
                str(
                    symbol_info.long_threshold_data.cancel_decrease_position_threshold
                )
            ),
        ),
    )

def _get_threshold_by_funding_delta(ex_name:str, symbol:str, threshold: SymbolConfig, funding_delta:Decimal, \
        percent: Decimal, max_threshold:Decimal):
    long = threshold.long_threshold
    short = threshold.short_threshold
    if funding_delta > 0:
        old_threshold = long.decrease_position_threshold
        long.decrease_position_threshold = max(long.decrease_position_threshold - funding_delta * percent, -max_threshold)
        long.cancel_decrease_position_threshold += (long.decrease_position_threshold - old_threshold)
        long.increase_position_threshold += (long.decrease_position_threshold - old_threshold)
        long.cancel_increase_position_threshold += (long.decrease_position_threshold - old_threshold)
        logging.info(f"{ex_name} {symbol} {percent} funding_delta={funding_delta} long_threshold={long}")
    else:
        old_threshold = short.decrease_position_threshold
        short.decrease_position_threshold = min(short.decrease_position_threshold - funding_delta * percent, max_threshold)
        short.cancel_decrease_position_threshold += (short.decrease_position_threshold - old_threshold)
        short.increase_position_threshold += (short.decrease_position_threshold - old_threshold)
        short.cancel_increase_position_threshold += (short.decrease_position_threshold - old_threshold)
        logging.info(f"{ex_name} {symbol} {percent} funding_delta={funding_delta} short_threshold={short}")
    return threshold

def process_funding_rate(threshold: SymbolConfig, symbol_info: OrderSymbolConfig, exchange_names: List[str], rc: redis.Redis) -> SymbolConfig:
    try:
        now = now_s()
        funding_interval = 8 * 60 * 60  # 8 hours
        max_threshold = Decimal(str(0.001))

        # ingore if not in last 3 hours of a funding interval
        if (now % funding_interval) / (60 * 60) <= 5.0:
            return threshold
        maker_exchange_name, taker_exchange_name = exchange_names

        # maker_position = get_position_status(rc, maker_exchange_name, symbol_info.symbol_name)
        # # return if not has position
        # if not maker_position or maker_position.qty == Decimal(0):
        #     return threshold

        res = []
        with rc.pipeline() as pipe:
            pipe.multi()
            pipe.get(get_funding_rate_key(maker_exchange_name, symbol_info.symbol_name))
            pipe.get(get_funding_rate_key(taker_exchange_name, symbol_info.symbol_name))
            res = pipe.execute()

        # return if any funding rate is None
        if not all(res):
            return threshold
        maker_funding_info = orjson.loads(res[0])
        taker_funding_info = orjson.loads(res[1])

        funding_delta = Decimal(maker_funding_info['funding_rate']) - Decimal(taker_funding_info['funding_rate'])

        if (now % funding_interval) / (60 * 60) <= 6.0:
            threshold = _get_threshold_by_funding_delta(maker_exchange_name, symbol_info.symbol_name, threshold, funding_delta, Decimal('0.33'), max_threshold)
        elif (now % funding_interval) / (60 * 60) <= 7:
            threshold = _get_threshold_by_funding_delta(maker_exchange_name, symbol_info.symbol_name, threshold, funding_delta, Decimal('0.67'), max_threshold)
        elif (now % funding_interval) / (60 * 60) <= 7.933: # 56 minutes
            threshold = _get_threshold_by_funding_delta(maker_exchange_name, symbol_info.symbol_name, threshold, funding_delta, Decimal('1'), max_threshold)
    except Exception as ex:
        logging.error(f"process_funding_rate error: {ex}")
        logging.exception(ex)
    return threshold

def process_orderbook_stat(threshold: SymbolConfig, symbol_info: OrderSymbolConfig, rc: redis.Redis) -> SymbolConfig:
    # TODO
    return threshold

def process_funding_rate_binance_okex_pair(ctx: CancelContext, threshold: SymbolConfig, config: OrderConfig, symbol_info: OrderSymbolConfig, rc: redis.Redis) -> SymbolConfig:
    ex_name = symbol_info.makeonly_exchange_name
    if ex_name:
        try:
            funding_info_raw = rc.get(
                get_funding_rate_key('okex', symbol_info.symbol_name)
            )
            if funding_info_raw:
                funding_info = orjson.loads(funding_info_raw)
                if funding_info["delta"]:
                    delta = Decimal(funding_info["delta"])
                    long = threshold.long_threshold
                    short = threshold.short_threshold
                    if ex_name == 'okex':
                        if delta > 0:
                            long.increase_position_threshold -= delta
                            long.cancel_increase_position_threshold -= delta
                        elif delta < 0:
                            short.increase_position_threshold -= delta
                            short.cancel_increase_position_threshold -= delta
                    else:
                        if delta > 0:
                            short.increase_position_threshold += delta
                            short.cancel_increase_position_threshold += delta
                        elif delta < 0:
                            long.increase_position_threshold += delta
                            long.cancel_increase_position_threshold += delta

                    logging.info(f"{symbol_info.makeonly_exchange_name} {symbol_info.symbol_name} funding_delta={funding_info['delta']} threshold={threshold}")
            return threshold
        except Exception as ex:
            logging.error(f"process_funding_rate_binance_okex_pair error: {ex}")
            logging.exception(ex)
            return threshold

def process_threshold_mainloop(ctx: CancelContext, config: OrderConfig):
    # init redis client
    rc = redis.Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )

    @lru_cache(2)
    def get_taker_exchange_name(maker_exchange_name):
        return (set(config.exchanges) - {maker_exchange_name}).pop()

    while not ctx.is_canceled():
        for symbol_info in config.cross_arbitrage_symbol_datas:
            # init
            threshold = init_symbol_config(symbol_info)

            # okex binance pair
            maker_exchange_name = symbol_info.makeonly_exchange_name
            taker_exchange_name = get_taker_exchange_name(maker_exchange_name)
            match (maker_exchange_name, taker_exchange_name):
                case ('okex', 'binance') | ('binance', 'okex'):
                    threshold = process_funding_rate_binance_okex_pair(
                        ctx, threshold, config, symbol_info, rc)
                case _:
                    raise ValueError(
                        f"unknown exchange pair: {maker_exchange_name}, {taker_exchange_name}")

            # funding rate
            threshold = process_funding_rate(threshold, symbol_info, [maker_exchange_name, taker_exchange_name], rc)

            # orderbook stat
            threshold = process_orderbook_stat(threshold, symbol_info, rc)

            if threshold is not None:
                try:
                    rc.hset(
                        get_threshold_key(maker_exchange_name),
                        symbol_info.symbol_name,
                        threshold.json_bytes(),)
                except Exception as ex:
                    logging.error(f"save threshold error: {ex}, {threshold=}, {maker_exchange_name=}")
                    logging.exception(ex)

        sleep_with_context(ctx, 2 * 60, interval=1.0)
