from functools import lru_cache
import logging
from decimal import Decimal

import orjson
import redis

from cross_arbitrage.fetch.fetch_funding_rate import get_funding_rate_key
from cross_arbitrage.order.config import OrderConfig
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

# use funding rate before open position
def use_funding_rate_pre(makeonly_exchange_name: str, symbol_config: SymbolConfig, rc: redis.Redis, symbol_info: OrderSymbolConfig):
    funding_info_raw = rc.get(
        get_funding_rate_key('okex', symbol_info.symbol_name)
    )
    if funding_info_raw:
        funding_info = orjson.loads(funding_info_raw)
        if funding_info["delta"]:
            delta = Decimal(funding_info["delta"])
            long = symbol_config.long_threshold
            short = symbol_config.short_threshold
            if makeonly_exchange_name == 'okex':
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
    return symbol_config

# use funding rate after open position
def use_funding_rate_post():
    pass

def process_threshold_okex_maker_binance_taker(ctx: CancelContext, config: OrderConfig, rc: redis.Redis, symbol_info: OrderSymbolConfig):
    ex_name = 'okex'
    try:
        # init based on json config
        res = init_symbol_config(symbol_info)

        # add symbol config process here
        res = use_funding_rate_pre(ex_name, res, rc, symbol_info)

        return res
    except Exception as ex:
        logging.error(f"process_threshold_okex_maker error: {ex}")
        logging.exception(ex)
        return None


def process_threshold_binance_maker_okex_taker(ctx: CancelContext, config: OrderConfig, rc: redis.Redis, symbol_info: OrderSymbolConfig):
    ex_name = 'binance'
    try:
        # init based on json config
        res = init_symbol_config(symbol_info)

        # add symbol config process here
        res = use_funding_rate_pre(ex_name, res, rc, symbol_info)

        return res
    except Exception as ex:
        logging.error(f"process_threshold_okex_maker error: {ex}")
        logging.exception(ex)
        return None


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
            # ex_name = symbol_info.makeonly_exchange_name
            maker_exchange_name = symbol_info.makeonly_exchange_name
            taker_exchange_name = get_taker_exchange_name(maker_exchange_name)
            match (maker_exchange_name, taker_exchange_name):
                case 'okex', 'binance':
                    threshold = process_threshold_okex_maker_binance_taker(
                        ctx, config, rc, symbol_info)
                case 'binance', 'okex':
                    threshold = process_threshold_binance_maker_okex_taker(
                        ctx, config, rc, symbol_info)
                case _:
                    raise ValueError(
                        f"unknown exchange pair: {maker_exchange_name}, {taker_exchange_name}")
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
