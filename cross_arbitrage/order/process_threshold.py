import logging
from decimal import Decimal

import orjson
import redis

from cross_arbitrage.fetch.fetch_funding_rate import get_funding_rate_key
from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.threshold import SymbolConfig, ThresholdConfig
from cross_arbitrage.utils.context import CancelContext, sleep_with_context


def get_threshold_key(exchange):
    return f"order:thresholds:{exchange}"


def process_threshold_mainloop(ctx: CancelContext, config: OrderConfig):
    # init redis client
    rc = redis.Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )
    while not ctx.is_canceled():
        for symbol_info in config.cross_arbitrage_symbol_datas:
            ex_name = symbol_info.makeonly_exchange_name
            try:
                res = SymbolConfig(
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
                funding_info_raw = rc.get(
                    get_funding_rate_key(ex_name, symbol_info.symbol_name)
                )
                if funding_info_raw:
                    funding_info = orjson.loads(funding_info_raw)
                    if funding_info["delta"]:
                        delta = Decimal(funding_info["delta"])
                        if delta > 0:
                            long = res.long_threshold
                            long.increase_position_threshold = (
                                long.increase_position_threshold - delta
                            )
                            long.cancel_increase_position_threshold = (
                                long.decrease_position_threshold
                                + (
                                    long.increase_position_threshold
                                    - long.decrease_position_threshold
                                )
                                * Decimal(
                                    config.default_increase_position_threshold
                                )
                            )
                            long.cancel_decrease_position_threshold = (
                                long.decrease_position_threshold
                                + (
                                    long.increase_position_threshold
                                    - long.decrease_position_threshold
                                )
                                * Decimal(
                                    config.default_decrease_position_threshold
                                )
                            )
                        elif delta < 0:
                            short = res.short_threshold
                            short.increase_position_threshold = (
                                short.increase_position_threshold - delta
                            )
                            short.cancel_increase_position_threshold = (
                                short.decrease_position_threshold
                                + (
                                    short.increase_position_threshold
                                    - short.decrease_position_threshold
                                )
                                * Decimal(
                                    config.default_increase_position_threshold
                                )
                            )
                            short.cancel_decrease_position_threshold = (
                                short.decrease_position_threshold
                                + (
                                    short.increase_position_threshold
                                    - short.decrease_position_threshold
                                )
                                * Decimal(
                                    config.default_decrease_position_threshold
                                )
                            )

                rc.hset(
                    get_threshold_key(ex_name),
                    symbol_info.symbol_name,
                    res.json_bytes(),
                )

            except Exception as ex:
                logging.exception(ex)

        sleep_with_context(ctx, 2 * 60, interval=1.0)
