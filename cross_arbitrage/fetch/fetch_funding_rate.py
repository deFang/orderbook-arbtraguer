import json
import logging
import time
from decimal import Decimal

import redis

from cross_arbitrage.fetch.config import FetchConfig
from cross_arbitrage.fetch.utils.common import now_s
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.exchange import create_exchange
from cross_arbitrage.utils.order import get_last_funding_rate
from cross_arbitrage.utils.symbol_mapping import (get_ccxt_symbol,
                                                  get_common_symbol_from_ccxt, get_exchange_symbol)


def get_funding_rate_key(ex_name: str, symbol: str):
    return f"funding_rate:{ex_name}:{symbol}"


def fetch_funding_rate_mainloop(config: FetchConfig, ctx: CancelContext):
    # init redis client
    rc = redis.Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )

    # init ccxt exchange instance
    exchanges = {}
    for ex_name in config.exchanges.keys():
        exchanges[ex_name] = create_exchange(config.exchanges[ex_name])

    # ccxt symbols
    # ccxt_symbols = [
    #     get_ccxt_symbol(symbol)
    #     for symbol in config.cross_arbitrage_symbol_datas
    # ]

    while True:
        start_at = now_s()

        if ctx.is_canceled():
            break

        for symbol in config.cross_arbitrage_symbol_datas:
            if ctx.is_canceled():
                break
            
            for ex_name in exchanges.keys():
                try:
                    res = {
                        "exchange": ex_name,
                        "symbol": symbol,
                        "delta": None,
                    }
                    exchange_symbol = get_exchange_symbol(symbol, ex_name)
                    funding_info = exchanges[ex_name].fetch_funding_rate(
                        symbol=exchange_symbol.name
                    )
                    res["funding_rate"] = str(
                        Decimal(str(funding_info["fundingRate"]))
                    )
                    res["funding_timestamp"] = funding_info["fundingTimestamp"]

                    previous_funding_info_raw = rc.get(
                        get_funding_rate_key(
                            ex_name, symbol
                        )
                    )
                    previous_funding_info = None
                    if previous_funding_info_raw:
                        previous_funding_info = json.loads(
                            previous_funding_info_raw
                        )
                    else:
                        previous_funding_info = get_last_funding_rate(
                            ex_name, symbol, config
                        )
                    if previous_funding_info:
                        tol = 1000
                        if (
                            previous_funding_info["funding_timestamp"]
                            + 60 * 60 * 8 * 1000
                            + tol
                            >= res["funding_timestamp"]
                            and previous_funding_info["funding_timestamp"]
                            + 60 * 60 * 8 * 1000
                            - tol
                            <= res["funding_timestamp"]
                        ):
                            res["delta"] = str(
                                Decimal(res["funding_rate"])
                                - Decimal(
                                    previous_funding_info["funding_rate"]
                                )
                            )
                        elif (
                            previous_funding_info["funding_timestamp"]
                            == res["funding_timestamp"]
                        ):
                            res["delta"] = previous_funding_info["delta"]
                    # print
                    if previous_funding_info and res['funding_timestamp'] == previous_funding_info['funding_timestamp']:
                        logging.info(
                            f"{ex_name} {symbol} funding_info={res}"
                        )
                    else:
                        logging.info(
                            f"{ex_name} {symbol} funding_info={res} previous_funding_info={previous_funding_info}"
                        )
                    rc.set(
                        get_funding_rate_key(
                            ex_name, symbol
                        ),
                        json.dumps(res),
                    )

                except Exception as ex:
                    logging.exception(ex)

            sleep_with_context(ctx, 3)

        sleep_with_context(ctx, seconds=6 * 60 - now_s() + start_at)
