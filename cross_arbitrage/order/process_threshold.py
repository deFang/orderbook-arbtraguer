from functools import lru_cache
import logging
from decimal import Decimal
from typing import List

import orjson
import pandas as pd
import numpy as np
import redis

from cross_arbitrage.fetch.fetch_funding_rate import get_funding_rate_key
from cross_arbitrage.fetch.utils.common import now_s
from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.position_status import PositionDirection, PositionStatus, get_position_status
from cross_arbitrage.order.threshold import SymbolConfig, ThresholdConfig
from cross_arbitrage.order.config import SymbolConfig as OrderSymbolConfig
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.decorator import retry

_threshold_ready = False

def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


def get_threshold_key(exchange):
    return f"order:thresholds:{exchange}"


def is_threshold_ready():
    return _threshold_ready


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

def _update_threshold_by_funding_delta_to_reduce_position(ex_name:str, threshold: SymbolConfig, config: OrderConfig, symbol_info: OrderSymbolConfig, funding_delta:Decimal, \
        increase_percent: Decimal, decrease_percent: Decimal, max_threshold:Decimal):
    symbol = symbol_info.symbol_name
    long = threshold.long_threshold
    short = threshold.short_threshold
    if funding_delta > 0:
        old_decrease_threshold = long.decrease_position_threshold
        long.decrease_position_threshold = max(long.decrease_position_threshold - funding_delta * decrease_percent, -max_threshold)
        long.cancel_decrease_position_threshold += (long.decrease_position_threshold - old_decrease_threshold)
 
        # tmp threshold is based on json config
        tmp_threshold = Decimal(str(symbol_info.long_threshold_data.increase_position_threshold)) - funding_delta * increase_percent
        if tmp_threshold < long.increase_position_threshold:
            long.increase_position_threshold = tmp_threshold
            long.cancel_increase_position_threshold = Decimal(str(symbol_info.long_threshold_data.cancel_increase_position_threshold)) + \
                    (long.decrease_position_threshold - old_decrease_threshold)
        if config.debug:
            logging.info(f"{ex_name} {symbol} {decrease_percent} funding_delta={funding_delta} long_threshold={long}")
    else:
        old_decrease_threshold = short.decrease_position_threshold
        short.decrease_position_threshold = min(short.decrease_position_threshold - funding_delta * decrease_percent, max_threshold)
        short.cancel_decrease_position_threshold += (short.decrease_position_threshold - old_decrease_threshold)

        # tmp threshold is based on json config
        tmp_threshold = Decimal(str(symbol_info.short_threshold_data.increase_position_threshold)) - funding_delta * increase_percent
        if tmp_threshold > short.increase_position_threshold:
            short.increase_position_threshold = tmp_threshold
            short.cancel_increase_position_threshold = Decimal(str(symbol_info.short_threshold_data.cancel_increase_position_threshold)) + \
                    (short.decrease_position_threshold - old_decrease_threshold)
        if config.debug:
            logging.info(f"{ex_name} {symbol} {decrease_percent} funding_delta={funding_delta} short_threshold={short}")
    return threshold

def _update_threshold_by_funding_delta_to_add_position(ex_name:str, threshold: SymbolConfig, config: OrderConfig, symbol_info: OrderSymbolConfig, funding_delta:Decimal, \
        increase_percent: Decimal, decrease_percent: Decimal, max_threshold:Decimal):
    symbol = symbol_info.symbol_name
    long = threshold.long_threshold
    short = threshold.short_threshold
    if funding_delta > 0:
        old_increase_threshold = short.increase_position_threshold
        old_decrease_threshold = short.decrease_position_threshold
        short.increase_position_threshold = short.increase_position_threshold - funding_delta * increase_percent
        short.cancel_increase_position_threshold += short.increase_position_threshold - old_increase_threshold
        short.decrease_position_threshold = short.decrease_position_threshold - funding_delta * decrease_percent
        short.cancel_decrease_position_threshold += short.decrease_position_threshold - old_decrease_threshold
        if config.debug:
            logging.info(f">{ex_name} {symbol} {increase_percent} funding_delta={funding_delta} short_threshold={short}")
    else:
        old_increase_threshold = long.increase_position_threshold
        old_decrease_threshold = long.decrease_position_threshold
        long.increase_position_threshold = long.increase_position_threshold - funding_delta * increase_percent
        long.cancel_increase_position_threshold += (long.increase_position_threshold - old_increase_threshold)
        long.decrease_position_threshold = long.decrease_position_threshold - funding_delta * decrease_percent
        long.cancel_decrease_position_threshold += (long.decrease_position_threshold - old_decrease_threshold)
        if config.debug:
            logging.info(f">{ex_name} {symbol} {increase_percent} funding_delta={funding_delta} long_threshold={long}")
    return threshold


def process_funding_rate(threshold: SymbolConfig, config: OrderConfig, symbol_info: OrderSymbolConfig, exchange_names: List[str], rc: redis.Redis) -> SymbolConfig:
    try:
        now = now_s()
        funding_interval = 8 * 60 * 60  # 8 hours
        max_threshold = Decimal(str(0.01))

        maker_exchange_name, taker_exchange_name = exchange_names

        # maker_position = get_position_status(rc, maker_exchange_name, symbol_info.symbol_name)
        # # return if not has position
        # if not maker_position or maker_position.qty == Decimal(0):
        #     return threshold

        res = []
        with rc.pipeline() as pipe:
            pipe.multi()
            pipe.get(get_funding_rate_key(
                maker_exchange_name, symbol_info.symbol_name))
            pipe.get(get_funding_rate_key(
                taker_exchange_name, symbol_info.symbol_name))
            res = pipe.execute()

        # return if any funding rate is None
        if not all(res):
            return threshold
        maker_funding_info = orjson.loads(res[0])
        taker_funding_info = orjson.loads(res[1])

        funding_delta = Decimal(
            maker_funding_info['funding_rate']) - Decimal(taker_funding_info['funding_rate'])

        # ingore if not in last 3 hours of a funding interval
        if (now % funding_interval) / (60 * 60) <= 4.0:
            threshold = _update_threshold_by_funding_delta_to_reduce_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('0.25'), max_threshold)
            threshold = _update_threshold_by_funding_delta_to_add_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('0.25'), Decimal('1'), max_threshold)
        elif (now % funding_interval) / (60 * 60) <= 5.0:
            threshold = _update_threshold_by_funding_delta_to_reduce_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('0.5'), max_threshold)
            threshold = _update_threshold_by_funding_delta_to_add_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('0.5'), Decimal('1'), max_threshold)
        elif (now % funding_interval) / (60 * 60) <= 6:
            threshold = _update_threshold_by_funding_delta_to_reduce_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('0.75'), max_threshold)
            threshold = _update_threshold_by_funding_delta_to_add_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('0.75'), Decimal('1'), max_threshold)
        elif (now % funding_interval) / (60 * 60) <= 7.933: # 56 minutes
            threshold = _update_threshold_by_funding_delta_to_reduce_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('1'), max_threshold)
            threshold = _update_threshold_by_funding_delta_to_add_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('1'), max_threshold)
        else:
            threshold = _update_threshold_by_funding_delta_to_reduce_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('1'), Decimal('0'), max_threshold)
            threshold = _update_threshold_by_funding_delta_to_add_position(maker_exchange_name, threshold, config, symbol_info, funding_delta, Decimal('0'), Decimal('1'), max_threshold)
    except Exception as ex:
        logging.error(f"process_funding_rate error: {ex}")
        logging.exception(ex)
    return threshold


def process_orderbook_stat(threshold: SymbolConfig, config: OrderConfig, symbol_info: OrderSymbolConfig, rc: redis.Redis,
                           ob_df: pd.DataFrame, exchange_pair: list[str]) -> SymbolConfig:
    if exchange_pair[0] == symbol_info.makeonly_exchange_name:
        maker_ex = 'ex1'
        taker_ex = 'ex2'
    elif exchange_pair[1] == symbol_info.makeonly_exchange_name:
        maker_ex = 'ex2'
        taker_ex = 'ex1'
    else:
        logging.error('maker exchange {} not in exchange pair {}'.format(
            symbol_info.makeonly_exchange_name, exchange_pair))

    def par_maker(name):
        return f'{maker_ex}_{name}'

    def par_taker(name):
        return f'{taker_ex}_{name}'

    df = ob_df[ob_df['symbol'] == symbol_info.symbol_name].sort_values(
        by='ts', ascending=True)
    if df.empty:
        logging.warning('symbol {} orderbook dataframe is empty'.format(symbol_info.symbol_name))
        return threshold
    
    df['delta_bid'] = df[par_maker('bid')] / df[par_taker('ask')] - 1
    df['delta_ask'] = df[par_maker('ask')] / df[par_taker('bid')] - 1
    df = df.drop(columns=[par_maker('bid'), par_maker('ask'), par_taker('bid'), par_taker('ask')])
    df = df[(df.shift(1)['trigger_exchange'] != df['trigger_exchange']) | (df['ts'] - df.shift(1)['ts'] > 100)]

    bid_mu = df['delta_bid'].mean()
    bid_sig = df['delta_bid'].std()
    ask_mu = df['delta_ask'].mean()
    ask_sig = df['delta_ask'].std()

    long_increase_threshold = Decimal(str(bid_mu - bid_sig * config.dyn_threshold.increase_sigma))
    long_decrease_threshold = Decimal(str(bid_mu - bid_sig * config.dyn_threshold.decrease_sigma))
    short_increase_threshold = Decimal(str(ask_mu + ask_sig * config.dyn_threshold.increase_sigma))
    short_decrease_threshold = Decimal(str(ask_mu + ask_sig * config.dyn_threshold.decrease_sigma))

    try:
        data = {
            'bid_mu': str(bid_mu),
            'bid_sig': str(bid_sig),
            'ask_mu': str(ask_mu),
            'ask_sig': str(ask_sig),
            'long_increase_threshold': long_increase_threshold,
            'long_decrease_threshold': long_decrease_threshold,
            'short_increase_threshold': short_increase_threshold,
            'short_decrease_threshold': short_decrease_threshold,
            'pre': threshold.dict(),
        }
        rc.hset(f'order_threshold_dynamic:{symbol_info.makeonly_exchange_name}', symbol_info.symbol_name, orjson.dumps(data, default=_json_default))
    except Exception as ex:
        logging.error(f"process_orderbook_stat error: {ex}")
        logging.exception(ex)

    if config.debug:
        logging.info(('\n\n[symbol: {}, maker_exchange: {}] bid_mu: {}, bid_sig: {}, ask_mu: {}, ask_sig: {},'
                      'long_increase_threshold: {}, long_decrease_threahold: {}, short_increase_threshold: {}, short_decrease_threahold: {}, pre_threshold: {}').format(
            symbol_info.symbol_name, symbol_info.makeonly_exchange_name,
            bid_mu, bid_sig, ask_mu, ask_sig,
            long_increase_threshold, long_decrease_threshold, short_increase_threshold, short_decrease_threshold,
            threshold))

    if threshold.long_threshold.increase_position_threshold > long_increase_threshold:
        delta = long_increase_threshold - threshold.long_threshold.increase_position_threshold
        threshold.long_threshold.increase_position_threshold = long_increase_threshold
        threshold.long_threshold.cancel_increase_position_threshold += delta
    if threshold.long_threshold.decrease_position_threshold < long_decrease_threshold:
        delta = long_decrease_threshold - threshold.long_threshold.decrease_position_threshold
        threshold.long_threshold.decrease_position_threshold = long_decrease_threshold
        threshold.long_threshold.cancel_decrease_position_threshold += delta
    if threshold.short_threshold.increase_position_threshold < short_increase_threshold:
        delta = short_increase_threshold - threshold.short_threshold.increase_position_threshold
        threshold.short_threshold.increase_position_threshold = short_increase_threshold
        threshold.short_threshold.cancel_increase_position_threshold += delta
    if threshold.short_threshold.decrease_position_threshold > short_decrease_threshold:
        delta = short_decrease_threshold - threshold.short_threshold.decrease_position_threshold
        threshold.short_threshold.decrease_position_threshold = short_decrease_threshold
        threshold.short_threshold.cancel_decrease_position_threshold += delta

    if config.debug:
        logging.info('\n[symbol: {}, maker_exchange: {}] after process: {}'.format(symbol_info.symbol_name, 
                                                                                 symbol_info.makeonly_exchange_name,
                                                                                 threshold))
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

                    if config.debug:
                        logging.info(
                            f"{symbol_info.makeonly_exchange_name} {symbol_info.symbol_name} funding_delta={funding_info['delta']} threshold={threshold}")
            return threshold
        except Exception as ex:
            logging.error(
                f"process_funding_rate_binance_okex_pair error: {ex}")
            logging.exception(ex)
            return threshold


@retry(max_retry_count=3, raise_exception=True, default_return_value=[])
def get_orderbook(ctx: CancelContext, config: OrderConfig, rc: redis.Redis, symbols: List[str] | set[str]) -> list:
    orderbooks = []
    last_id = (now_s() - config.dyn_threshold.time_window_seconds) * 1000

    while not ctx.is_canceled():
        res = rc.xread({config.redis.orderbook_stream: last_id}, count=1000)
        if not res:
            break

        obs = res[0][1]
        if not obs:
            break
        last_id = obs[-1][0]
        for _id, ob in obs:
            for symbol, ob in ob.items():
                if symbol not in symbols:
                    continue
                orderbooks.append(orjson.loads(ob))
    return orderbooks


def get_dataframe_from_orderbook(obs: list, exchanges: list[str]):
    if len(exchanges) != 2:
        raise ValueError("exchanges length must be 2")
    ex1, ex2 = exchanges
    arr = []
    for ob in obs:
        if ob['exchange'] not in [ex1, ex2]:
            continue
        if not ob.get(ex1, None) or not ob.get(ex2, None)\
                or not ob[ex1]['bids'] or not ob[ex1]['asks'] \
                or not ob[ex2]['bids'] or not ob[ex2]['asks']:
            continue
        arr.append(
            (ob['symbol'], np.int64(ob['ts']), ob['exchange'],
             np.float64(ob[ex1]['bids'][0][0]), np.float64(
                 ob[ex1]['asks'][0][0]),
             np.float64(ob[ex2]['bids'][0][0]), np.float64(ob[ex2]['asks'][0][0]),)
        )
    df = pd.DataFrame(arr, columns=['symbol', 'ts', 'trigger_exchange',
                                    'ex1_bid', 'ex1_ask', 'ex2_bid', 'ex2_ask'])
    return df


def process_threshold_mainloop(ctx: CancelContext, config: OrderConfig):
    global _threshold_ready

    # init redis client
    rc = redis.Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )

    @lru_cache(2)
    def get_taker_exchange_name(maker_exchange_name):
        return (set(config.exchanges) - {maker_exchange_name}).pop()

    symbols_set = set(
        symbol_info.symbol_name for symbol_info in config.cross_arbitrage_symbol_datas)
    symbols = list(symbols_set)
    exchanges = list(config.exchange_pair_names)

    while not ctx.is_canceled():
        orderbook = get_orderbook(ctx, config, rc, symbols_set)

        ob_df = get_dataframe_from_orderbook(orderbook, exchanges)
        del orderbook

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
            threshold = process_funding_rate(threshold, config, symbol_info, [
                                             maker_exchange_name, taker_exchange_name], rc)

            # orderbook stat
            threshold = process_orderbook_stat(
                threshold, config, symbol_info, rc, ob_df, exchanges)

            if threshold is not None:
                try:
                    rc.hset(
                        get_threshold_key(maker_exchange_name),
                        symbol_info.symbol_name,
                        threshold.json_bytes(),)
                except Exception as ex:
                    logging.error(
                        f"save threshold error: {ex}, {threshold=}, {maker_exchange_name=}")
                    logging.exception(ex)
        del ob_df
        _threshold_ready = True
        sleep_with_context(ctx, 2 * 60, interval=1.0)
