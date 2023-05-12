from decimal import Decimal
import logging
from threading import Thread
import threading
import time
from typing import Dict, List


import ccxt
from cross_arbitrage.order.globals import get_order_status_stream_is_ready
from cross_arbitrage.order.order_status import start_order_status_stream_mainloop
from cross_arbitrage.order.position_status import PositionDirection, align_position_loop, refresh_position_loop
import redis
from cross_arbitrage.order.process_threshold import process_threshold_mainloop

from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.exchange import create_exchange
from cross_arbitrage.utils.order import get_order_qty, order_mode_is_maintain, order_mode_is_pending, order_mode_is_reduce_only
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_exchange_symbol_from_exchange
from .config import OrderConfig
from .order_book import fetch_orderbooks_from_redis, get_signal_from_orderbooks
from .signal_dealer import deal_loop
from .check_exchange_status import check_exchange_status_loop
from .threshold import Threshold

from cross_arbitrage.utils.ccxt_patch import patch
patch()


def start_loop(ctx: CancelContext, config: OrderConfig):
    # preprocess exchanges
    exchanges = {
        exchange_name: create_exchange(
            params=account_config, proxy=config.network.proxies())
        for exchange_name, account_config in config.exchanges.items()
    }
    for exchange_name, exchange in exchanges.items():
        logging.info(f"==> init exchange {exchange_name}")
        exchange.load_markets()

    rc = redis.Redis.from_url(config.redis.url)
    symbols = [s.symbol_name for s in config.cross_arbitrage_symbol_datas]
    clear_orders(ctx, symbols, exchanges)
    set_leverage(ctx, exchanges, symbols, config.symbol_leverage)
    clear_redis_status(ctx, rc, config)
    refresh_account_balance(ctx, exchanges, rc)

    refresh_account_balance_thread = threading.Thread(
        target=refresh_account_balance_loop,
        args=(ctx, exchanges, rc),
        name="refresh_account_balance_loop_thread",
        daemon=True,
    )
    refresh_account_balance_thread.start()

    order_status_thread = threading.Thread(
        target=start_order_status_stream_mainloop,
        args=(ctx, config),
        name="order_status_stream_mainloop_thread",
        daemon=True,
    )
    order_status_thread.start()

    position_status_thread = threading.Thread(
        target=refresh_position_loop,
        args=(ctx, rc, exchanges, symbols),
        name="refresh_position_mainloop_thread",
        daemon=True,
    )
    position_status_thread.start()

    align_position_thread = threading.Thread(
        target=align_position_loop,
        args=(ctx, rc, exchanges, symbols, config),
        name="align_position_mainloop_thread",
        daemon=True,
    )
    align_position_thread.start()

    check_exchange_status_thread = threading.Thread(
        target=check_exchange_status_loop,
        args=(ctx, config, exchanges),
        name="check_exchange_status_loop_thread",
        daemon=True,
    )
    check_exchange_status_thread.start()

    process_threshold_thread = threading.Thread(
        target=process_threshold_mainloop,
        args=(ctx, config),
        name="process_threshold_thread_loop_thread",
        daemon=True,
    )
    process_threshold_thread.start()

    thresholds: dict[str, Threshold] = {}
    okex_threshold = Threshold(config, rc, makeonly_exchange="okex")
    thresholds["okex"] = okex_threshold
    binance_threshold = Threshold(config, rc, makeonly_exchange="binance")
    thresholds["binance"] = binance_threshold

    for threshold in thresholds.values():
        t = threading.Thread(
            target=threshold.refresh_loop,
            args=(ctx, 5),
            name=f"{threshold.makeonly_exchange}_threshold_refresh_loop_thread",
            daemon=True,
        )
        t.start()

    # start main loop
    order_loop(ctx, config, thresholds, exchanges, rc)

    # clear orders when exit
    clear_orders(ctx, symbols, exchanges)


def order_loop(ctx: CancelContext, config: OrderConfig, thresholds: dict[str, Threshold],
               exchanges: Dict[str, ccxt.Exchange], rc: redis.Redis):
    last_id = '$'
    ob_count = 0
    st = 0
    while not ctx.is_canceled():
        orderbooks = fetch_orderbooks_from_redis(
            ctx, rc, config.redis.orderbook_stream, last_id, 1000, 100)
        if not orderbooks:
            continue
        last_id = orderbooks[-1][0]
        signals = get_signal_from_orderbooks(
            rc, exchanges, config, thresholds, list(map(lambda x: x[1], orderbooks)))
        if not signals:
            continue

        if not get_order_status_stream_is_ready():
            continue

        for (symbol, maker_exchange), signal in signals.items():
            if config.debug:
                if ob_count == 0:
                    st = time.time()
                ob_count += 1
                if ob_count >= 1000:
                    logging.info(
                        f"==> fetch orderbook count: {ob_count}, time: {time.time() - st:.3f}s")
                    ob_count = 0

            lock_key = f'{signal.maker_exchange}:{signal.symbol}'
            if not rc.sismember('order:signal:processing', lock_key):
                logging.info(f"==> signal: {signal}")
                # order mode is pending
                if order_mode_is_pending(ctx):
                    logging.info(
                        f"order mode is pending, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}")
                    continue

                if order_mode_is_maintain(ctx):
                    logging.info(
                        f'order mode is maintain, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}')
                    continue

                # order mode is reduce only, ignore open orders
                if order_mode_is_reduce_only(ctx) and (not signal.is_reduce_position):
                    logging.info(
                        f"order mode is reduce only, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}")
                    continue

                # is margin rate is not satisfied, ignore spawn thread
                order_qty = get_order_qty(signal, rc, config)
                if order_qty == Decimal(0):
                    # logging.info(f"order_qty is 0, skip place order: {signal}")
                    if config.debug:
                        logging.info(
                            f"order_qty is 0, skip place order for {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price}")
                    continue

                # check position notional value
                pos = signal.maker_position
                if (pos and pos.avg_price and
                        signal.maker_side == pos.direction.buy_or_sell()):
                    symbol_config = config.get_symbol_data_by_makeonly(
                        signal.symbol, signal.maker_exchange)
                    notional = pos.avg_price * pos.qty
                    if notional > Decimal(symbol_config.max_notional_per_symbol):
                        logging.info('[maker_exchange: {}] [{}] position notional value {} is greater than max_notional_per_symbol {}'.format(
                            signal.maker_exchange, signal.symbol, pos.avg_price * pos.qty, symbol_config.max_notional_per_symbol))
                        continue

                # add symbol to processing
                rc.sadd('order:signal:processing', lock_key)

                # TODO: start process thread
                thread = Thread(target=deal_loop, args=(
                    ctx, config, signal, exchanges, rc), daemon=True)
                thread.start()


def clear_redis_status(ctx: CancelContext, rc: redis.Redis, config: OrderConfig):
    # remove processing lock
    rc.delete('order:signal:processing')

    # remove thresholds
    exchange_names = config.exchange_pair_names
    for exchange_name in exchange_names:
        rc.delete(f'order:thresholds:{exchange_name}')


def clear_orders(ctx: CancelContext, symbols: List[str], exchanges: Dict[str, ccxt.Exchange]):
    for exchange_name, exchange in exchanges.items():
        logging.info(f"==> cancel orders on {exchange_name}")
        for symbol in symbols:
            exchange_symbol_name = get_exchange_symbol_from_exchange(
                exchange, symbol).name
            try:
                match exchange:
                    case ccxt.okex():
                        orders = exchange.fetch_open_orders(
                            symbol=exchange_symbol_name)
                        if orders:
                            # TODO: fix cancel orders
                            exchange.cancel_orders(orders)
                            logging.info(
                                f"===> canceled orders on {exchange_name} for {symbol}")
                    case _:
                        exchange.cancel_all_orders(symbol=exchange_symbol_name)
                        logging.info(
                            f"===> canceled orders on {exchange_name} for {symbol}")
            except Exception as e:
                logging.error(f"cancel orders on {exchange_name} failed: {e}")


def set_leverage(ctx: CancelContext, exchanges: Dict[str, ccxt.Exchange], symbols: List[str], leverage: int):
    for exchange_name, exchange in exchanges.items():
        exchange: ccxt.binanceusdm | ccxt.okex
        try:
            for symbol in symbols:
                exchange_symbol_name = get_exchange_symbol_from_exchange(
                    exchange, symbol).name
                exchange.set_margin_mode(
                    marginMode='cross', symbol=exchange_symbol_name)
        except Exception as e:
            logging.error(f"set margin mode on {exchange_name} failed: {e}")

        try:
            for symbol in symbols:
                exchange_symbol_name = get_exchange_symbol_from_exchange(
                    exchange, symbol).name
                exchange.set_leverage(
                    leverage=leverage, symbol=exchange_symbol_name)
                logging.info(
                    f"===> set leverage on {exchange_name} for {symbol} to {leverage}")
        except Exception as e:
            logging.error(f"set leverage on {exchange_name} failed: {e}")

        try:
            exchange.set_position_mode(hedged=False)
        except Exception as e:
            logging.error(f"set position mode on {exchange_name} failed: {e}")


def refresh_account_balance(ctx: CancelContext, exchanges: Dict[str, ccxt.Exchange], rc: redis.Redis):
    ret = {}
    for exchange_name, exchange in exchanges.items():
        exchange: ccxt.binanceusdm | ccxt.okex
        margin = {}
        try:
            balance = exchange.fetch_balance()
        except Exception as e:
            logging.error(f"fetch balance on {exchange_name} failed")
            logging.exception(e)
            continue
        margin['used'] = balance['used']['USDT']
        margin['free'] = balance['free']['USDT']
        margin['total'] = balance['total']['USDT']
        ret[exchange_name] = margin
    if ret:
        for exchange_name, margin in ret.items():
            rc.hset(f'margin:{exchange_name}', mapping=margin)
    return ret


def refresh_account_balance_loop(ctx: CancelContext, exchange: Dict[str, ccxt.Exchange], rc: redis.Redis):
    while not ctx.is_canceled():
        refresh_account_balance(ctx, exchange, rc)
        time.sleep(20)
