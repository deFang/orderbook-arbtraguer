from decimal import Decimal
import logging
from threading import Thread
import threading
import time
from typing import Dict, List

import ccxt
from cross_arbitrage.order.globals import get_order_status_stream_is_ready
from cross_arbitrage.order.order_status import start_order_status_stream_mainloop
from cross_arbitrage.order.position_status import align_position_loop, refresh_position_loop
import redis

from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.exchange import create_exchange
from cross_arbitrage.utils.order import get_order_qty, order_mode_is_maintain, order_mode_is_pending, order_mode_is_reduce_only
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol
from .config import OrderConfig
from .order_book import fetch_orderbooks_from_redis, get_signal_from_orderbooks
from .signal_dealer import deal_loop
from .check_exchange_status import check_exchange_status_loop


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
    clear_redis_status(ctx, rc)
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

    # start main loop
    order_loop(ctx, config, exchanges, rc)

    # clear orders when exit
    clear_orders(ctx, symbols, exchanges)


def order_loop(ctx: CancelContext, config: OrderConfig, exchanges: Dict[str, ccxt.Exchange], rc: redis.Redis):
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
            rc, exchanges, config, list(map(lambda x: x[1], orderbooks)))
        if not signals:
            continue

        if not get_order_status_stream_is_ready():
            continue

        for symbol, signal in signals.items():
            if config.debug:
                if ob_count == 0:
                    st = time.time()
                ob_count += 1
                if ob_count >= 1000:
                    logging.info(
                        f"==> fetch orderbook count: {ob_count}, time: {time.time() - st:.3f}s")
                    ob_count = 0

            if not rc.sismember('order:signal:processing', symbol):
                logging.info(f"==> signal: {signal}")
                # order mode is pending
                if order_mode_is_pending(ctx):
                    logging.info(f"order mode is pending, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}")
                    continue

                if order_mode_is_maintain(ctx):
                    logging.info(f'order mode is maintain, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}')
                    continue

                # order mode is reduce only, ignore open orders
                if order_mode_is_reduce_only(ctx) and (not signal.is_reduce_position):
                    logging.info(f"order mode is reduce only, ignore signal {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price} {signal.is_reduce_position}")
                    continue

                # is margin rate is not satisfied, ignore spawn thread
                order_qty = get_order_qty(signal, rc, config)
                if order_qty == Decimal(0):
                    # logging.info(f"order_qty is 0, skip place order: {signal}")
                    if config.debug:
                        logging.info(f"order_qty is 0, skip place order for {signal.symbol} {signal.maker_exchange} {signal.maker_side} {signal.maker_price}")
                    continue

                # add symbol to processing
                rc.sadd('order:signal:processing', symbol)

                # TODO: start process thread
                thread = Thread(target=deal_loop, args=(
                    ctx, config, signal, exchanges, rc), daemon=True)
                thread.start()


def clear_redis_status(ctx: CancelContext, rc: redis.Redis):
    rc.delete('order:signal:processing')


def clear_orders(ctx: CancelContext, symbols: List[str], exchanges: Dict[str, ccxt.Exchange]):
    for exchange_name, exchange in exchanges.items():
        logging.info(f"==> cancel orders on {exchange_name}")
        for symbol in symbols:
            ccxt_symbol = get_ccxt_symbol(symbol)
            try:
                match exchange:
                    case ccxt.okex():
                        orders = exchange.fetch_open_orders(symbol=ccxt_symbol)
                        if orders:
                            # TODO: fix cancel orders
                            exchange.cancel_orders(orders)
                            logging.info(
                                f"===> canceled orders on {exchange_name} for {symbol}")
                    case _:
                        exchange.cancel_all_orders(symbol=ccxt_symbol)
                        logging.info(
                            f"===> canceled orders on {exchange_name} for {symbol}")
            except Exception as e:
                logging.error(f"cancel orders on {exchange_name} failed: {e}")


def set_leverage(ctx: CancelContext, exchanges: Dict[str, ccxt.Exchange], symbols: List[str], leverage: int):
    for exchange_name, exchange in exchanges.items():
        exchange: ccxt.binanceusdm | ccxt.okex
        try:
            for symbol in symbols:
                ccxt_symbol = get_ccxt_symbol(symbol)
                exchange.set_margin_mode(marginMode='cross', symbol=ccxt_symbol)
        except Exception as e:
            logging.error(f"set margin mode on {exchange_name} failed: {e}")

        try:
            for symbol in symbols:
                ccxt_symbol = get_ccxt_symbol(symbol)
                exchange.set_leverage(leverage=leverage, symbol=ccxt_symbol)
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
