from decimal import Decimal
import logging
import time
from typing import Dict

import ccxt
from pydantic import BaseModel
import pydantic
from cross_arbitrage.fetch.utils.redis import get_ob_storage_key
from cross_arbitrage.utils.csv import CSVModel
from cross_arbitrage.utils.decorator import retry
from cross_arbitrage.utils.exchange import get_exchange_name
from cross_arbitrage.utils.order import get_order_qty, get_order_status_key
import orjson
import redis
import numpy as np

from cross_arbitrage.fetch.utils.common import now_ms
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_exchange_symbol_from_exchange
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from .config import OrderConfig
from .order_book import OrderSignal
from .market import align_qty, maker_only_order, market_order, cancel_order
from .model import Order as OrderModel, OrderStatus, normalize_common_order


class _Status(BaseModel):
    timestamp: float = pydantic.Field(default_factory=now_ms)
    status: str
    order_id: str | None
    post_qty: Decimal | None
    filled_qty: Decimal | None
    post_price: Decimal | None
    followed_qty: Decimal | None
    processing_seconds: float | None

    @classmethod
    def default(cls, status="na"):
        return cls(status=status)


class OrderDataModel(CSVModel):
    signal: OrderSignal
    status: _Status


def deal_loop(ctx: CancelContext, config: OrderConfig, signal: OrderSignal, exchanges: Dict[str, ccxt.Exchange], rc: redis.Redis):

    symbol = signal.symbol
    maker_exchange: ccxt.okex = exchanges[signal.maker_exchange]
    taker_exchange: ccxt.binance = exchanges[signal.taker_exchange]

    order_price = signal.maker_price
    order_qty = get_order_qty(signal, rc, config)

    if order_qty == 0:
        if config.debug:
            logging.info(f"order_qty is 0, skip place order: {signal}")
        rc.srem('order:signal:processing', symbol)

        stat = OrderDataModel(
            signal=signal, status=_Status.default('no_enough_margin'))
        stat.write(config.output_data.order_loop)
        return

    ts = now_ms()
    maker_client_id = f'crTmkoT{ts}'
    taker_client_id_prefix = f'crTmktT{ts}T'
    taker_client_id_count = 0

    # symbol_config = config.get_symbol_data_by_makeonly(symbol, signal.maker_exchange)

    # if order_mode_is_pending(ctx):
    #     logging.info(f'dry run, skip place order: {signal}')
    #     rc.srem('order:signal:processing', symbol)
    #     return

    retry = 2
    while retry:
        try:
            maker_order = maker_only_order(maker_exchange, symbol,
                                           signal.maker_side, order_qty, order_price,
                                           client_id=maker_client_id)
            break
        except Exception as e:
            retry -= 1

            logging.error(f'place maker order failed: {type(e)}')
            logging.exception(e)

            if not retry:
                rc.srem('order:signal:processing', symbol)

                stat = OrderDataModel(
                    signal=signal, status=_Status.default('maker_order_failed'))
                return

    if maker_order['status'] in ['rejected', 'expired', 'canceled']:
        logging.error(f'maker order rejected: {maker_order}')
        rc.srem('order:signal:processing', symbol)
        return

    maker_order_id = maker_order['id']

    start_time = now_ms()
    maker_filled_qty = Decimal('0')
    followed_qty = Decimal('0')

    taker_exchange_symbol = get_exchange_symbol_from_exchange(taker_exchange, symbol)
    taker_exchange_minimum_qty = Decimal(
        str(taker_exchange.market(taker_exchange_symbol.name)['limits']['amount']['min'])) * taker_exchange_symbol.multiplier
    taker_exchange_bag_size = Decimal(
        str(taker_exchange.market(taker_exchange_symbol.name)['contractSize'])) * taker_exchange_symbol.multiplier

    # set to start exiting tasks
    _clear = False
    # set for finished exiting tasks
    _cleared = False
    mark_clear_time = None
    is_filled = False
    is_canceled_by_program = False

    while not _cleared:
        if ctx.is_canceled() and not _clear:
            _cancel_order(maker_exchange, symbol, maker_order_id)
            _clear = True

        # get maker order filled status
        items = []
        try:
            n = rc.blpop(get_order_status_key(
                maker_order_id, 'okex'), timeout=0.2)
            if n:
                items.append(n[1])
                while True:
                    n = rc.lpop(get_order_status_key(
                        maker_order_id, 'okex'), 10)
                    if n:
                        items.append(*n)
                    else:
                        break
        except redis.RedisError as e:
            logging.warning(f'get a redis error: {type(e)}')
            logging.exception(e)

        is_canceled_or_filled = False
        new_trade = False
        for item in items:
            data = orjson.loads(item)
            event = OrderModel.parse_obj(data)
            if event.status == OrderStatus.canceled:
                is_canceled_or_filled = True
                break

            if event.status in [OrderStatus.filled, OrderStatus.partially_filled]:
                new_trade = True
                maker_filled_qty = Decimal(event.filled)

            if event.status == OrderStatus.filled:
                is_canceled_or_filled = True
                is_filled = True
                break

        # make market order
        if new_trade:
            if config.debug:
                logging.info(
                    f'followed_qty: {followed_qty}, maker_filled_qty: {maker_filled_qty}')
            need_order_qty, _ = align_qty(
                taker_exchange, symbol, maker_filled_qty - followed_qty)
            if config.debug:
                logging.info(
                    f'need_order_qty: {need_order_qty}, taker_exchange_minimum_qty: {taker_exchange_minimum_qty}')
            logging.info(
                f'[{taker_exchange}][{symbol}] new trade, need_order_qty: {need_order_qty}')
            if need_order_qty >= taker_exchange_minimum_qty:
                taker_client_id_count += 1
                taker_client_id = f'{taker_client_id_prefix}{taker_client_id_count}'
                logging.info(
                    f'[{taker_exchange}][{symbol}] place taker order: {need_order_qty}')
                try:
                    order = market_order(taker_exchange, symbol,
                                         signal.taker_side, need_order_qty,
                                         client_id=taker_client_id)
                    followed_qty += Decimal(str(order['amount'])) * \
                        taker_exchange_bag_size
                except Exception as e:
                    logging.error(f'place taker order failed: {type(e)}')
                    logging.exception(e)

        if is_canceled_or_filled or is_canceled_by_program:
            _clear = True

        # clear and exit
        if _clear:
            if (not is_canceled_or_filled) or is_canceled_by_program:
                if mark_clear_time is None:
                    mark_clear_time = time.time()
                    continue
                elif time.time() - mark_clear_time > 10:
                    logging.info(
                        f'order should be canceled, but not signal fetched: {maker_order_id}({maker_client_id})')
                else:
                    time.sleep(0.1)
                    continue
            # check position before exit
            # if not is_canceled_or_filled:
            if (not is_filled):
                order_info = _get_order(maker_exchange, symbol, maker_order_id)
                if order_info:
                    filled_qty = Decimal(order_info.filled)
                    if filled_qty > followed_qty:
                        logging.info(
                            f"order qty is not match: {symbol} maker qty {filled_qty}, taker qty {followed_qty}")
                        new_qty, _ = align_qty(
                            taker_exchange, symbol, filled_qty - followed_qty)
                        if new_qty > taker_exchange_minimum_qty:
                            retry = 3
                            while retry > 0:
                                try:
                                    taker_client_id_count += 1
                                    taker_client_id = f'{taker_client_id_prefix}{taker_client_id_count}Tfix'
                                    order = market_order(taker_exchange, symbol,
                                                         signal.taker_side, new_qty,
                                                         client_id=taker_client_id)
                                    retry = 0
                                except Exception as e:
                                    logging.error(
                                        f'place taker order failed: {type(e)}')
                                    logging.exception(e)
                                    retry -= 1
                    elif filled_qty < followed_qty:
                        logging.warn(
                            f"order qty is not match: {symbol} maker qty {filled_qty}, taker qty {followed_qty}")

            try:
                now = time.time()
                _stat = OrderDataModel(signal=signal,
                                       status=_Status(
                                           status='cleared',
                                           order_id=maker_order_id,
                                           post_qty=order_qty,
                                           filled_qty=maker_filled_qty,
                                           followed_qty=followed_qty,
                                           processing_seconds=now-start_time/1000,
                                       ))
                _stat.write(config.output_data.order_loop)
            except Exception as e:
                logging.error(f'write order loop data failed: {type(e)}')
                logging.exception(e)

            sleep_time = 10
            if mark_clear_time is not None:
                sleep_time = 10 - (time.time() - mark_clear_time)
            sleep_with_context(ctx, sleep_time)
            rc.srem('order:signal:processing', symbol)
            _cleared = True

            return

        # check if is over time
        if now_ms() - start_time > config.default_cancel_position_timeout * 1000:
            if config.debug:
                logging.info('order timeout, cancel order')
            ok = cancel_order_once(maker_exchange, symbol, maker_order_id)
            if ok:
                _clear = True
                is_canceled_by_program = True
                continue

        # check if price delta is less than cancel_order_threshold on taker side
        ob_raw = redis_get(rc,
                           get_ob_storage_key(signal.taker_exchange, symbol))
        if not ob_raw:
            continue
        ob = orjson.loads(ob_raw)

        if should_cancel_makeonly_order(ctx, config, signal, ob, order_qty, taker_exchange_bag_size):
            logging.info(
                f'cancel makeonly order: {maker_order_id}({maker_client_id})')
            ok = cancel_order_once(maker_exchange, symbol, maker_order_id)
            if ok:
                _clear = True
                is_canceled_by_program = True

        # match signal.taker_side:
        #     case 'buy':
        #         ask1_price = Decimal(ob['asks'][0][0])
        #         if order_price / ask1_price < 1 + signal.cancel_order_threshold:
        #             if config.debug:
        #                 logging.info(
        #                     f'maker price / binance ask1 = {order_price / ask1_price}')
        #             ok = cancel_order_once(
        #                 maker_exchange, symbol, maker_order_id)
        #             if ok:
        #                 _clear = True
        #     case 'sell':
        #         bid1_price = Decimal(ob['bids'][0][0])
        #         if order_price / bid1_price > 1 + signal.cancel_order_threshold:
        #             if config.debug:
        #                 logging.info(
        #                     f'maker price / binance bid1 = {order_price / bid1_price}')
        #             ok = cancel_order_once(
        #                 maker_exchange, symbol, maker_order_id)
        #             if ok:
        #                 _clear = True


@retry(2, raise_exception=False)
def redis_get(rc: redis.Redis, key: str):
    return rc.get(key)


@retry(3, raise_exception=False)
def _cancel_order(exchange: ccxt.Exchange, symbol: str, order_id: str):
    cancel_order(exchange, order_id, symbol)


@retry(3, raise_exception=False)
def _get_order(exchange: ccxt.Exchange, symbol: str, order_id: str) -> OrderModel:
    if symbol:
        symbol = get_exchange_symbol_from_exchange(exchange, symbol).name
    ccxt_order = exchange.fetch_order(id=order_id, symbol=symbol)

    res = normalize_common_order(ccxt_order, get_exchange_name(exchange))
    return res


def cancel_order_once(exchange: ccxt.Exchange, symbol: str, order_id: str):
    try:
        cancel_order(exchange, order_id, symbol)
        return True
    except ccxt.errors.OrderNotFound as e:
        msg = str(e)
        # if 'already completed' in msg:
        logging.info(f'cancel order failed: {msg}')
        return True
    except Exception as e:
        logging.error(f'cancel order failed: {type(e)}')
        logging.exception(e)
        return False


def should_cancel_makeonly_order(ctx: CancelContext, config: OrderConfig, signal: OrderSignal,
                                 taker_ob: dict, need_depth_qty: Decimal, bag_size: Decimal):
    bag_size = np.float64(bag_size)

    threshold_line = signal.maker_price / \
        Decimal(str(1 + signal.cancel_order_threshold))

    match signal.taker_side:
        case 'buy':
            ob = np.array(taker_ob['asks'], dtype=np.float64)
            if len(ob) == 0:
                logging.warning('no asks on taker side: {}: {}'.format(
                    signal.taker_exchange, taker_ob))
                return True

            # cancel order threshold if out of order book level
            if ob[-1, 0] < threshold_line:
                return False

            ob[:, 1] *= bag_size
            if ob[ob[:, 0] <= threshold_line, 1].sum() < need_depth_qty:
                if config.debug:
                    logging.info(
                        'depth qty is not enough, signal: {}, ob: {}'.format(signal, ob))
                return True
            return False
        case 'sell':
            ob = np.array(taker_ob['bids'], dtype=np.float64)
            if len(ob) == 0:
                logging.warning('no bids on taker side: {}: {}'.format(
                    signal.taker_exchange, taker_ob))
                return True

            # cancel order threshold if out of order book level
            if ob[-1, 0] > threshold_line:
                return False

            ob[:, 1] *= bag_size
            if ob[ob[:, 0] >= threshold_line, 1].sum() < need_depth_qty:
                if config.debug:
                    logging.info(
                        'depth qty is not enough, signal: {}, ob: {}'.format(signal, ob))
                return True
            return False
