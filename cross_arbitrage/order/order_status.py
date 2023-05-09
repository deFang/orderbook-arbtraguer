import logging
import queue
import threading
import time

import orjson as json
from redis import Redis
from cross_arbitrage.exchange.binance_usdm_ws import BinanceUsdsPublicWebSocketClient

from cross_arbitrage.exchange.okex_ws import OkexPublicWebSocketClient
from cross_arbitrage.fetch.utils.common import now_s
from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.model import OrderStatus, normalize_binance_ws_order, normalize_okex_order
from cross_arbitrage.utils.color import color
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.order import get_order_status_key
from cross_arbitrage.utils.symbol_mapping import symbol_mapping
from cross_arbitrage.order.globals import set_order_status_stream_is_ready


def start_okex_ws_task(cancel_ctx, symbols, task_queue, config: OrderConfig):
    global ws
    try:
        ws = OkexPublicWebSocketClient(
            context_args={
                "ctx": cancel_ctx,
                "is_private": True,
                "task_queue": task_queue,
                "ping_interval": 15,
                "ping_timeout": 8,
                "http_proxy": config.network.http_proxy,
                "public_key": config.exchanges["okex"].api_key,
                "private_key": config.exchanges["okex"].secret,
                "password": config.exchanges["okex"].password,
            },
        )
        start_exchange_wsclient(ws, "okex")

    except Exception as ex:
        logging.error(ex)
        return

    while True:
        if cancel_ctx.is_canceled():
            ws.stop_client()
            set_order_status_stream_is_ready({'okex': False})
            break

        # check last_rev_timestamp and restart ws client
        # if now_s() - ws.last_rev_timestamp > 32:
        if ws.client_ws and ws.get_status() in ["DISCONNECTED"]:
            ws.stop_client()
            set_order_status_stream_is_ready({'okex': False})
            time.sleep(2)
            start_exchange_wsclient(ws, "okex")
        time.sleep(5)

def start_binance_ws_task(cancel_ctx, symbols, task_queue, config: OrderConfig):
    global ws
    try:
        ws = BinanceUsdsPublicWebSocketClient(
            context_args={
                "ctx": cancel_ctx,
                "is_private": True,
                "task_queue": task_queue,
                "ping_interval": 30,
                "ping_timeout": 10,
                "http_proxy": config.network.http_proxy,
                "public_key": config.exchanges["binance"].api_key,
                "private_key": config.exchanges["binance"].secret,
            },
        )
        start_exchange_wsclient(ws, "binance")

    except Exception as ex:
        logging.error(ex)
        return

    while True:
        if cancel_ctx.is_canceled():
            ws.stop_client()
            ws.remove_listen_key()
            set_order_status_stream_is_ready({'binance': False})
            break

        # check last_rev_timestamp and restart ws client
        # if now_s() - ws.last_rev_timestamp > 62:
        if ws.client_ws and ws.get_status() in ["DISCONNECTED"]:
                ws.stop_client()
                set_order_status_stream_is_ready({'binance': False})
                time.sleep(2)
                start_exchange_wsclient(ws, "binance")
        time.sleep(5)


def start_exchange_wsclient(ws, ex_name):
    if ex_name == "binance":
        retries = 20

        while ws.listen_key is None and retries > 0:
            ws.start_refresh_listen_key()
            time.sleep(5)
            retries -= 1

        if ws.listen_key:
            ws.start_client()
        else:
            raise Exception(f"start_exchange_wsclient Error: failed with max retry with {ex_name}")
    else:
        ws.start_client()

    retries = 30
    while retries > 0:
        if ws.get_status() == "CONNECTED":
            break

        time.sleep(1)
        retries -= 1
    else:
        raise Exception(
            f"{ex_name} websocket client connected failed in {retries} seconds"
        )

    ws.login()
    time.sleep(3)
    ws.watch_user_order()
    set_order_status_stream_is_ready({ex_name: True})


def process_okex_taskqueue_task(
    cancel_ctx: CancelContext, task_queue: queue.Queue, config: OrderConfig
):
    rc = Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )
    while True:
        if cancel_ctx.is_canceled():
            break

        try:
            data = task_queue.get(block=True, timeout=1)
            parsed_data = json.loads(data)
            if parsed_data.get("event"):
                print(f"-- ws event: {data}")
            else:
                orders = parsed_data.get("data")
                if orders and len(orders) > 0:
                    for o in orders:
                        order = normalize_okex_order(o)
                        key = get_order_status_key(order.id, order.exchange)
                        # logging.info(f"-- order status: {order.json()}")
                        status_color = 'blue'
                        if order.status == OrderStatus.canceled:
                            status_color = 'yellow'
                        elif order.status == OrderStatus.filled:
                            status_color = 'green'
                        logging.info(f"-- order status: {order.exchange} id={order.id} {order.symbol}  {order.type} {order.side} {order.price} {order.amount} filled={order.filled} {color(status_color, order.status)} ")
                        rc.rpush(key, order.json())
        except queue.Empty:
            pass


def process_binance_taskqueue_task(
    cancel_ctx: CancelContext, task_queue: queue.Queue, config: OrderConfig
):
    rc = Redis.from_url(
        config.redis.url, encoding="utf-8", decode_responses=True
    )
    while True:
        if cancel_ctx.is_canceled():
            break
        try:
            data = task_queue.get(block=True, timeout=1)
            parsed_data = json.loads(data)
            if parsed_data:
                logging.info(f"-- parsed_data: {parsed_data}")
                if parsed_data.get('e', None) == "ORDER_TRADE_UPDATE":
                    order = normalize_binance_ws_order(parsed_data)
                    key = get_order_status_key(order.id, order.exchange)
                    status_color = 'blue'
                    if order.status == OrderStatus.canceled:
                        status_color = 'yellow'
                    elif order.status == OrderStatus.filled:
                        status_color = 'green'
                    logging.info(f"-- order status: {order.exchange} id={order.id} {order.symbol}  {order.type} {order.side} {order.price} {order.amount} filled={order.filled} {color(status_color, order.status)} ")
                    rc.rpush(key, order.json())
        except queue.Empty:
            pass


def start_order_status_stream_mainloop(
    cancel_ctx: CancelContext,
    config: OrderConfig,
):
    symbols = [s.symbol_name for s in config.cross_arbitrage_symbol_datas]
    config_symbols = {
        k: v for k, v in symbol_mapping.items() if k in symbols
    }

    thread_objects = []
    task_queue = queue.Queue(maxsize=0)

    thread_objects.append(
        threading.Thread(
            target=start_okex_ws_task,
            args=(cancel_ctx, config_symbols, task_queue, config),
            name="fetch_okex_order_status_stream_thread",
            daemon=True,
        )
    )

    thread_objects.append(
        threading.Thread(
            target=process_okex_taskqueue_task,
            args=(cancel_ctx, task_queue, config),
            name="process_okex_order_status_stream_thread",
            daemon=True,
        )
    )

    for thread_object in thread_objects:
        thread_object.start()

    while True:
        if cancel_ctx.is_canceled():
            for thread_object in thread_objects:
                thread_object.join()
            break

        time.sleep(5)
