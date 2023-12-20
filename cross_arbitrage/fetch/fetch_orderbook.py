# import json
from decimal import Decimal
import logging
import queue
import sys
import threading
import time
import numpy as np

import orjson as json
import redis

from cross_arbitrage.exchange.binance_usdm_ws import \
    BinanceUsdsPublicWebSocketClient
from cross_arbitrage.exchange.okex_ws import OkexPublicWebSocketClient
from cross_arbitrage.fetch.config import FetchConfig
from cross_arbitrage.fetch.utils.common import now_ms
from cross_arbitrage.fetch.utils.redis import (get_ob_notify_key,
                                               get_ob_storage_key)
from cross_arbitrage.utils.context import CancelContext


def start_exchange_wsclient(ws, ex_name, config_symbols):
    ws.start_client()

    retries = 10
    while retries > 0:
        if ws.get_status() == "CONNECTED":
            break

        time.sleep(1)
        retries += 1
    else:
        raise Exception(
            f"{ex_name} websocket client connected failed in {retries} seconds"
        )

    ws.watch_order_books(
        symbols=[
            v[ex_name]["name"] if isinstance(v[ex_name], dict) else v[ex_name]
            for v in config_symbols.values()
        ]
    )


def start_okex_ws_task(
    cancel_ctx, config_symbols, task_queue, conf: FetchConfig
):
    try:
        ws = OkexPublicWebSocketClient(
            context_args={
                "task_queue": task_queue,
                "ping_interval": 15,
                "ping_timeout": 8,
                "http_proxy": conf.network.http_proxy,
            },
        )
        start_exchange_wsclient(ws, "okex", config_symbols)

    except Exception as ex:
        logging.error(ex)
        return

    while True:
        if cancel_ctx.is_canceled():
            ws.stop_client()
            break

        # check last_rev_timestamp and restart ws client
        # if now_s() - ws.last_rev_timestamp > 32:
        if ws.client_ws and ws.get_status() in ["DISCONNECTED"]:
            ws.stop_client()
            time.sleep(2)
            start_exchange_wsclient(ws, "okex", config_symbols)
        time.sleep(5)


def start_binance_ws_task(
    cancel_ctx, config_symbols, task_queue, conf: FetchConfig
):
    try:
        ws = BinanceUsdsPublicWebSocketClient(
            context_args={
                "task_queue": task_queue,
                "ping_interval": 30,
                "ping_timeout": 10,
                "http_proxy": conf.network.http_proxy,
            },
        )
        start_exchange_wsclient(ws, "binance", config_symbols)

    except Exception as ex:
        logging.error(ex)
        return

    while True:
        if cancel_ctx.is_canceled():
            ws.stop_client()
            break

        if ws.client_ws and ws.get_status() in ["DISCONNECTED"]:
            ws.stop_client()
            time.sleep(2)
            start_exchange_wsclient(ws, "binance", config_symbols)

        time.sleep(5)


def process_okex_ws_task(cancel_ctx, config_symbols, task_queue, conf):
    ex_name = "okex"
    symbol_cache = {
        (v[ex_name]["name"] if isinstance(v[ex_name], dict) else v[ex_name]): k
        for k, v in config_symbols.items()
    }
    symbol_info_cache = {}
    redis_client = redis.Redis.from_url(
        conf.redis.url, encoding="utf-8", decode_responses=True
    )

    count = 0
    while True:
        if cancel_ctx.is_canceled():
            break
        try:
            res = []
            data = None
            if task_queue.qsize() > 0:
                try:
                    for _ in range(task_queue.qsize()):
                        data = task_queue.get_nowait()
                        res.append(data)
                except queue.Empty:
                    pass
            else:
                try:
                    data = task_queue.get(block=True, timeout=5)
                    res.append(data)
                except queue.Empty:
                    pass
            for item_raw in res:
                item = json.loads(item_raw)
                # print(f"-- {ex_name} {item}")
                if item.get("arg") and item.get("data"):
                    d = item["data"][0]
                    symbol = symbol_cache[d["instId"]]
                    if symbol:
                        # if conf.debug:
                        #     now = now_ms()
                        #     logging.info(
                        #         f"-- {ex_name} {symbol_cache[d['instId']]} {now - int(d['ts'])}ms"
                        #     )
                        try:
                            key = get_ob_storage_key(ex_name, symbol)
                            notify_key = get_ob_notify_key(ex_name, symbol)
                            result = {
                                "ex": ex_name,
                                "symbol": symbol,
                                "ts": int(d["ts"]),
                                "bids": [i[:2] for i in d["bids"]],
                                "asks": [i[:2] for i in d["asks"]],
                            }
                            # print(f">> {ex_name} {result}")
                            if (
                                symbol_info_cache.get(symbol)
                                and symbol_info_cache[symbol] == result
                            ):
                                continue
                            symbol_info_cache[symbol] = result
                            redis_client.set(key, json.dumps(result))
                            if redis_client.llen(notify_key) == 0:
                                redis_client.lpush(
                                    notify_key, json.dumps({"updated": True})
                                )
                        except Exception as ex:
                            logging.error(ex)
                    # logging.info(
                    #     f"{d['instId']} ts={d['ts']} duration={round(time.time() - float(int(d['ts'])/1000),3)}s {d['bids']}"
                    # )
        except Exception as ex:
            logging.exception(ex)
        finally:
            count += 1
            if count == sys.maxsize:
                count = 0
            if count % 1000 == 0:
                logging.info(
                    f"--------> okex task queue size: {task_queue.qsize()}"
                )


def normalize_orderbook_5(ob, multiplier):
    # res = np.array(ob).astype(Decimal)
    # multiplier = np.array([1/multiplier,1])
    # return (res * multiplier).astype(str).tolist()
    for row in ob:
        row[0] = str(Decimal(str(row[0]))/Decimal(str(multiplier)))
    return ob


def process_binance_ws_task(cancel_ctx, config_symbols, task_queue, conf):
    ex_name = "binance"
    symbol_cache = {
        (v[ex_name]["name"] if isinstance(v[ex_name], dict) else v[ex_name]): k
        for k, v in config_symbols.items()
    }
    symbol_multiplier_cache = {
        (v[ex_name]["name"] if isinstance(v[ex_name], dict) else v[ex_name]): (
            v[ex_name]["multiplier"] if isinstance(v[ex_name], dict) else 1.0
        )
        for _, v in config_symbols.items()
    }
    # print('symbol_multiplier_cache', symbol_multiplier_cache)
    symbol_info_cache = {}

    redis_client = redis.Redis.from_url(
        conf.redis.url, encoding="utf-8", decode_responses=True
    )

    count = 0
    while True:
        if cancel_ctx.is_canceled():
            break
        try:
            res = []
            data = None
            if task_queue.qsize() > 0:
                try:
                    for _ in range(task_queue.qsize()):
                        data = task_queue.get_nowait()
                        res.append(data)
                except queue.Empty:
                    pass
            else:
                try:
                    data = task_queue.get(block=True, timeout=5)
                    res.append(data)
                except queue.Empty:
                    pass
            for item_raw in res:
                item = json.loads(item_raw)
                # print(f"-- {ex_name} {item}")
                if item.get("E"):
                    symbol = symbol_cache[item["s"]]
                    if symbol:
                        # if conf.debug:
                        #     now = now_ms()
                        #     logging.info(
                        #         f"-- {ex_name} {symbol} {now - int(item['T'])}ms"
                        #     )
                        try:
                            key = get_ob_storage_key(ex_name, symbol)
                            notify_key = get_ob_notify_key(ex_name, symbol)
                            result = {
                                "ex": ex_name,
                                "symbol": symbol,
                                "ts": item["T"],
                                "bids": normalize_orderbook_5(item["b"], symbol_multiplier_cache[item['s']]),
                                "asks": normalize_orderbook_5(item["a"], symbol_multiplier_cache[item['s']]),
                            }
                            # print(f">> {ex_name} {result}")
                            if (
                                symbol_info_cache.get(symbol)
                                and symbol_info_cache[symbol] == result
                            ):
                                continue
                            symbol_info_cache[symbol] = result
                            redis_client.set(key, json.dumps(result))
                            if redis_client.llen(notify_key) == 0:
                                redis_client.lpush(
                                    notify_key, json.dumps({"updated": True})
                                )
                        except Exception as ex:
                            logging.error(ex)
                            logging.exception(ex)
                # if item.get("E"):
                #     logging.info(
                #         f"{item['s']} ts={item['E']} duration={round(time.time() - float(item['E']/1000),3)}s {item['b']}"
                #     )
        except Exception as ex:
            logging.exception(ex)
        finally:
            count += 1
            if count == sys.maxsize:
                count = 0
            if count % 1000 == 0:
                logging.info(
                    f"--------> binance task queue size: {task_queue.qsize()}"
                )


def fetch_orderbook_mainloop(conf: FetchConfig, cancel_ctx: CancelContext):
    redis_client = redis.Redis.from_url(
        conf.redis.url, encoding="utf-8", decode_responses=True
    )
    enabled_symbols = conf.cross_arbitrage_symbol_datas
    if not enabled_symbols:
        logging.warning(
            "enabled_symbols field is not found in config, use all symbols"
        )
        enabled_symbols = list(conf.symbol_name_datas.keys())

    config_symbols = {
        k: v for k, v in conf.symbol_name_datas.items() if k in enabled_symbols
    }
    logging.info(config_symbols)
    # clear notify list
    for ex_name in ["okex", "binance"]:
        for symbol in config_symbols.keys():
            logging.info(
                f"-- reset orderbook data: {get_ob_storage_key(ex_name,symbol)}"
            )
            redis_client.delete(get_ob_notify_key(ex_name, symbol))
            redis_client.delete(get_ob_storage_key(ex_name, symbol))

    binance_task_queue = queue.Queue(maxsize=0)
    okex_task_queue = queue.Queue(maxsize=0)
    thread_task_objects = []

    thread_task_objects.append(
        threading.Thread(
            target=start_okex_ws_task,
            args=(cancel_ctx, config_symbols, okex_task_queue, conf),
            name=f"okex_ws_task",
            daemon=True,
        )
    )

    thread_task_objects.append(
        threading.Thread(
            target=start_binance_ws_task,
            args=(cancel_ctx, config_symbols, binance_task_queue, conf),
            name=f"binance_ws_task",
            daemon=True,
        )
    )

    for i in range(conf.worker_number):
        thread_task_objects.append(
            threading.Thread(
                target=process_okex_ws_task,
                args=(cancel_ctx, config_symbols, okex_task_queue, conf),
                name=f"process_okex_ws_task_{i}",
                daemon=True,
            )
        )
        thread_task_objects.append(
            threading.Thread(
                target=process_binance_ws_task,
                args=(cancel_ctx, config_symbols, binance_task_queue, conf),
                name=f"process_binance_ws_task_{i}",
                daemon=True,
            )
        )

    for thread_object in thread_task_objects:
        thread_object.start()

    while True:
        if cancel_ctx.is_canceled():
            for thread_object in thread_task_objects:
                thread_object.join()
            break

        time.sleep(5)
