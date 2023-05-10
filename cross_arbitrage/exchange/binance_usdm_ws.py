import json
import logging
import sys
import threading
import time
from urllib.parse import urlparse

import requests
from websocket import WebSocketApp

from cross_arbitrage.fetch.utils.common import now_s
from cross_arbitrage.utils.context import sleep_with_context


# == binance websocket client
class BinanceUsdsWebSocketApp(WebSocketApp):
    def send_ping(self, payload=""):
        if self.sock:
            try:
                self.sock.ping(payload)
            except Exception as ex:
                logging.exception(ex)
        return

    def send_pong(self, payload=""):
        if self.sock:
            try:
                self.sock.pong(payload)
            except Exception as ex:
                logging.exception(ex)
        return


class BinanceUsdsPublicWebSocketClient:
    def __init__(self, context_args={}):
        self.ws_url = "wss://fstream.binance.com/ws/"
        self.http_url = "https://fapi.binance.com"
        self.ctx = context_args.pop("ctx", None)
        self.context_args = context_args
        # self.account = self.context_args.pop("account")
        if self.context_args.pop("is_private", False):
            self.is_private = True
            self.ws_url = "wss://fstream.binance.com/ws"
            self.public_key = self.context_args.get("public_key")
            self.private_key = self.context_args.get("private_key")
            self.http_headers = {"X-MBX-APIKEY": self.public_key}
        else:
            self.http_headers = {}
            self.is_private = False

        self.task_queue = self.context_args.pop("task_queue")
        self.ping_interval = self.context_args.get("ping_interval") or 60
        self.ping_timeout = self.context_args.get("ping_timeout") or 10
        self.http_proxy = self.context_args.get("http_proxy")
        self.debug = self.context_args.get("debug")
        if self.http_proxy:
            try:
                urlobj = urlparse(self.http_proxy)
                self.http_proxy_host = urlobj.hostname
                self.http_proxy_port = urlobj.port
                self.proxy_type = urlobj.scheme
                logging.info(
                    f"BinanceWebsocketClient: using proxy {self.http_proxy_host}:{self.http_proxy_port}"
                )
            except Exception as ex:
                logging.error(ex)
                pass
        else:
            self.http_proxy_host = None
            self.http_proxy_port = None
            self.proxy_type = None

        self.client_ws = None
        self.client_thread = None
        self.listen_key = None
        self.listen_key_thread = None
        self.ws_status = "DISCONNECTED"  # ("CONNECTING", "CONNECTED","DISCONNECTING", "DISCONNECTED")
        self.last_ping_timestamp = int(time.time())
        self.cid = 0
        self.last_rev_timestamp = int(time.time())
        self.message_count = 0

        return

    def next_cid(self):
        self.cid += 1
        if self.cid == sys.maxsize:
            self.cid = 1
        return self.cid

    def _build_jsonrpc_method(self, method, params):
        return json.dumps(
            {
                "id": self.next_cid(),
                "method": method,
                "params": params,
            }
        )

    def _send(self, method, params={}):
        self.send_message(self._build_jsonrpc_method(method, params))

    def login(self):
        pass

    def start_refresh_listen_key(self):
        # thread to refresh listen key
        self.listen_key_thread = threading.Thread(
            target=self.refresh_listen_key_loop,
            kwargs={"ctx": self.ctx},
            daemon=True,
        )
        self.listen_key_thread.start()

    def stop_refresh_listen_key(self):
        if self.listen_key_thread and self.listen_key_thread.is_alive():
            self.listen_key_thread.join()

    def user_ws_url(self):
        if self.is_private and self.listen_key:
            return f"{self.ws_url}/{self.listen_key}"
        elif self.is_private:
            raise Exception(f"binance user stream error: listen_key is empty")
        else:
            return f"{self.ws_url}"

    def create_listen_key(self) -> bool:
        resp = requests.post(
            f"{self.http_url}/fapi/v1/listenKey", headers=self.http_headers
        )
        res = resp.json()
        # print(res)
        if res and res.get("listenKey"):
            self.listen_key = res["listenKey"]
            logging.info(f"creating listen key: {self.listen_key}")
            return True
        return False

    def refresh_listen_key(self) -> bool:
        logging.info(f"refreshing listen key: {self.listen_key}")
        if self.listen_key == None:
            return self.create_listen_key()
        else:
            resp = requests.put(
                f"{self.http_url}/fapi/v1/listenKey", headers=self.http_headers
            )
            if resp.ok:
                return True
            return False

    def remove_listen_key(self) -> bool:
        # print(f"hit {self.listen_key}")
        if self.listen_key == None:
            return False
        else:
            resp = requests.delete(
                f"{self.http_url}/fapi/v1/listenKey", headers=self.http_headers
            )
            if resp.ok:
                print(f"removing listen_key {self.listen_key}...done")
                self.listen_key = None
                return True
            return False

    def refresh_listen_key_loop(self, ctx=None):
        minutes = 30
        while True:
            if ctx and ctx.is_canceled():
                # print('exiting refresh listen key loop...')
                self.remove_listen_key()
                break

            self.refresh_listen_key()
            sleep_with_context(self.ctx, minutes * 60)

    def _get_order_book_channel(self, symbol, depth=5, interval="100ms"):
        return f"{symbol.lower()}@depth{depth}@{interval}"

    def watch_order_book(self, symbol, depth=5, interval="100ms"):
        method = "SUBSCRIBE"
        channel = self._get_order_book_channel(
            symbol=symbol, depth=depth, interval=interval
        )
        self._send(method, [channel])

    def watch_order_books(self, symbols, depth=5, interval="100ms"):
        method = "SUBSCRIBE"
        channels = [
            self._get_order_book_channel(
                symbol=symbol, depth=depth, interval=interval
            )
            for symbol in symbols
        ]
        self._send(method, channels)

    def watch_user_order(self, symbol=None):
        # channel = self._get_user_order_channel(symbol=symbol)
        # if channel:
        #     self._subscribe_channel([channel])
        pass

    def watch_user_orders(self, symbols=[]):
        # channels = [
        #     self._get_user_order_channel(symbol=symbol) for symbol in symbols
        # ]
        # channels = list(filter(lambda x: x != None, channels))
        # self._subscribe_channel(channels)
        pass

    def get_status(self):
        return self.ws_status

    def send_message(self, message):
        logging.info(f"websocket client send message: {message}")
        if self.client_ws:
            self.client_ws.send(message)
        return

    def send_ping(self, payload=""):
        logging.debug(f"websocket client send ping: {payload}")
        if self.client_ws:
            self.client_ws.send_ping(payload)
        return

    def send_pong(self, payload=""):
        logging.debug(f"websocket client send pong: {payload}")
        if self.client_ws:
            self.client_ws.send_pong(payload)
        return

    def decode_message(self, message):
        return message

    def get_parse_data(self, message):
        raise NotImplementedError

    def on_open(self, ws):
        logging.info(f"websocket on_open")
        self.last_rev_timestamp = int(time.time())
        self.ws_status = "CONNECTED"
        return

    def on_error(self, ws, error):
        logging.error(f"websocket on_error: {error}")
        return

    def on_close(self, ws, code, msg):
        logging.warning("websocket on_close")
        self.ws_status = "DISCONNECTED"
        return

    def on_message(self, ws, message):
        # logging.info(f"WebSocket on_message: {message}")
        try:
            self.last_rev_timestamp = int(time.time())
            self.task_queue.put(message)
            self.message_count += 1
            if self.debug and self.message_count % 200 == 0:
                logging.info(f"binance messsage count: {self.message_count}")
        except Exception as ex:
            logging.error(ex)
        return

    def on_ping(self, ws, ping):
        logging.debug(f"WebSocket on_ping: {ping}")
        self.last_rev_timestamp = int(time.time())
        if self.client_ws:
            self.client_ws.send_pong(ping)
            self.last_ping_timestamp = int(time.time())
        return

    def on_pong(self, ws, pong):
        logging.debug(f"WebSocket on_pong: {pong}")
        self.last_rev_timestamp = int(time.time())
        return

    def start_client(self):
        logging.info(f"websocket client starting...")
        if self.ws_status != "DISCONNECTED":
            logging.error(f"websocket status is invalid: {self.ws_status}")
            return
        if not self.ws_url:
            logging.error(f"websocket url is empty")
            return
        if self.client_thread is not None:
            logging.error(f"websocket thread is not None")
            return
        if self.client_ws is not None:
            logging.error(f"websocket client is not None")
            return
        if self.listen_key is None:
            logging.error(f"websocket listen key is None")
            return
        self.client_ws = BinanceUsdsWebSocketApp(
            self.user_ws_url(),
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
            on_pong=self.on_pong,
        )

        self.client_thread = threading.Thread(
            target=self.client_ws.run_forever,
            kwargs={
                "ping_interval": self.ping_interval,
                "ping_timeout": self.ping_timeout,
                "http_proxy_host": self.http_proxy_host,
                "http_proxy_port": self.http_proxy_port,
                "proxy_type": self.proxy_type,
            },
            daemon=True,
        )
        self.ws_status = "CONNECTING"
        self.client_thread.start()
        self.last_rev_timestamp = int(time.time())
        return

    def stop_client(self):
        logging.info("websocket client is stopping...")
        self.ws_status = "DISCONNECTING"
        if self.client_ws:
            self.client_ws.close()
        if self.client_thread and self.client_thread.is_alive():
            self.client_thread.join()

        self.client_ws = None
        self.client_thread = None
        self.ws_status = "DISCONNECTED"

        return
