import base64
import hashlib
import hmac
import json
import logging
import sys
import threading
import time
import traceback
from urllib.parse import urlparse

from websocket import WebSocketApp


# == binance websocket client
class OkexWebSocketApp(WebSocketApp):
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


class OkexPublicWebSocketClient:
    def __init__(self, context_args={}):
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.context_args = context_args
        if self.context_args.pop("is_private", False):
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/private"
            self.public_key = self.context_args.get("public_key")
            self.private_key = self.context_args.get("private_key")
            self.password = self.context_args.get("password")

        self.task_queue = self.context_args.pop("task_queue")
        self.ping_interval = self.context_args.get("ping_interval") or 30
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
                    f"OkexWebsocketClient: using proxy {self.http_proxy_host}:{self.http_proxy_port}"
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

    def _build_channel_message(self, method, params):
        return json.dumps(
            {
                "op": method,
                "args": params,
            }
        )

    def _build_method_message(self, method, params):
        id = self.next_cid()
        return json.dumps(
            {
                "id": str(id),
                "op": method,
                "args": params,
            }
        )

    def _subscribe_channel(self, params={}):
        method = "subscribe"
        self.send_message(self._build_channel_message(method, params))

    def _send_method(self, method, params={}):
        self.send_message(self._build_method_message(method, params))

    def login(self):
        try:
            if hasattr(self, "public_key") and hasattr(self, "private_key"):
                method = "login"
                now_s = int(time.time())
                # print("timestamp: " + str(now_s))
                sign = f"{now_s}GET/users/self/verify"
                total_params = bytes(sign, encoding="utf-8")
                signature = hmac.new(
                    bytes(self.private_key, encoding="utf-8"),
                    total_params,
                    digestmod=hashlib.sha256,
                ).digest()
                signature = base64.b64encode(signature)
                signature = str(signature, "utf-8")
                params = [
                    {
                        "apiKey": self.public_key,
                        "passphrase": self.password,
                        "timestamp": now_s,
                        "sign": signature,
                    }
                ]
                self._send_method(method, params)
        except:
            traceback.print_exc()

    def _get_order_book_channel(self, symbol, depth=5, interval="100ms"):
        return {
            "channel": "books5",
            "instId": symbol,
        }

    def _get_user_order_channel(self, symbol):
        if symbol:
            return {
                "channel": "orders",
                "instType": "SWAP",
                "instId": symbol,
            }
        else:
            return {
                "channel": "orders",
                "instType": "SWAP",
            }

    def watch_order_book(self, symbol, depth=5, interval="500ms"):
        channel = self._get_order_book_channel(
            symbol=symbol, depth=depth, interval=interval
        )
        if channel:
            self._subscribe_channel([channel])

    def watch_order_books(self, symbols, depth=5, interval="500ms"):
        channels = [
            self._get_order_book_channel(
                symbol=symbol, depth=depth, interval=interval
            )
            for symbol in symbols
        ]
        channels = list(filter(lambda x: x != None, channels))
        self._subscribe_channel(channels)

    def watch_user_order(self, symbol=None):
        channel = self._get_user_order_channel(symbol=symbol)
        if channel:
            self._subscribe_channel([channel])

    def watch_user_orders(self, symbols):
        channels = [
            self._get_user_order_channel(symbol=symbol) for symbol in symbols
        ]
        channels = list(filter(lambda x: x != None, channels))
        self._subscribe_channel(channels)

    def create_order(
        self,
        symbol,
        side,
        amount,
        price=None,
        order_type="post_only",
        margin_mode="cross",
        client_order_id=None,
        reduce_only=False,
    ):
        method = "order"
        arg = {
            "side": side,  # buy or sell
            "instId": symbol,
            "ordType": order_type,  # market, limit, post_only, fok, ioc
            "sz": amount,
            "tdMode": margin_mode,  # cross or isolated
        }
        if price:
            arg.update({"px": price})
        if client_order_id:
            arg.update({"clOrdId": client_order_id})
        if reduce_only:
            arg.update({"reduceOnly": reduce_only})
        self._send_method(method, [arg])

    def cancel_order(
        self,
        order_id,
        symbol,
    ):
        method = "cancel-order"
        arg = {
            "ordId": order_id,
            "instId": symbol,
        }
        self._send_method(method, [arg])

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
        self.client_ws = OkexWebSocketApp(
            self.ws_url,
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
