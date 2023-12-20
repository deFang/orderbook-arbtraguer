from typing import Dict
import ccxt

from cross_arbitrage.order.config import OrderConfig

exchanges = {}

order_status_stream_is_ready = {}


def init_globals(config: OrderConfig):
    global exchanges

    if len(exchanges.keys()) == 0:
        # exchanges = {k: None for k in config.exchanges.keys()}

        exchanges["okex"] = ccxt.okex()
        exchanges["binance"] = ccxt.binanceusdm()

        for ex in exchanges.values():
            ex.load_markets()


def get_order_status_stream_is_ready():
    global order_status_stream_is_ready
    if len(order_status_stream_is_ready.values()) > 0:
        return all(order_status_stream_is_ready.values())
    else:
        return False


def set_order_status_stream_is_ready(res: Dict[str, bool]):
    global order_status_stream_is_ready
    order_status_stream_is_ready.update(res)
