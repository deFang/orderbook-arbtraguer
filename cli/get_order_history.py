# entry
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from os.path import exists, join
from time import sleep
import traceback
import ccxt

import click

from cross_arbitrage.fetch.utils.common import base_name, get_project_root, now_ms, save_dictlist_to_csv
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.utils.decorator import paged_since, retry
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, init_symbol_mapping_from_file


# constants
DATA_DIR = 'data'

def date_now_str():
    return datetime.now().strftime("%Y%m%d")


@retry(max_retry_count=2, retry_interval_base=0.5)
def fetch_closed_orders(exchange, symbol, since, limit=1000):
    return exchange.fetch_orders(symbol=symbol, since=since, limit=limit)


@paged_since(since_field_name="timestamp", paged_id_field_name="id")
def fetch_closed_orders_since(exchange, symbol, since, limit=1000):
    return fetch_closed_orders(
        exchange, symbol=symbol, since=since, limit=limit
    )


def sync_symbols_orders(exchange, symbols, since, until, dir="data"):
    orders_raw = []
    if exchange.ex_name == "okex":
        last_timestamp = since
        cache = {}
        retry = 0
        while retry <= 3:
            print("last_timestamp: ", last_timestamp)
            try:
                orders = exchange.privateGetTradeOrdersHistoryArchive(
                    {
                        "instType": "SWAP",
                        "begin": last_timestamp,
                    }
                )
                for order in orders["data"]:
                    cache[order["ordId"]] = order

                if len(orders["data"]) < 100:
                    break
                else:
                    timestamps = [int(o["cTime"]) for o in orders["data"]]
                    last_timestamp = max(timestamps)
                    if last_timestamp > until:
                        break
            except Exception as ex:
                traceback.print_exc()
                print(f"fetch error: {ex}")
                retry += 1
            else:
                retry = 0

            sleep(1)

        orders_raw = list(cache.values())
        # orders_raw = list(
        #     filter(lambda o: o["instId"] in okex_symbols, orders_raw)
        # )

    else:
        orders_raw = []
        for symbol in symbols:
            orders = fetch_closed_orders_since(
                exchange, symbol=symbol, since=since, limit=1000
            )
            print(f"-- {exchange.ex_name} {symbol} {len(orders)}")
            orders_raw.extend(orders)

    with open(
        f"{dir}/{exchange.ex_name}_raw.json",
        "w",
    ) as f:
        f.write(json.dumps(orders_raw))


def gen_order_csv(exchange):
    json_name = f"{DATA_DIR}/{exchange.ex_name}_raw.json"
    with open(json_name, "r") as f:
        orders_raw = json.load(f)

        if exchange.ex_name == "okex":
            exchange.load_markets()
            res = []
            for o in orders_raw:
                symbol = o["instId"]
                symbol_parts = symbol.split("-")
                ccxt_symbol = (
                    f"{symbol_parts[0]}/{symbol_parts[1]}:{symbol_parts[1]}"
                )
                symbol_info = exchange.market(ccxt_symbol)
                # print(f"{symbol} {symbol_info['contractSize']}")
                res.append(
                    {
                        "id": o["ordId"],
                        "clientOrderId": o["clOrdId"],
                        "symbol": o["instId"],
                        "timestamp": int(o["cTime"]),
                        "updateTimestamp": int(o["uTime"]),
                        "datetime": datetime.fromtimestamp(
                            int(o["cTime"]) / 1000
                        )
                        .astimezone(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "type": o["ordType"],
                        "side": o["side"],
                        "price": o["px"],
                        "avgPrice": o["avgPx"],
                        "origQty": o["sz"],
                        "executedQty": o["accFillSz"],
                        "cost": str(
                            Decimal(o["accFillSz"])
                            * Decimal(o["fillPx"])
                            * Decimal(str(symbol_info["contractSize"]))
                        ),
                        "status": o["state"],
                        "reduceOnly": o["reduceOnly"],
                        "leverage": o["lever"],
                        "fee": o["fee"],
                    }
                )

            save_dictlist_to_csv(
                f"{DATA_DIR}/{exchange.ex_name}_orders_{date_now_str()}.csv",
                headers=[
                    "id",
                    "clientOrderId",
                    "symbol",
                    "timestamp",
                    "updateTimestamp",
                    "datetime",
                    "type",
                    "side",
                    "price",
                    "avgPrice",
                    "origQty",
                    "executedQty",
                    "cost",
                    "status",
                ],
                dictlist=res,
                file_mode="w",
            )
        else:
            res = [
                {
                    "id": o["info"]["orderId"],
                    "clientOrderId": o["info"]["clientOrderId"],
                    "symbol": o["info"]["symbol"],
                    "status": o["info"]["status"],
                    "timestamp": int(o["info"]["time"]),
                    "updateTimestamp": int(o["info"]["updateTime"]),
                    "datetime": datetime.fromtimestamp(
                        int(o["info"]["time"]) / 1000
                    )
                    .astimezone(timezone.utc)
                    .strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "type": o["info"]["type"],
                    "side": o["info"]["side"],
                    "price": o["info"]["price"],
                    "avgPrice": o["info"]["avgPrice"],
                    "origQty": o["info"]["origQty"],
                    "executedQty": o["info"]["executedQty"],
                    "cost": o["info"]["cumQuote"],
                    "timeInForce": o["info"]["timeInForce"],
                    "reduceOnly": o["info"]["reduceOnly"],
                    "closePosition": o["info"]["closePosition"],
                }
                for o in orders_raw
            ]

            save_dictlist_to_csv(
                f"{DATA_DIR}/{exchange.ex_name}_orders_{date_now_str()}.csv",
                headers=[
                    "id",
                    "clientOrderId",
                    "symbol",
                    "timestamp",
                    "updateTimestamp",
                    "datetime",
                    "type",
                    "side",
                    "price",
                    "avgPrice",
                    "origQty",
                    "executedQty",
                    "cost",
                    "status",
                    "timeInForce",
                ],
                dictlist=res,
                file_mode="w",
            )


@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
@click.option("--since", help="since timestamp")
def main(env: str, since:str):
    logger = init_logger(base_name(__file__))

    config_files = [
        join(get_project_root(), "configs/common_config.json"),
        join(get_project_root(), "configs/order_config.common.json"),
    ]
    if env:
        file_path = join(
            get_project_root(), f"configs/order_config.{env.lower()}.json"
        )
        if exists(file_path):
            config_files.append(file_path)
        else:
            logging.info(f"-- config file {file_path} is not exist, skipping")

    config = get_config(file_path=config_files, env=env)
    logger.setLevel(getattr(logging, config.log.level.upper()))
    config.print()
    init_symbol_mapping_from_file(
        join(get_project_root(), "configs/common_config.json")
    )

    init_globals(config)

    symbols = [
      get_ccxt_symbol(symbol.symbol_name)  for symbol in config.cross_arbitrage_symbol_datas
    ]
    print(symbols)
    since = int(since)
    until = now_ms()

    d = config.exchanges["okex"]
    print(d)
    okex_option = {
        "apiKey": d.api_key,
        "secret": d.secret,
        "password": d.password,
    }
    okex = ccxt.okex(okex_option)
    okex.ex_name = "okex"

    d = config.exchanges["binance"]
    binance_option = {
        "apiKey": d.api_key,
        "secret": d.secret,
    }

    binance = ccxt.binanceusdm(binance_option)
    binance.ex_name = "binance"

    sync_symbols_orders(binance, symbols, since=since, until=until, dir=DATA_DIR)
    sync_symbols_orders(okex, symbols, since=since, until=until, dir=DATA_DIR)

    gen_order_csv(okex)
    gen_order_csv(binance)


if __name__ == "__main__":
    main()
