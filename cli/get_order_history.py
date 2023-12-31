# entry
import json
import logging
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from os.path import exists, join
from time import sleep

import ccxt
import click
import pandas as pd

from cross_arbitrage.fetch.utils.common import (base_name, get_project_root,
                                                now_ms, save_dictlist_to_csv)
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.utils.ccxt_patch import patch
from cross_arbitrage.utils.decorator import paged_since, retry
from cross_arbitrage.utils.exchange import get_bag_size
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import (
    get_ccxt_symbol, get_ccxt_symbols, get_common_symbol_from_exchange_symbol,
    get_exchange_symbol, get_exchange_symbol_from_exchange,
    init_symbol_mapping_from_file, init_symbol_mapping)

patch()

# constants
DATA_DIR = "data"


def date_now_str():
    return datetime.now().strftime("%Y%m%d")


def normalize_symbol(origin_symbol: str, quote="USDT"):
    if origin_symbol.endswith(quote):
        base = origin_symbol.removesuffix(quote)
        return f"{base}/{quote}"
    elif len(origin_symbol.split("-")) == 3:
        parts = origin_symbol.split("-")
        return f"{parts[0]}/{parts[1]}"
    else:
        raise Exception(f"unsupported symbol: {origin_symbol}")


def is_okex_row(row):
    return "-SWAP" in row["symbol"]


def set_dyn_cost(row):
    if row["side"] in ["buy", "BUY"]:
        return -row["cost"]
    else:
        return row["cost"]


def set_dyn_amount(row):
    if row["side"] in ["buy", "BUY"]:
        return row["executedQty"]
    else:
        return -row["executedQty"]


def set_okex_cost(row):
    if is_okex_row(row):
        return row["cost"]
    else:
        return 0


def set_binance_cost(row):
    if is_okex_row(row):
        return 0
    else:
        return row["cost"]


def set_normalized_symbol(row):
    return normalize_symbol(row["symbol"])


@retry(max_retry_count=2, retry_interval_base=0.5)
def fetch_closed_orders(exchange, symbol, since, limit=1000):
    return exchange.fetch_orders(symbol=symbol, since=since, limit=limit)

@retry(max_retry_count=2, retry_interval_base=0.5)
def fetch_funding_history(exchange, since, limit=100):
    return exchange.fetch_funding_history(since=since, limit=limit)

@paged_since(since_field_name="timestamp", paged_id_field_name="id")
def fetch_closed_orders_since(exchange, symbol, since, limit=1000):
    return fetch_closed_orders(
        exchange, symbol=symbol, since=since, limit=limit
    )

@paged_since(since_field_name="timestamp", paged_id_field_name="id")
def fetch_funding_history_since(exchange, since, limit=100):
    return fetch_funding_history(
        exchange, since=since, limit=limit
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
        exchange_symbol_names = [
            get_exchange_symbol_from_exchange(exchange, s).name
            for s in symbols
        ]
        orders_raw = []
        for symbol in exchange_symbol_names:
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


def gen_order_csv(exchange, symbols, env):
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
                common_symbol = get_common_symbol_from_exchange_symbol(
                    o["instId"], exchange.ex_name
                )
                if common_symbol in symbols:
                    bag_size = get_bag_size(exchange, common_symbol)
                    res.append(
                        {
                            "id": o["ordId"],
                            "clientOrderId": o["clOrdId"],
                            "symbol": common_symbol,
                            "timestamp": int(o["cTime"]),
                            "updateTimestamp": int(o["uTime"]),
                            "datetime": datetime.fromtimestamp(
                                int(o["cTime"]) / 1000
                            )
                            .astimezone(timezone.utc)
                            .strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            "type": o["ordType"],
                            "side": o["side"].upper(),
                            "price": o["px"],
                            "avgPrice": o["avgPx"],
                            "origQty": str(Decimal(str(o["sz"])) * bag_size),
                            "executedQty": str(
                                Decimal(str(o["accFillSz"])) * bag_size
                            ),
                            "cost": str(
                                Decimal(o["accFillSz"])
                                * Decimal(o["fillPx"])
                                * bag_size
                            ),
                            "status": o["state"],
                            "reduceOnly": o["reduceOnly"],
                            "leverage": o["lever"],
                            "fee": o["fee"],
                            "source": exchange.ex_name,
                        }
                    )

            save_dictlist_to_csv(
                f"{DATA_DIR}/{env}_{exchange.ex_name}_orders_{date_now_str()}.csv",
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
                    "source",
                ],
                dictlist=res,
                file_mode="w",
            )
        else:
            res = []
            for o in orders_raw:
                common_symbol = get_common_symbol_from_exchange_symbol(
                    o["info"]["symbol"], exchange.ex_name
                )
                bag_size = get_bag_size(exchange, common_symbol)
                res.append(
                    {
                        "id": o["info"]["orderId"],
                        "clientOrderId": o["info"]["clientOrderId"],
                        "symbol": common_symbol,
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
                        "price": str(
                            Decimal(str(o["info"]["price"])) / bag_size
                        ),
                        "avgPrice": str(
                            Decimal(str(o["info"]["avgPrice"])) / bag_size
                        )
                        if o["info"]["avgPrice"]
                        else "",
                        "origQty": str(
                            Decimal(str(o["info"]["origQty"])) * bag_size
                        ),
                        "executedQty": str(
                            Decimal(str(o["info"]["executedQty"])) * bag_size
                        )
                        if o["info"]["executedQty"]
                        else "",
                        "cost": o["info"]["cumQuote"],
                        "timeInForce": o["info"]["timeInForce"],
                        "reduceOnly": o["info"]["reduceOnly"],
                        "closePosition": o["info"]["closePosition"],
                        "source": exchange.ex_name,
                    }
                )

            save_dictlist_to_csv(
                f"{DATA_DIR}/{env}_{exchange.ex_name}_orders_{date_now_str()}.csv",
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
                    "source",
                ],
                dictlist=res,
                file_mode="w",
            )


def analysis_orders(env):
    # dev
    fee_rate = {
        "binance": {
            "maker": 0.0002,
            "taker": 0.0004,
        },
        "okex": {
            "maker": 0.0002,
            "taker": 0.0005,
        },
    }
    if env == "aalh11":
        fee_rate = {
            "binance": {
                "maker": 0,
                "taker": 0.00017,
            },
            "okex": {
                "maker": 0,
                "taker": 0.0002,
            },
        }
    data_path = join(get_project_root(), DATA_DIR)
    df1 = pd.read_csv(f"{data_path}/{env}_binance_orders_{date_now_str()}.csv")
    df1 = df1.drop(columns=["timeInForce"])

    df2 = pd.read_csv(f"{data_path}/{env}_okex_orders_{date_now_str()}.csv")

    df3 = pd.concat([df1, df2]).sort_values(
        ["timestamp", "clientOrderId"], ascending=[True, True]
    )

    df3 = df3.assign(dyn_cost=df3.apply(set_dyn_cost, axis=1))
    df3 = df3.assign(dyn_amount=df3.apply(set_dyn_amount, axis=1))
    df3 = df3.assign(ok_cost=df3.apply(set_okex_cost, axis=1))
    df3 = df3.assign(bn_cost=df3.apply(set_binance_cost, axis=1))

    # summary

    print("")
    print("=" * 20, "汇总", "=" * 20)
    ok_orders = df3.loc[df3["symbol"].str.contains("-SWAP", case=True)]
    print(
        f"okex订单: count={ok_orders['id'].count()}, notional={ok_orders['cost'].sum()}"
    )
    bn_orders = df3.loc[~df3["symbol"].str.contains("-SWAP", case=True)]
    print(
        f"bn订单:   count={bn_orders['id'].count()}, notional={bn_orders['cost'].sum()}"
    )

    notnull_df3 = df3[~df3["clientOrderId"].isnull()]
    align_orders = notnull_df3.loc[
        notnull_df3["clientOrderId"].str.contains("TalgT", case=True)
    ]
    print(
        f"对齐订单: count={align_orders['id'].count()}, notional={align_orders['cost'].sum()}"
    )
    print(f"毛利润:   {df3['dyn_cost'].sum()}")
    print(f"净利润:   {df3['dyn_cost'].sum() - df3['bn_cost'].sum() * 0.00017}")

    print("")
    print("=" * 20, "标的明细", "=" * 20)
    df4 = df3.assign(_symbol=df3.apply(set_normalized_symbol, axis=1))
    # df4.groupby('_symbol')['dyn_cost'].sum()
    df5 = df4.groupby("_symbol")["dyn_amount"].sum().to_frame()
    df5["毛利润"] = (df4.groupby("_symbol")["dyn_cost"].sum().to_frame())[
        "dyn_cost"
    ]
    df5["手续费"] = (df4.groupby("_symbol")["cost"].sum() * 0.000085).to_frame()[
        "cost"
    ]
    df5 = df5.rename(columns={"dyn_cost": "毛利润", "dyn_amount": "净仓位"})
    df5["净利润"] = df5["毛利润"] - df5["手续费"]

    df5.index.rename("标的", inplace=True)

    print(df5)


def gen_funding_csv(exchange, ccxt_symbols, since, env):
    res = fetch_funding_history_since(exchange, since=since, limit=100)
    #res  = [r for r in res if r['symbol'] in ccxt_symbols]
    save_dictlist_to_csv(
        f"{DATA_DIR}/{env}_{exchange.ex_name}_funding_{date_now_str()}.csv",
        headers=[
            "id",
            "symbol",
            "timestamp",
            "datetime",
            "amount",
            "code",
        ],
        dictlist=res,
        file_mode="w",
    )


@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
@click.option("--since", help="since timestamp")
def main(env: str, since: str):
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
    init_symbol_mapping(config.symbol_name_datas)
    init_globals(config)
    symbols = [
        symbol.symbol_name for symbol in config.cross_arbitrage_symbol_datas
    ]

    ccxt_symbols = get_ccxt_symbols()

    print(symbols)
    since = int(since)
    until = now_ms()

    d = config.exchanges["okex"]
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

    sync_symbols_orders(
        binance, symbols, since=since, until=until, dir=DATA_DIR
    )
    sync_symbols_orders(okex, symbols, since=since, until=until, dir=DATA_DIR)

    gen_order_csv(okex, symbols, env=env)
    gen_order_csv(binance, symbols, env=env)

    gen_funding_csv(okex, ccxt_symbols, since, env=env)
    gen_funding_csv(binance, ccxt_symbols, since, env=env)

    # analysis_orders(env)


if __name__ == "__main__":
    main()
