# entry
import logging
import json
from pprint import pprint
from os.path import exists, join

import ccxt
import click
import redis

from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.order.config import OrderConfig, get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file

def get_symbol_funding(symbol:str, exchange: str, rc: redis.Redis):
    try:
        key = lambda e,s: f"funding_rate:{e}:{s}"
        res = rc.get(key(exchange, symbol))
        if res:
            return json.loads(res)
    except Exception as ex:
        logging.error(ex)
        logging.exception(ex)
    return None

def get_symbol_threshold(symbol:str, exchange:str, rc: redis.Redis):
    try:
        key = lambda e: f"order:thresholds:{e}"
        res = rc.hget(key(exchange), symbol)
        if res:
            return json.loads(res)
    except Exception as ex:
        logging.error(ex)
        logging.exception(ex)
    return None

def get_symbol_position(symbol:str, exchange:str, rc: redis.Redis):
    try:
        key = lambda :f"order:position_status"
        res = rc.hget(key(), f"{exchange}:{symbol}")
        if res:
            return json.loads(res)
    except Exception as ex:
        logging.error(ex)
        logging.exception(ex)
    return None

def get_symbol_info(symbol:str, exchange: str, rc:redis.Redis):
    funding = get_symbol_funding(symbol, exchange, rc)
    threshold = get_symbol_threshold(symbol, exchange, rc)
    return (funding, threshold)

def print_symbol_infos(symbol:str, config: OrderConfig, rc:redis.Redis):
    res = {}
    for ex_name in config.exchanges.keys():
        if not res.get(ex_name):
            res[ex_name] = {}
        funding, threshold = get_symbol_info(symbol, ex_name, rc)
        res[ex_name]['funding'] = funding
        res[ex_name]['threshold'] = threshold

    ex_names = list(res.keys())

    print(f"funding_delta={float(res[ex_names[0]]['funding']['funding_rate']) - float(res[ex_names[1]]['funding']['funding_rate'])}")
    print(f"----> {ex_names[0]}:")
    pprint(res[ex_names[0]]['funding'])
    pprint(res[ex_names[0]]['threshold'])
    print(f"----> {ex_names[1]}:")
    pprint(res[ex_names[1]]['funding'])
    pprint(res[ex_names[1]]['threshold'])


@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
@click.option("--symbol", "-s", help="use a symbol", default="")
def main(env: str, symbol:str):
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
    init_symbol_mapping_from_file(
        join(get_project_root(), "configs/common_config.json")
    )
    init_globals(config)

    config.print()

    rc:redis.Redis = redis.Redis.from_url(config.redis.url)

    if symbol:
        print_symbol_infos(symbol, config, rc)
    else:
        for s in config.cross_arbitrage_symbol_datas:
            print_symbol_infos(s.symbol_name, config, rc)


if __name__ == "__main__":
    main()
