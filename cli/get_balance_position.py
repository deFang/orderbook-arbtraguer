import logging
from os.path import exists, join
import ccxt
import click
from cross_arbitrage.fetch.utils.common import base_name, get_project_root

from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file

def print_balance(exchange):
    balance1 = exchange.fetch_balance()
    match exchange:
        case ccxt.okex():
            if len(balance1['info']['data'][0]['details']) > 0:
                print(f"--- okex    eq={balance1['info']['data'][0]['details'][0]['eq']} wallet={balance1['info']['data'][0]['details'][0]['cashBal']}")
            else:
                print(f"--- okex    eq=0.0 wallet=0.0")
        case ccxt.binanceusdm():
            print(f"--- binance eq={balance1['info']['totalMarginBalance']} wallet={balance1['info']['totalWalletBalance']}")

    print(f'{exchange.ex_name} total',
        {
            coin: balance
            for coin, balance in balance1["total"].items()
            if balance > 0
        }
    )
    print(f'{exchange.ex_name} used',
        {
            coin: balance
            for coin, balance in balance1["used"].items()
            if balance > 0
        }
    )

def print_positions(exchange):

    positions = exchange.fetch_positions()

    positions = [p for p in positions if p['contracts'] > 0]

    return positions

@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
def main(env):
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

    binance = ccxt.binanceusdm(
        {
            "apiKey": config.exchanges["binance"].api_key,
            "secret": config.exchanges["binance"].secret,
        }
    )
    binance.ex_name = 'binance'


    okex = ccxt.okex(
        {
            "apiKey": config.exchanges["okex"].api_key,
            "secret": config.exchanges["okex"].secret,
            "password": config.exchanges["okex"].password,
        }
    )
    okex.ex_name = 'okex'

    print_balance(binance)
    print_balance(okex)

    p1 = print_positions(binance)
    p2 = print_positions(okex)

    d1 = {p['symbol']:p for p in p1}
    d2 = {p['symbol']:p for p in p2}

    print(f"binance position: {len(d1.keys())}")
    print(f"okex positons   : {len(d2.keys())}")

    for symbol in d1.keys():
        p1 = d1[symbol]
        p2 = d2.get(symbol)
        if p2:
            print(f"binance {p1['symbol']:<20} {round(p1['contracts'] *  p1['contractSize'],2):<10} {round(p1['notional'],2):<10} {round(p1['entryPrice'],4):<10} {p1['side']:<5} {p1['unrealizedPnl']}")
            print(f"okex    {p2['symbol']:<20} {round(p2['contracts'] *  p2['contractSize'],2):<10} {round(p2['notional'],2):<10} {round(p2['entryPrice'],4):<10} {p2['side']:<5} {p2['unrealizedPnl']}")
        else:
            print(f"- binance {p1['symbol']} {p1['contracts'] *  p1['contractSize']} {p1['notional']} {round(p1['entryPrice'],4):<10} {p1['side']} {p1['unrealizedPnl']}")

    for symbol in d2.keys():
        p2 = d2[symbol]
        if not d1.get(symbol):
            print(f"- okex {p2['symbol']} {p2['contracts'] *  p2['contractSize']} {p2['notional']} {round(p2['entryPrice'],4):<10} {p2['side']} {p2['unrealizedPnl']}")


if __name__ == "__main__":
    main()
