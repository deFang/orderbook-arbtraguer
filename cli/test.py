# entry
import logging
from os.path import exists, join

import ccxt
import click

from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.order.model import normalize_ccxt_order
from cross_arbitrage.order.signal_dealer import _get_order
from cross_arbitrage.utils.exchange import get_exchange_name, get_symbol_min_amount
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file


@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
def main(env: str):
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
    print(config.cross_arbitrage_symbol_datas)
    print(config.get_symbol_datas("APT/USDT"))
    print(config.get_symbol_data_by_makeonly("APT/USDT", "okex"))


if __name__ == "__main__":
    main()
