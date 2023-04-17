import logging
from os.path import join, exists
import signal
import threading
import time

import click

from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.order.position_status import refresh_position_loop, get_position_status, refresh_position_status
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.exchange import create_exchange
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file
import redis


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
    config.print()
    init_symbol_mapping_from_file(
        join(get_project_root(), "configs/common_config.json")
    )

    init_globals(config)

    cancel_ctx = CancelContext()
    thread_objects = []

    def _exit(signum, frame):
        logging.info(
            f"cat signal {signal.Signals(signum).name}, stoping program..."
        )
        cancel_ctx.cancel()

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    symbols = [s.symbol_name for s in config.enabled_symbols]
    exchanges = {n: create_exchange(e) for n, e in config.exchanges.items()}
    rc = redis.Redis.from_url(config.redis.url)

    refresh_position_status(rc, exchanges, symbols)
    print_position_status(rc, list(exchanges.keys()), symbols)

    thread_objects.append(
        threading.Thread(
            target=refresh_position_loop,
            args=(cancel_ctx, rc, exchanges, symbols),
            name="refresh_position_mainloop_thread",
            daemon=True,
        )
    )

    for thread_object in thread_objects:
        thread_object.start()

    while not cancel_ctx.is_canceled():
        print_position_status(rc, list(exchanges.keys()), symbols)
        time.sleep(5)


def print_position_status(rc: redis.Redis, exchange_list: list[str], symbols: list[str]):
    for exchange in exchange_list:
        for symbol in symbols:
            position_status = get_position_status(rc, exchange, symbol)
            print(f"==> {exchange}:{symbol}: {position_status}")


if __name__ == "__main__":
    main()
