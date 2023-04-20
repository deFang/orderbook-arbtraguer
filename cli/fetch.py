import logging
import signal
import threading
import time
from os.path import exists, join

import click

from cross_arbitrage.fetch.config import get_config
from cross_arbitrage.fetch.fetch_orderbook import fetch_orderbook_mainloop
from cross_arbitrage.fetch.agg_orderbook import agg_orderbook_mainloop
from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.logger import init_logger


# entry
@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
def main(env: str):
    logger = init_logger(base_name(__file__))

    config_files = [
        join(get_project_root(), "configs/common_config.json"),
    ]
    if env:
        file_path = join(
            get_project_root(), f"configs/fetch_config.{env.lower()}.json"
        )
        if exists(file_path):
            config_files.append(file_path)
        else:
            logging.info(f"-- config file {file_path} is not exist, skipping")

    config = get_config(file_path=config_files, env=env)

    logger.setLevel(getattr(logging, config.log.level.upper()))

    config.print()

    # vars
    cancel_ctx = CancelContext()
    thread_objects = []

    # signal
    def _exit(signum, frame):
        logging.info(f"get signal {signal.Signals(signum).name}, stopping...")
        cancel_ctx.cancel()

        for thread_object in thread_objects:
            thread_object.join()

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    # start threads
    thread_objects.append(
        threading.Thread(
            target=fetch_orderbook_mainloop,
            args=(config, cancel_ctx),
            name="fetch_orderbook_mainloop_thread",
            daemon=True,
        )
    )

    thread_objects.append(
        threading.Thread(
            target=agg_orderbook_mainloop,
            args=(config, cancel_ctx),
            name="agg_orderbook_mainloop_thread",
            daemon=True,
        )
    )

    for thread_object in thread_objects:
        thread_object.start()

    while True:
        if cancel_ctx.is_canceled():
            for thread_object in thread_objects:
                thread_object.join()
            break
        time.sleep(5)


if __name__ == "__main__":
    main()
