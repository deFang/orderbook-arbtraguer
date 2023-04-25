import logging
import signal
import threading
import time
from os.path import exists, join

import click
from playhouse.sqlite_ext import SqliteExtDatabase

from cross_arbitrage.api.config import AppConfig, get_config
from cross_arbitrage.api.collect_data import collect_data
from cross_arbitrage.api.model import init_db
from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.logger import init_logger


# entry
@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
def main(env: str):
    logger = init_logger(base_name(__file__))

    config_files = [
        join(get_project_root(), "configs/common_config.json"),
        join(get_project_root(), "configs/api_config.common.json"),
    ]
    if env:
        file_path = join(
            get_project_root(), f"configs/api_config.{env.lower()}.json"
        )
        if exists(file_path):
            config_files.append(file_path)
        else:
            logging.info(f"-- config file {file_path} is not exist, skipping")

    config = get_config(file_path=config_files, env=env)

    logger.setLevel('INFO')

    init(config)

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

    thread = threading.Thread(
        target=collect_data,
        args=(cancel_ctx, config),
        name="collect_data",
        daemon=True,
    )
    thread.start()
    thread_objects.append(thread)

    while True:
        if cancel_ctx.is_canceled():
            for thread_object in thread_objects:
                thread_object.join()
            break
        sleep_with_context(cancel_ctx, 1)


def init(config: AppConfig):
    db = SqliteExtDatabase(config.sqlite_path)
    init_db(db)


if __name__ == "__main__":
    main()
