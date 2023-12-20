import logging
import queue
import signal
import threading
import time
from os.path import exists, join

import click

from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.order.order_status import \
    start_order_status_stream_mainloop
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file


# entry
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
    task_queue = queue.Queue(maxsize=0)
    thread_objects = []

    def _exit(signum, frame):
        logging.info(
            f"cat signal {signal.Signals(signum).name}, stoping program..."
        )
        cancel_ctx.cancel()

        for thread_object in thread_objects:
            thread_object.join()

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    thread_objects.append(
        threading.Thread(
            target=start_order_status_stream_mainloop,
            args=(cancel_ctx, config),
            name="order_status_stream_mainloop_thread",
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
