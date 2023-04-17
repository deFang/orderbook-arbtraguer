import logging
from os.path import exists, join
import signal
import time

import click

from cross_arbitrage.order.config import get_config
from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.order.globals import init_globals
from cross_arbitrage.order.order import start_loop
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.logger import init_logger
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file


# entry
@click.command()
@click.option("--env", "-e", help="use a environment", default="dev")
def main(env: str):
    logger = init_logger(base_name(__file__))

    config_files = [join(get_project_root(), "configs/common_config.json"),
                    join(get_project_root(), "configs/order_config.common.json")]
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
    init_symbol_mapping_from_file(join(get_project_root(), "configs/common_config.json"))

    init_globals(config)

    ctx = CancelContext()

    def _exit(signum, frame):
        logging.warning(f"===> get a signal {signum}, exiting")
        ctx.cancel()
        logging.warning("===> sleep 5 seconds to wait for exit")
        time.sleep(5)
        exit(0)

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    start_loop(ctx, config)

    logging.warning("===> exit")


if __name__ == "__main__":
    main()
