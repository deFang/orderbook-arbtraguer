import json
from logging import Logger
import logging
from os.path import exists, join
import signal
import sys
import time
import click

import redis
from cross_arbitrage.fetch.config import get_config

from cross_arbitrage.fetch.utils.common import base_name, get_project_root
from cross_arbitrage.utils.context import CancelContext
from cross_arbitrage.utils.logger import init_logger


# global vars
cancel_ctx = CancelContext()


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

    global redis_client
    if hasattr(config, "redis"):
        redis_client = redis.Redis.from_url(
            config.redis.url, encoding="utf-8", decode_responses=True
        )
    else:
        redis_client = redis.Redis(
            "localhost", 6379, 0, encoding="utf-8", decode_responses=True
        )

    def _exit(signum, frame):
        logging.info(
            f"cat signal {signal.Signals(signum).name}, stoping program..."
        )
        cancel_ctx.cancel()

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)

    while True:
        if cancel_ctx.is_canceled():
            logging.info('exiting...')
            break

        try:
            res = redis_client.xread({config.redis.orderbook_stream: "$"}, block=100, count=1)
            if len(res) == 0:
                continue
            raw_data = res[0][1][0][1]

            for symbol in raw_data.keys():
                obj = json.loads(raw_data[symbol])

                current_ts = int(time.time() * 1000)
                mdata = {
                    "process_ts": obj["ts"],
                    "binance_ts": obj["binance"]["ts"],
                    "okex_ts": int(obj["okex"]["ts"]),
                    "current_ts": current_ts,
                }
                # print(mdata)
                logging.info(
                        f"{symbol:>10} {current_ts} {(mdata['current_ts'] - mdata['process_ts']):>3}ms    bn diff: {(mdata['process_ts'] - mdata['binance_ts']):>3}ms   ok diff: {(mdata['process_ts'] - mdata['okex_ts']):>3}ms"
            )
        except Exception as ex:
            logging.error(ex)

        time.sleep(1)


if __name__ == "__main__":
    main()
