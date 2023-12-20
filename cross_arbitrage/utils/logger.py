import logging
import os
import sys
from functools import lru_cache
from logging import handlers
from os.path import abspath, isabs, join

from cross_arbitrage.fetch.utils.common import get_project_root



@lru_cache(maxsize=64)
def init_logger(
    name: str,
    level: str = 'info',
    dir: str = "logs",
):
    dir = str(join(get_project_root(), dir))

    logger = logging.getLogger()
    logger.name = name
    logger.setLevel(getattr(logging, level.upper()))

    logger.addHandler(logging.StreamHandler(stream=sys.stdout))

    path = join(dir, f"{name}.log")
    os.makedirs(dir, exist_ok=True)

    file_handler = handlers.TimedRotatingFileHandler(
        path if isabs(path) else abspath(path),
        when="D",
        interval=1,
        utc=True,
    )
    logger.addHandler(file_handler)

    for handler in logger.handlers:
        handler.setFormatter(
            logging.Formatter(
                fmt=r"%(asctime)s::%(name)s::%(funcName)s::%(levelname)s: %(message)s",
                datefmt=r"%Y-%m-%d %H:%M:%S %z",
            )
        )
    return logger
