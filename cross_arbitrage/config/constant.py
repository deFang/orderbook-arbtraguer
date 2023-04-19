ENVS = ["dev", "test", "prod"]

EXCHANGES = ["okex", "binance"]

LOG_LEVELS = ["debug", "info", "warning", "error", "critical"]

ORDER_MODES = ["pending", "reduce_only", "normal"]


def to_ccxt_exchange_name(ex_name: str) -> str:
    if ex_name == "binance":
        return "binanceusdm"
    return ex_name
