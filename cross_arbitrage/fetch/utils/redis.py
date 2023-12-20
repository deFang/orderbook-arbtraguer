def get_ob_storage_key(ex_name, symbol):
    return f"origin_orderbook:{ex_name}:{symbol}"


def get_ob_notify_key(ex_name, symbol):
    return f"origin_orderbook:{ex_name}:{symbol}:notify"
