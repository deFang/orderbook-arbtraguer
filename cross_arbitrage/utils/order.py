import logging
from decimal import Decimal
from typing import List, Union

import ccxt
import redis
from cross_arbitrage.fetch.utils.common import now_ms

from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.globals import exchanges
from cross_arbitrage.order.model import OrderSide
from cross_arbitrage.order.order_book import OrderSignal
from cross_arbitrage.order.position_status import PositionDirection
from cross_arbitrage.utils.exchange import create_exchange, get_bag_size
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_common_symbol_from_ccxt, get_exchange_symbol, get_exchange_symbol_from_exchange


def order_mode_is_pending(ctx):
    return ctx.get('order_mode') == 'pending'


def order_mode_is_reduce_only(ctx):
    return ctx.get('order_mode') == 'reduce_only'


def order_mode_is_normal(ctx):
    return ctx.get('order_mode') == 'normal'


def order_mode_is_maintain(ctx):
    return ctx.get('order_mode') == 'maintain'


def get_order_status_key(order_id: str, ex_name: str):
    return f"order_status:{ex_name}:{order_id}"


def normalize_order_qty(exchange: ccxt.Exchange, symbol: str, qty: Union[float, str, Decimal]):
    exchange.load_markets()
    
    exchange_symbol = get_exchange_symbol_from_exchange(exchange, symbol)
    symbol_info = exchange.market(exchange_symbol.name)
    bag_size = get_bag_size(exchange, symbol)
    match exchange:
        case ccxt.okex() | ccxt.binanceusdm():
            exchange_amount = Decimal(str(qty)) / bag_size
            aligned_exchange_amount = exchange.amount_to_precision(exchange_symbol.name, exchange_amount)
            return Decimal(str(aligned_exchange_amount)) * bag_size
        # case ccxt.binanceusdm():
        #     return Decimal(exchange.amount_to_precision(symbol, Decimal(str(qty)) / Decimal(str(exchange_symbol.multiplier)))) * Decimal(str(exchange_symbol.multiplier))
        case _:
            raise Exception(f"unsupported exchanges: {exchange.name}")


def normalize_exchanges_order_qty(exchanges: List[ccxt.Exchange], symbol: str, qty: Union[float, str, Decimal]):
    qtys = [normalize_order_qty(ex, symbol, qty) for ex in exchanges]
    return min(qtys)


def is_margin_rate_ok(
    signal: OrderSignal, rc: redis.Redis, config: OrderConfig
):
    is_ok = True
    if signal.maker_position:
        match (signal.maker_position.direction, signal.maker_side):
            case (PositionDirection.long, OrderSide.sell) | (
                PositionDirection.short,
                OrderSide.buy,
            ):
                return is_ok
    for exchange in [signal.maker_exchange, signal.taker_exchange]:
        key = f"margin:{exchange}"
        margin_raw = rc.hgetall(name=key)
        margin = {k.decode(): v.decode() for k, v in margin_raw.items()}
        if config.debug:
            logging.info(f"{exchange} margin: {margin}")
        if (
            Decimal(str(margin["used"])) / Decimal(str(margin["total"]))
            > config.max_used_margin
        ):
            is_ok = False
            break

    return is_ok


def get_order_qty(signal: OrderSignal, rc: redis.Redis, config: OrderConfig):
    try:
        if (not is_margin_rate_ok(signal, rc, config)) and (
            not signal.is_reduce_position
        ):
            logging.warn(
                f"margin rate is max than {config.max_used_margin}, return order_qty 0.0"
            )
            return Decimal(0)
        else:
            symbol_config = config.get_symbol_data_by_makeonly(
                signal.symbol, signal.maker_exchange)
            max_notional_per_order = Decimal(
                str(symbol_config.max_notional_per_order)
            )
            if signal.maker_price * signal.maker_qty > max_notional_per_order:
                res = max_notional_per_order / signal.maker_price
            else:
                res = signal.maker_qty
        symbol = signal.symbol
        order_qty = normalize_exchanges_order_qty(
            exchanges=[exchanges[signal.maker_exchange],
                       exchanges[signal.taker_exchange]],
            symbol=symbol,
            qty=res,
        )
        if config.debug:
            logging.info(f"{signal.symbol} order_qty: {order_qty}")
        return order_qty
    except ccxt.ArgumentsRequired as ex:
        if "must be greater than minimum amount precision" in str(ex):
            logging.error(ex)
            return Decimal(0)
        else:
            raise ex
    except Exception as ex:
        logging.exception(ex)
        raise ex


def get_last_funding_rate(exchange_name: str, symbol: str, config: OrderConfig):
    now = now_ms()
    since = now - now % (8 * 60 * 60 * 1000)

    exchange = create_exchange(config.exchanges[exchange_name])

    exchange_symbol_name = get_exchange_symbol(symbol, exchange_name).name

    res = exchange.fetch_funding_rate_history(exchange_symbol_name, since=since)
    if res and len(res) > 0:
        return {
            "exchange": exchange_name,
            "symbol": get_common_symbol_from_ccxt(res[0]['symbol']),
            "funding_rate": str(Decimal(str(res[0]['fundingRate']))),
            "funding_timestamp": res[0]['timestamp'],
            "delta": None,
        }
    else:
        return None
