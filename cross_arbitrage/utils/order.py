import logging
from decimal import Decimal
from typing import List, Union

import ccxt
import redis

from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.order.globals import exchanges
from cross_arbitrage.order.model import OrderSide
from cross_arbitrage.order.order_book import OrderSignal
from cross_arbitrage.order.position_status import PositionDirection
from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol

def order_mode_is_pending(ctx):
    return ctx.get('order_mode') == 'pending'

def order_mode_is_reduce_only(ctx):
    return ctx.get('order_mode') == 'reduce_only'

def order_mode_is_normal(ctx):
    return ctx.get('order_mode') == 'normal'

def get_order_status_key(order_id: str, ex_name: str):
    return f"order_status:{ex_name}:{order_id}"


def normalize_order_qty(exchange: ccxt.Exchange, symbol:str, qty:Union[float,str,Decimal]):
    exchange.load_markets()
    symbol_info = exchange.market(symbol)
    match exchange:
        case ccxt.okex():
            qty = Decimal(str(qty)) / Decimal(str(symbol_info["contractSize"]))
            qty = exchange.amount_to_precision(symbol, qty)
            return Decimal(str(qty)) * Decimal(str(symbol_info["contractSize"]))
        case ccxt.binanceusdm():
            return Decimal(exchange.amount_to_precision(symbol, qty))
        case _:
            raise Exception(f"unsupported exchanges: {exchange.name}")

def normalize_exchanges_order_qty(exchanges: List[ccxt.Exchange], symbol:str, qty:Union[float,str,Decimal]):
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
            symbol_config = config.get_symbol_data_by_makeonly(signal.symbol, signal.maker_exchange)
            max_notional_per_order = Decimal(
                str(symbol_config.max_notional_per_order)
            )
            if signal.maker_price * signal.maker_qty > max_notional_per_order:
                res = max_notional_per_order / signal.maker_price
            else:
                res = signal.maker_qty
        ccxt_symbol = get_ccxt_symbol(signal.symbol)
        order_qty = normalize_exchanges_order_qty(
            exchanges=[exchanges[signal.maker_exchange],exchanges[signal.taker_exchange]],
            symbol=ccxt_symbol,
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
