from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from cross_arbitrage.fetch.utils.common import ts_to_str
from cross_arbitrage.order.globals import exchanges
from cross_arbitrage.utils.symbol_mapping import (
    get_ccxt_symbol, get_common_symbol_from_ccxt, get_common_symbol_from_exchange_symbol, get_exchange_symbol)


class OrderType(str, Enum):
    limit = "limit"
    market = "market"
    stop_loss = "stop_loss"
    stop_loss_limit = "stop_loss_limit"
    take_profit = "take_profit"
    take_profit_limit = "take_profit_limit"
    limit_maker = "limit_maker"


class OrderSide(str, Enum):
    buy = "buy"
    sell = "sell"


class OrderStatus(str, Enum):
    new = "new"
    partially_filled = "partially_filled"
    filled = "filled"
    canceled = "canceled"
    rejected = "rejected"
    expired = "expired"


class Order(BaseModel):
    exchange: str
    id: str
    order_client_id: str
    timestamp: int
    timestamp_str: str
    last_trade_timestamp: Optional[int] = None
    symbol: str
    type: OrderType
    side: OrderSide
    status: OrderStatus
    price: str
    average_price: Optional[str] = None
    amount: str
    filled: str
    cost: str

    class Config:
        use_enum_values = True


def normalize_okex_order(info) -> Order:
    _type = OrderType.market
    if info["ordType"] in ["limit", "post_only"]:
        _type = OrderType.limit

    side = OrderSide.buy
    if info["side"] == "sell":
        side = OrderSide.sell

    symbol = get_common_symbol_from_exchange_symbol(info["instId"], "okex")

    exchange_symbol = get_exchange_symbol(symbol, 'okex')
    exchange_symbol_name = exchange_symbol.name
    symbol_info = exchanges["okex"].market(exchange_symbol_name)

    status = OrderStatus.new
    if info["state"] == "canceled":
        status = OrderStatus.canceled
    elif info["state"] in ["order_failed", "failed"]:
        status = OrderStatus.rejected
    elif info["state"] == "partially_filled":
        status = OrderStatus.partially_filled
    elif info["state"] == "filled":
        status = OrderStatus.filled

    return Order(
        id=info["ordId"],
        order_client_id=info["clOrdId"],
        exchange="okex",
        timestamp=int(info["cTime"]),
        timestamp_str=ts_to_str(int(info["cTime"]) / 1000),
        last_trade_timestamp=int(info["uTime"]),
        type=_type,
        side=side,
        symbol=symbol,
        amount=str(
            Decimal(info["sz"]) * Decimal(str(symbol_info["contractSize"])) * exchange_symbol.multiplier
        ),
        filled=str(
            Decimal(info["accFillSz"]) * Decimal(str(symbol_info["contractSize"])) * exchange_symbol.multiplier
        ),
        price=str(Decimal(info["px"]) / exchange_symbol.multiplier),
        cost=info["fillNotionalUsd"],
        average_price=str(Decimal(info["fillPx"]) / exchange_symbol.multiplier) if info["fillPx"] is not None else None,
        status=status,
    )

# TODO: for binance implement
def normalize_common_order(info, ex_name) -> Order:
    _type = OrderType.market
    if info["type"] == "limit":
        _type = OrderType.limit

    side = OrderSide.buy
    if info["side"] == "sell":
        side = OrderSide.sell

    symbol = get_common_symbol_from_ccxt(info["symbol"])
    exchange_symbol = get_exchange_symbol(symbol, ex_name)
    exchange_symbol_name = exchange_symbol.name
    symbol_info = exchanges[ex_name].market(exchange_symbol_name)

    status = OrderStatus.new
    if info["status"] == "canceled":
        status = OrderStatus.canceled
    elif info["status"] in ["order_failed", "failed"]:
        status = OrderStatus.rejected
    elif info["status"] == "partially_filled":
        status = OrderStatus.partially_filled
    elif info["status"] == "closed":
        status = OrderStatus.filled

    return Order(
        id=info["id"],
        order_client_id=info["clientOrderId"],
        exchange=ex_name,
        timestamp=int(info["timestamp"]),
        timestamp_str=ts_to_str(int(info["timestamp"]) / 1000),
        last_trade_timestamp=int(info["lastTradeTimestamp"]) if info.get("lastTradeTimestamp") else 0,
        type=_type,
        side=side,
        symbol=symbol,
        amount=str(
            Decimal(info["amount"]) * Decimal(str(symbol_info["contractSize"])) * exchange_symbol.multiplier
        ),
        filled=str(
            Decimal(info["filled"]) * Decimal(str(symbol_info["contractSize"])) * exchange_symbol.multiplier
        ),
        price=str(Decimal(info["price"]) / exchange_symbol.multiplier),
        cost=info["cost"],
        average_price=str(Decimal(info["average"]) / exchange_symbol.multiplier) if info["fillPx"] is not None else None,
        status=status,
    )
