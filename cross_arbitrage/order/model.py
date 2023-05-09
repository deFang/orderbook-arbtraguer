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

    avg_price = str(Decimal(info["fillPx"]) / exchange_symbol.multiplier) if info["fillPx"] else None

    filled_amount =str(
        Decimal(info["accFillSz"]) * Decimal(str(symbol_info["contractSize"])) * exchange_symbol.multiplier
    )

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
        filled=filled_amount,
        price=str(Decimal(info["px"]) / exchange_symbol.multiplier) if info["px"] else "",
        cost=str(Decimal(avg_price) * Decimal(filled_amount)) if avg_price else "",
        average_price=avg_price,
        status=status,
    )

def normalize_binance_ws_order(info) -> Order:
    _type = OrderType.market
    if info["o"]["o"] in ["LIMIT"]:
        _type = OrderType.limit

    side = OrderSide.buy
    if info["o"]["S"] == "SELL":
        side = OrderSide.sell

    symbol = get_common_symbol_from_exchange_symbol(info["o"]["s"], "binance")
    exchange_symbol = get_exchange_symbol(symbol, 'binance')

    status = OrderStatus.new
    if info["o"]["X"] == "CANCELED":
        status = OrderStatus.canceled
    elif info["o"]["X"] in ["EXPIRED"]:
        status = OrderStatus.rejected
    elif info["o"]["X"] == "PARTIALLY_FILLED":
        status = OrderStatus.partially_filled
    elif info["o"]["X"] == "FILLED":
        status = OrderStatus.filled

    return Order(
        id=info["o"]["i"],
        order_client_id=info["o"]["c"],
        exchange="binance",
        timestamp=int(info["T"]),
        timestamp_str=ts_to_str(int(info["T"]) / 1000),
        last_trade_timestamp=int(info["o"]["T"]),
        type=_type,
        side=side,
        symbol=symbol,
        amount=str(
            Decimal(info["o"]["q"]) * exchange_symbol.multiplier
        ),
        filled=str(
            Decimal(info["o"]["z"]) * exchange_symbol.multiplier
        ),
        price=str(Decimal(info["o"]["p"])/exchange_symbol.multiplier),
        cost=str(Decimal(info["o"]["ap"]) * Decimal(info["o"]["z"])),
        average_price=str(Decimal(info["o"]["ap"])/exchange_symbol.multiplier),
        status=status,
    )


def normalize_binance_ccxt_order(info) -> Order:
    _type = OrderType.market
    if info["type"] == "limit":
        _type = OrderType.limit

    side = OrderSide.buy
    if info["side"] == "sell":
        side = OrderSide.sell

    symbol = get_common_symbol_from_ccxt(info["symbol"])
    exchange_symbol = get_exchange_symbol(symbol, 'binance')
    exchange_symbol_name = exchange_symbol.name
    symbol_info = exchanges['binance'].market(exchange_symbol_name)

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
        exchange='binance',
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
        average_price=str(Decimal(info["average"]) / exchange_symbol.multiplier) if info["average"] is not None else None,
        status=status,
    )


def normalize_common_ccxt_order(info, ex_name) -> Order:
    match ex_name:
        case 'okex':
            return normalize_okex_order(info['info'])
        case 'binance':
            return normalize_binance_ccxt_order(info)
        case _:
            raise ValueError(f"Unknown exchange: {ex_name}")
