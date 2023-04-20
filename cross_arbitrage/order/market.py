from decimal import Decimal
from typing import Literal, Tuple

import ccxt
from pydantic import BaseModel

from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol


class SimpleMarginInfo(BaseModel):
    used: Decimal
    available: Decimal
    total_wallet: Decimal
    total_margin: Decimal


class DetailMarginInfo(BaseModel):
    total_maint_margin: Decimal | None
    total_margin_balance: Decimal
    total_wallet_balance: Decimal
    available_balance: Decimal
    position_init_margin: Decimal | None
    open_order_margin: Decimal | None
    total_used_margin: Decimal
    unrealized_pnl: Decimal


class MarginInfo(BaseModel):
    simple: SimpleMarginInfo
    detail: DetailMarginInfo


def get_margin_info(exchange: ccxt.Exchange) -> MarginInfo:
    match exchange:
        case ccxt.okex():
            # TODO
            pass
        case ccxt.binanceusdm():
            res = exchange.fetch_balance()

            position_init_margin = Decimal(
                res['info']['totalPositionInitialMargin'])
            open_order_margin = Decimal(
                res['info']['totalOpenOrderInitialMargin'])

            detail = DetailMarginInfo(
                total_position_maint_margin=Decimal(
                    res['info']['totalMaintMargin']),
                total_margin_balance=Decimal(
                    res['info']['totalMarginBalance']),
                total_wallet_balance=Decimal(
                    res['info']['totalWalletBalance']),
                total_used_margin=Decimal(res['info']['totalInitialMargin']),
                available_balance=Decimal(res['info']['availableBalance']),
                position_init_margin=position_init_margin,
                open_order_margin=open_order_margin,
                unrealized_pnl=Decimal(res['info']['totalUnrealizedProfit']),
            )
            simple = SimpleMarginInfo(
                used=detail.total_used_margin,
                position_maint=detail.total_maint_margin,
                available=detail.available_balance,
                total_wallet=detail.total_wallet_balance,
                total_margin=detail.total_margin_balance,
            )
            return MarginInfo(simple=simple, detail=detail)
        case _:
            raise ccxt.ExchangeNotAvailable(
                f'get margin info not support exchange: {type(exchange)}')


def place_order(exchange: ccxt.Exchange,
                symbol: str,
                side: Literal['sell'] | Literal['long'],
                qty: Decimal,
                method: str,
                price: Decimal = None,
                client_id=None,
                align_qty=True,
                reduce_only=False):
    ccxt_symbol = get_ccxt_symbol(symbol)

    params = {}
    if client_id:
        params['clientOrderId'] = client_id

    if reduce_only:
        params['reduceOnly'] = True

    # qty to market amount
    m = exchange.market(ccxt_symbol)
    amount = qty / Decimal(str(m['contractSize']))

    if align_qty:
        amount = exchange.amount_to_precision(ccxt_symbol, amount)

    match method:
        case 'market':
            return exchange.create_order(symbol=ccxt_symbol, type='market', side=side, amount=amount, params=params)
        case 'limit':
            return exchange.create_order(symbol=ccxt_symbol, type='limit', side=side, amount=amount, price=price, params=params)
        case 'maker_only':
            return exchange.create_post_only_order(symbol=ccxt_symbol, type='limit', side=side, amount=amount, price=price, params=params)


def market_order(exchange: ccxt.Exchange,
                 symbol: str,
                 side: Literal['sell'] | Literal['buy'],
                 qty: Decimal,
                 client_id=None,
                 align_qty=True,
                 reduce_only=False):
    return place_order(exchange, symbol, side, qty, 'market', client_id=client_id, align_qty=align_qty, reduce_only=reduce_only)


def maker_only_order(exchange: ccxt.Exchange,
                     symbol: str,
                     side: Literal['sell'] | Literal['buy'],
                     qty: Decimal,
                     price: Decimal,
                     client_id=None,
                     align_qty=True,
                     reduce_only=False):
    return place_order(exchange, symbol, side, qty, 'maker_only', price, client_id=client_id, align_qty=align_qty, reduce_only=reduce_only)


def cancel_order(exchange: ccxt.Exchange, order_id: str, symbol: str = None):
    if symbol:
        symbol = get_ccxt_symbol(symbol)
    return exchange.cancel_order(order_id, symbol)


def align_qty(exchange: ccxt.Exchange, symbol: str, qty: Decimal) -> Tuple[Decimal, Decimal]:
    ccxt_symbol = get_ccxt_symbol(symbol)
    match exchange:
        case ccxt.okex():
            contract_size = Decimal(
                str(exchange.market(ccxt_symbol)['contractSize']))
            r1 = qty.quantize(contract_size)
            r2 = contract_size - r1
            return r1, r2
        case ccxt.binanceusdm():
            market_precesion = exchange.market(
                ccxt_symbol)['precision']['amount']
            r1 = round(qty, market_precesion)
            r2 = qty - r1
            return r1, r2
        case _:
            raise ccxt.ExchangeNotAvailable(
                f'align qty not support exchange: {exchange.id}')


def get_contract_size(exchange: ccxt.Exchange, symbol: str) -> Decimal:
    ccxt_symbol = get_ccxt_symbol(symbol)
    return Decimal(str(exchange.market(ccxt_symbol)['contractSize']))


__ALL__ = [
    'place_order',
    'market_order',
    'maker_only_order',
    'cancel_order',
]
