from decimal import Decimal
from typing import Literal, Tuple

import ccxt

from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol


def place_order(exchange: ccxt.Exchange,
                symbol: str,
                side: Literal['sell'] | Literal['long'],
                qty: Decimal,
                method: str,
                price: Decimal = None,
                client_id=None,
                align_qty=True):
    ccxt_symbol = get_ccxt_symbol(symbol)

    params = {}
    if client_id:
        params['clientOrderId'] = client_id

    # qty to market amount
    m = exchange.market(ccxt_symbol)
    amount = qty / Decimal(m['contractSize'])

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
                 side: Literal['sell'] | Literal['long'],
                 qty: Decimal,
                 client_id=None,
                 align_qty=True):
    return place_order(exchange, symbol, side, qty, 'market', client_id=client_id, align_qty=align_qty)


def maker_only_order(exchange: ccxt.Exchange,
                     symbol: str,
                     side: Literal['sell'] | Literal['long'],
                     qty: Decimal,
                     price: Decimal,
                     client_id=None,
                     align_qty=True):
    return place_order(exchange, symbol, side, qty, 'maker_only', price, client_id=client_id, align_qty=align_qty)


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


__ALL__ = [
    'place_order',
    'market_order',
    'maker_only_order',
    'cancel_order',
]
