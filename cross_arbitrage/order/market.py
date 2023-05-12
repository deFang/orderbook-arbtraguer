from decimal import Decimal
import time
from typing import Any, Literal, Tuple

import ccxt
from pydantic import BaseModel
from cross_arbitrage.utils.exchange import get_bag_size

from cross_arbitrage.utils.symbol_mapping import get_ccxt_symbol, get_exchange_symbol_from_exchange


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
    exchange_symbol = get_exchange_symbol_from_exchange(exchange, symbol)
    exchange_symbol_name = exchange_symbol.name

    params = {}
    if client_id:
        params['clientOrderId'] = client_id

    if reduce_only:
        params['reduceOnly'] = True

    if price is not None:
        price *= exchange_symbol.multiplier

    # qty to market amount
    bag_size = get_bag_size(exchange, symbol)
    amount = qty / bag_size

    if align_qty:
        amount = exchange.amount_to_precision(exchange_symbol_name, amount)

    match method:
        case 'market':
            return exchange.create_order(symbol=exchange_symbol_name, type='market', side=side, amount=amount, params=params)
        case 'limit':
            return exchange.create_order(symbol=exchange_symbol_name, type='limit', side=side, amount=amount, price=price, params=params)
        case 'maker_only':
            return exchange.create_post_only_order(symbol=exchange_symbol_name, type='limit', side=side, amount=amount, price=price, params=params)


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
        symbol = get_exchange_symbol_from_exchange(exchange, symbol).name
    return exchange.cancel_order(order_id, symbol)


def align_qty(exchange: ccxt.Exchange, symbol: str, qty: Decimal) -> Tuple[Decimal, Decimal]:
    exchange_symbol = get_exchange_symbol_from_exchange(exchange, symbol)
    exchange_symbol_name = exchange_symbol.name
    match exchange:
        case ccxt.okex():
            bag_size = get_bag_size(exchange, symbol)
            print('bag_size',bag_size)
            # r1 = qty.quantize(bag_size)
            r2 = qty % bag_size
            r1 = qty - r2
            return r1, r2
        case ccxt.binanceusdm():
            # market_precesion = exchange.market(
            #     ccxt_symbol)['precision']['amount']
            r1 = Decimal(str(exchange.amount_to_precision(exchange_symbol_name, qty / exchange_symbol.multiplier))) * exchange_symbol.multiplier
            r2 = qty - r1
            return r1, r2
        case _:
            raise ccxt.ExchangeNotAvailable(
                f'align qty not support exchange: {exchange.id}')


def get_contract_size(exchange: ccxt.Exchange, symbol: str) -> Decimal:
    exchange_symbol = get_exchange_symbol_from_exchange(exchange, symbol)
    return Decimal(str(exchange.market(exchange_symbol.name)['contractSize']))


class ExchangeStatus(BaseModel):
    ok: bool
    status: Literal['ok'] | Literal['maintenance'] | Literal['error']
    msg: str


def check_exchange_status(exchange: ccxt.Exchange, retry=1) -> ExchangeStatus:
    if retry < 1:
        retry = 1

    match exchange:
        case ccxt.binanceusdm():
            while retry > 0:
                try:
                    status = exchange.fetch_status()
                    break
                except Exception as e:
                    retry -= 1
                    if retry <= 0:
                        return ExchangeStatus(ok=False, status='error', msg=str(e))

            if status['status'] != 'ok':
                return ExchangeStatus(ok=False, status='maintenance', msg=status['msg'])

            return ExchangeStatus(ok=True, status='ok', msg='')
        case ccxt.okex():
            while retry > 0:
                try:
                    status = exchange.publicGetSystemStatus()
                    break
                except Exception as e:
                    retry -= 1
                    if retry <= 0:
                        return ExchangeStatus(ok=False, status='error', msg=str(e))

            if status['code'] != '0':
                return ExchangeStatus(ok=False, status='error', msg=status['msg'])

            for s in status['data']:
                # return false if websocket and trading api is on maintenance
                if s['state'] == 'ongoing' and s['serviceType'] in ['0', '5', '8', '9']:
                    return ExchangeStatus(ok=False, status='maintenance', msg=s['title'])

            return ExchangeStatus(ok=True, status='ok', msg='')
            

__ALL__ = [
    'place_order',
    'market_order',
    'maker_only_order',
    'cancel_order',
]
