from decimal import Decimal
from flask import Flask
import orjson

from .model import ExchangeBalance

api_app = Flask(__name__)


@api_app.route('/get_balance')
def get_balance():
    balances = ExchangeBalance.select().order_by(ExchangeBalance.timestamp, ExchangeBalance.exchange).dicts()
    ls = []
    for balance in balances:
        ls.append({
            'timestamp': balance['timestamp'],
            'exchange': balance['exchange'],
            'total_cash_usd': balance['total_cash_usd'],
            'total_cash_usdt': balance['total_cash_usdt'],
            'total_margin_usd': balance['total_margin_usd'],
            'total_margin_usdt': balance['total_margin_usdt'],
            'pos_used_usd': balance['pos_used_usd'],
            'pnl_usd': balance['pnl_usd'],
        })
    return orjson.dumps({'datas': ls}, default=_json_default)


def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError