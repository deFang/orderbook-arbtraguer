import logging

import ccxt
import schedule
from cross_arbitrage.fetch.utils.common import now_ms

from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from cross_arbitrage.utils.exchange import create_exchange
from .config import AppConfig
from .model import init_db, ExchangeBalance


def collect_data(ctx: CancelContext, config: AppConfig):
    exchanges: dict[str, ccxt.Exchange] = {}

    for exchange_name, exchange_config in config.exchanges.items():
        if exchange_config.exchange_name is None:
            exchange_config.exchange_name = exchange_name
        exchanges[exchange_name] = create_exchange(exchange_config)

    schedule.every(config.collect_every_x_hours).hours.at(":00").do(
        worker, ctx, config, exchanges
    )

    schedule.run_all()
    while not ctx.is_canceled():
        schedule.run_pending()

        next_interval = schedule.idle_seconds()
        if not next_interval and next_interval < 10:
            next_interval = 10
        sleep_with_context(ctx, next_interval)


def worker(ctx: CancelContext, config: AppConfig, exchanges: dict[str, ccxt.Exchange]):
    now_ts = now_ms()

    logging.info("start collect data")

    rows = []
    for exchange_name, exchange in exchanges.items():
        while True:
            try:
                balance = exchange.fetch_balance()
                break
            except Exception as e:
                logging.error(f"fetch_balance failed: {e}")
                sleep_with_context(ctx, 1)
        row = {}
        match exchange:
            case ccxt.binanceusdm():
                info = balance["info"]
                total_cash_usd = info['totalWalletBalance']
                total_margin_usd = info['totalMarginBalance']
                pos_used_usd = info['totalPositionInitialMargin']
                pnl_usd = info['totalUnrealizedProfit']

                asset_infos = list(
                    filter(lambda x: x['asset'] == 'USDT', info['assets']))
                if len(asset_infos) > 0:
                    usdt_info = asset_infos[0]
                    row['total_cash_usdt'] = usdt_info['walletBalance']
                    row['total_margin_usdt'] = usdt_info['marginBalance']
            case ccxt.okex():
                if len(balance['info']['data']) == 0:
                    raise Exception("no data in okex balance")
                asset_infos = list(
                    filter(lambda x: x['ccy'] == 'USDT', balance['info']['data'][0]['details']))
                if len(asset_infos) == 0:
                    raise Exception("no USDT in okex balance")
                info = asset_infos[0]
                total_cash_usd = info['cashBal']
                total_margin_usd = info['eqUsd']
                pos_used_usd = info['frozenBal']
                pnl_usd = info['upl']
                row['total_cash_usdt'] = info['cashBal']
                row['total_margin_usdt'] = info['eq']
        row.update({
            'timestamp': now_ts,
            'exchange': exchange_name,
            'total_cash_usd': total_cash_usd,
            'total_margin_usd': total_margin_usd,
            'pos_used_usd': pos_used_usd,
            'pnl_usd': pnl_usd,
        })
        rows.append(row)
    ExchangeBalance.insert_many(rows).execute()
