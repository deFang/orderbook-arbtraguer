import logging
import ccxt

from cross_arbitrage.order.config import OrderConfig
from cross_arbitrage.utils.context import CancelContext, sleep_with_context
from .market import check_exchange_status, ExchangeStatus


_last_ok_mode = 'normal'
_maintain_mode = 'maintain'

def check_exchange_status_loop(ctx: CancelContext, config: OrderConfig, exchanges: dict[str, ccxt.Exchange]):
    global _last_ok_mode

    default_sleep_time = 30

    while not ctx.is_canceled():
        sleep_time = default_sleep_time
        
        current_mode = ctx.get('order_mode')
        if current_mode != _maintain_mode:
            _last_ok_mode = current_mode

        exchange_status: dict[str, ExchangeStatus] = {}
        for exchange_name, exchange in exchanges.items():
            exchange_status[exchange_name] = check_exchange_status(exchange)
        
        if current_mode == _maintain_mode:
            if all(status.ok for status in exchange_status.values()):
                logging.warn(f"change order_mode from maintain to {_last_ok_mode} mode: {exchange_status}")
                ctx.set('order_mode', _last_ok_mode)
            else:
                sleep_time = 120
        else:
            if any(not status.ok for status in exchange_status.values()):
                logging.warn(f"change order_mode from {current_mode} to maintain mode: {exchange_status}")
                ctx.set('order_mode', _maintain_mode)
                sleep_time = 120
        
        sleep_with_context(ctx, sleep_time)

