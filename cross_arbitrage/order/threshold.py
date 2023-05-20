from decimal import Decimal
import logging
import pprint
import time
import orjson
import pydantic
import redis

from cross_arbitrage.utils.context import CancelContext, sleep_with_context

from .config import OrderConfig

_default_threshold_redis_fmt = 'order:thresholds:{makeonly_exchange}'


def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


class ThresholdConfig(pydantic.BaseModel):
    increase_position_threshold: Decimal
    decrease_position_threshold: Decimal
    cancel_increase_position_threshold: Decimal
    cancel_decrease_position_threshold: Decimal


class SymbolConfig(pydantic.BaseModel):
    long_threshold: ThresholdConfig
    short_threshold: ThresholdConfig

    def json_bytes(self):
        return orjson.dumps(self.dict(), default=_json_default)


class Threshold:
    rc: redis.Redis
    redis_key: str
    config: OrderConfig

    def __init__(self, config, rc,
                 key=None,
                 key_fmt=_default_threshold_redis_fmt,
                 makeonly_exchange='okex'):
        self.config = config
        self.rc = rc
        if key is None:
            key = key_fmt.format(makeonly_exchange=makeonly_exchange)
        self.redis_key = key
        self.makeonly_exchange = makeonly_exchange
        self.symbol_thresholds: dict[str, SymbolConfig] = {}

    def refresh_thresholds(self):
        redis_thresholds = self.rc.hgetall(self.redis_key)

        dyn_thresholds: dict[str, SymbolConfig] = {}

        for symbol_name, symbol_threshold in redis_thresholds.items():
            symbol_threshold = SymbolConfig.parse_obj(
                orjson.loads(symbol_threshold))
            symbol_name = symbol_name.decode()
            dyn_thresholds[symbol_name] = symbol_threshold

        for c in filter(lambda d: d.makeonly_exchange_name == self.makeonly_exchange,
                        self.config.cross_arbitrage_symbol_datas):
            if c.symbol_name not in dyn_thresholds:
                self.symbol_thresholds[c.symbol_name] = SymbolConfig(
                    long_threshold=ThresholdConfig(
                        increase_position_threshold=Decimal(
                            str(c.long_threshold_data.increase_position_threshold)),
                        decrease_position_threshold=Decimal(
                            str(c.long_threshold_data.decrease_position_threshold)),
                        cancel_increase_position_threshold=Decimal(
                            str(c.long_threshold_data.cancel_increase_position_threshold)),
                        cancel_decrease_position_threshold=Decimal(
                            str(c.long_threshold_data.cancel_decrease_position_threshold)),
                    ),
                    short_threshold=ThresholdConfig(
                        increase_position_threshold=Decimal(
                            str(c.short_threshold_data.increase_position_threshold)),
                        decrease_position_threshold=Decimal(
                            str(c.short_threshold_data.decrease_position_threshold)),
                        cancel_increase_position_threshold=Decimal(
                            str(c.short_threshold_data.cancel_increase_position_threshold)),
                        cancel_decrease_position_threshold=Decimal(
                            str(c.short_threshold_data.cancel_decrease_position_threshold)),
                    ),
                )
            else:
                self.symbol_thresholds[c.symbol_name] = dyn_thresholds[c.symbol_name]

    def refresh_loop(self, ctx: CancelContext, interval=1):
        count = 0
        while not ctx.is_canceled():
            start_time = time.time()
            try:
                self.refresh_thresholds()
            except Exception as e:
                logging.error('[mo_ex: {}] refresh_thresholds error: {}'.format(self.makeonly_exchange, e))
                logging.exception(e)

            if self.config.debug:
                if count % 300 == 0:
                    logging.info('[mo_ex: {}] thresholds: {}'.format(self.makeonly_exchange, pprint.pformat(self.symbol_thresholds)))
                count += 1
            sleep_time = interval - (time.time() - start_time)
            sleep_with_context(ctx, sleep_time)

    def get_symbol_thresholds(self, symbol_name: str) -> SymbolConfig:
        ret = self.symbol_thresholds.get(symbol_name, None)
        if ret is None:
            c = self.config.get_symbol_data_by_makeonly(
                symbol_name, self.makeonly_exchange)
            ret = SymbolConfig(
                long_threshold=ThresholdConfig(
                    increase_position_threshold=Decimal(
                        str(c.long_threshold_data.increase_position_threshold)),
                    decrease_position_threshold=Decimal(
                        str(c.long_threshold_data.decrease_position_threshold)),
                    cancel_increase_position_threshold=Decimal(
                        str(c.long_threshold_data.cancel_increase_position_threshold)),
                    cancel_decrease_position_threshold=Decimal(
                        str(c.long_threshold_data.cancel_decrease_position_threshold)),
                ),
                short_threshold=ThresholdConfig(
                    increase_position_threshold=Decimal(
                        str(c.short_threshold_data.increase_position_threshold)),
                    decrease_position_threshold=Decimal(
                        str(c.short_threshold_data.decrease_position_threshold)),
                    cancel_increase_position_threshold=Decimal(
                        str(c.short_threshold_data.cancel_increase_position_threshold)),
                    cancel_decrease_position_threshold=Decimal(
                        str(c.short_threshold_data.cancel_decrease_position_threshold)),
                ),
            )
        return ret
