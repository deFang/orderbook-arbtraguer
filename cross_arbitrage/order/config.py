import logging
from decimal import Decimal
from typing import Dict, List, Union

from pydantic import BaseModel, root_validator, validator

from cross_arbitrage.config.account import AccountConfig
from cross_arbitrage.config.constant import ENVS, ORDER_MODES
from cross_arbitrage.config.log import LogConfig
from cross_arbitrage.config.network import NetworkConfig
from cross_arbitrage.config.redis import RedisConfig
from cross_arbitrage.config.symbol import SymbolConfig
from cross_arbitrage.fetch.utils.common import load_json_file, merge_dict


class OutputData(BaseModel):
    order_loop: str

class OrderConfig(BaseModel):
    env: str = "dev"
    log: LogConfig
    debug: bool = False
    order_mode: str = "normal"
    redis: RedisConfig
    network: NetworkConfig
    cross_arbitrage_symbol_datas: List[SymbolConfig] = []
    exchanges: Dict[str, AccountConfig]

    # order cli config
    name: str = "order_cli"
    exchange_pair_names: List[str] = ["binance", "okex"]
    default_makeonly_exchange_name: str = "okex"
    default_increase_position_threshold: float = 0.0012
    default_decrease_position_threshold: float = 0.0002
    default_cancel_increase_position_ratio: float = 0.75
    default_cancel_decrease_position_ratio: float = 0.25
    default_max_notional_per_order: float = 20.0
    default_max_notional_per_symbol: float = 100.0
    default_cancel_position_timeout: float = 120.0  # seconds
    max_used_margin: float = 0.9
    symbol_leverage: int = 2

    dry_run: bool = False

    output_data: OutputData

    @validator("env")
    def env_must_in_list(cls, value):
        if (value not in ENVS) and (not value.startswith('aa')):
            raise ValueError(f"env must in {','.join(ENVS)}")
        return value

    @validator("order_mode")
    def order_mode_must_in_list(cls, value):
        if value not in ORDER_MODES:
            raise ValueError(f"order mode must in {','.join(ORDER_MODES)}")
        return value

    @root_validator
    def update_exchange_name(cls, values):
        exchanges = values.get("exchanges")
        if exchanges and len(exchanges.keys()) > 0:
            for ex_name, account in exchanges.items():
                if not account.exchange_name:
                    account.exchange_name = ex_name
        # print('>>',exchanges)
        return values

    @root_validator
    def update_symbol_default_values(cls, values):
        symbol_datas = []
        if values.get("cross_arbitrage_symbol_datas"):
            symbol_datas = values["cross_arbitrage_symbol_datas"]
        if len(symbol_datas) > 0:
            for symbol in symbol_datas:
                if not symbol.makeonly_exchange_name:
                    symbol.makeonly_exchange_name = values[
                        "default_makeonly_exchange_name"
                    ]
                if not symbol.max_notional_per_order:
                    symbol.max_notional_per_order = values["default_max_notional_per_order"]
                if not symbol.max_notional_per_symbol:
                    symbol.max_notional_per_symbol = values["default_max_notional_per_symbol"]
                for index, threshold_data in enumerate([
                    symbol.long_threshold_data,
                    symbol.short_threshold_data,
                ]):
                    if index == 0:
                        if threshold_data.increase_position_threshold is None:
                            threshold_data.increase_position_threshold = -values[
                                "default_increase_position_threshold"
                            ]
                        if threshold_data.decrease_position_threshold is None:
                            threshold_data.decrease_position_threshold = -values[
                                "default_decrease_position_threshold"
                            ]
                    else:
                        if threshold_data.increase_position_threshold is None:
                            threshold_data.increase_position_threshold = values[
                                "default_increase_position_threshold"
                            ]
                        if threshold_data.decrease_position_threshold is None:
                            threshold_data.decrease_position_threshold = values[
                                "default_decrease_position_threshold"
                            ]
                    if threshold_data.cancel_increase_position_threshold is None:
                        threshold_data.cancel_increase_position_threshold = float(
                            (Decimal(
                                str(threshold_data.increase_position_threshold)
                            ) - Decimal(str(threshold_data.decrease_position_threshold)))
                            * Decimal(
                                str(values["default_cancel_increase_position_ratio"])
                            ) + Decimal(str(threshold_data.decrease_position_threshold))
                        )
                    if threshold_data.cancel_decrease_position_threshold is None:
                        threshold_data.cancel_decrease_position_threshold = float(
                            (Decimal(
                                str(threshold_data.increase_position_threshold)
                            ) - Decimal(str(threshold_data.decrease_position_threshold)))
                            * Decimal(
                                str(values["default_cancel_decrease_position_ratio"])
                            ) + Decimal(str(threshold_data.decrease_position_threshold))
                        )
                    if not threshold_data.cancel_position_timeout:
                        threshold_data.cancel_position_timeout = values["default_cancel_position_timeout"]
        return values

    def get_symbol_datas(self, symbol_name:str):
        symbols = list(filter(lambda d: d.symbol_name == symbol_name, self.cross_arbitrage_symbol_datas))
        if len(symbols) > 0:
            return symbols
        else:
            raise Exception(f"config.get_symbol_datas(): cannot find symbol data for {symbol_name}")

    def get_symbol_data_by_makeonly(self, symbol_name:str, makeonly_exchange_name:str):
        symbols = list(filter(lambda d: d.symbol_name == symbol_name and d.makeonly_exchange_name == makeonly_exchange_name, self.cross_arbitrage_symbol_datas))
        if len(symbols) > 0:
            return symbols[0]
        else:
            raise Exception(f"config.get_symbol_data_by_makeonly(): cannot find symbol data for {symbol_name} and {makeonly_exchange_name}")


    def print(self):
        logging.info(f"=> name:                    {self.name}")
        logging.info(f"=> env:                     {self.env}")
        logging.info(f"=> log_level:               {self.log.level}")
        logging.info(f"=> log_dir:                 {self.log.dir}")
        logging.info(f"=> exchanges:               {','.join(self.exchange_pair_names)}")
        logging.info(f"=> len(symbols):            {len(self.cross_arbitrage_symbol_datas)}")
        logging.info(f"=> order_mode:              {self.order_mode}")
        logging.info(f"=> symbol_leverage:         {self.symbol_leverage}")
        logging.info(f"=> max_margin_ratio:        {self.max_used_margin}")
        logging.info(f"=> max_notional_per_order:  {self.default_max_notional_per_order}")
        logging.info(f"=> max_notional_per_symbol: {self.default_max_notional_per_symbol}")
        for symbol_config in self.cross_arbitrage_symbol_datas:
            logging.info(f"=> symbol config :      {symbol_config}")

    @classmethod
    def load(cls, obj):
        return cls.parse_obj(obj)


def get_config(file_path: Union[List[str], str], env="dev"):
    if type(file_path) == str:
        obj = load_json_file(file_path)
        obj["env"] = env
        return OrderConfig.load(obj)
    elif type(file_path) == list:
        json_objs = [load_json_file(filepath) for filepath in file_path]
        obj = merge_dict(*json_objs)
        obj["env"] = env
        return OrderConfig.load(obj)
    else:
        raise Exception(f"get_config(): invalid file_path {file_path}")
