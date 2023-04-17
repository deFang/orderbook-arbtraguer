import logging
from typing import Dict, List, Union

from pydantic import BaseModel, root_validator, validator

from cross_arbitrage.config.account import AccountConfig
from cross_arbitrage.config.constant import ENVS
from cross_arbitrage.config.log import LogConfig
from cross_arbitrage.config.network import NetworkConfig
from cross_arbitrage.config.redis import RedisConfig
from cross_arbitrage.fetch.utils.common import load_json_file, merge_dict_with_list_item_id


class FetchConfig(BaseModel):
    # common config
    env: str = "dev"
    log: LogConfig
    debug: bool = False
    redis: RedisConfig
    cross_arbitrage_symbol_datas: List[str] = []
    symbol_name_datas: Dict[str, Dict[str, str]] = {}
    network: NetworkConfig
    exchanges: Dict[str, AccountConfig]

    # fetch cli config
    name: str = "fetch_cli"
    worker_number: int = 2

    @validator("env")
    def env_must_in_list(cls, value):
        if value not in ENVS:
            raise ValueError(f"env must in {','.join(ENVS)}")
        return value

    @root_validator
    def update_exchange_name(cls, values):
        exchanges = values.get('exchanges')
        if exchanges and len(exchanges.keys()) > 0:
            for ex_name, account in exchanges.items():
                if not account.exchange_name:
                    account.exchange_name = ex_name
        # print('>>',exchanges)
        return values



    def print(self):
        logging.info(f"=> name:        {self.name}")
        logging.info(f"=> env:         {self.env}")
        logging.info(f"=> debug:       {self.debug}")
        logging.info(f"=> log_level:   {self.log.level}")
        logging.info(f"=> log_dir:     {self.log.dir}")
        logging.info(f"=> http_proxy:  {self.network.http_proxy}")
        logging.info(f"=> https_proxy: {self.network.https_proxy}")
        logging.info(f"=> exchanges:   {','.join(self.exchanges.keys())}")
        logging.info(f"=> len(symbols):{len(self.symbol_name_datas.keys())}")
        logging.info(f"=> len(enabled):{len(self.cross_arbitrage_symbol_datas)}")
        logging.info(f"=> redis:       {self.redis.url}")
        logging.info(f"=> ob stream:   {self.redis.orderbook_stream}")


    @classmethod
    def load(cls, obj):
        return cls.parse_obj(obj)


def get_config(file_path: Union[List[str], str], env="dev"):
    if type(file_path) == str:
        obj = load_json_file(file_path)
        obj["env"] = env
        return FetchConfig.load(obj)
    elif type(file_path) == list:
        json_objs = [load_json_file(filepath) for filepath in file_path]
        obj = merge_dict_with_list_item_id(*json_objs)
        obj["env"] = env
        return FetchConfig.load(obj)
    else:
        raise Exception(f"get_config(): invalid file_path {file_path}")
