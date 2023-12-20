import pydantic

from cross_arbitrage.config.account import AccountConfig
from cross_arbitrage.fetch.utils.common import load_json_file, merge_dict_with_list_item_id


class AppConfig(pydantic.BaseModel):
    exchanges: dict[str, AccountConfig]
    sqlite_path: str
    collect_every_x_hours: int


def get_config(file_path: list[str] | str, env="dev"):
    if type(file_path) == str:
        obj = load_json_file(file_path)
        obj["env"] = env
        return AppConfig.parse_obj(obj)
    elif type(file_path) == list:
        json_objs = [load_json_file(filepath) for filepath in file_path]
        obj = merge_dict_with_list_item_id(*json_objs)
        obj["env"] = env
        return AppConfig.parse_obj(obj)
    else:
        raise Exception(f"get_config(): invalid file_path {file_path}")
