from os.path import join

from cross_arbitrage.fetch.config import get_config
from cross_arbitrage.fetch.utils.common import get_project_root


def test_get_common_config():
    config = get_config(
        file_path=[
            join(get_project_root(), "tests/fixtures/common_config.json"),
            join(get_project_root(), "tests/fixtures/symbols.json"),
        ],
        env="test",
    )
    assert config.env == "test"
    assert config.name == "fetch_cli"
    assert config.redis.url == "redis://localhost:6379/3"
    assert config.network.http_proxy == "http://localhost:1081"
    assert len(config.symbol_name_datas.keys()) == 12
