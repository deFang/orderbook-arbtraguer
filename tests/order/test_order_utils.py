
from decimal import Decimal
from os.path import join
import pytest
from cross_arbitrage.fetch.utils.common import get_project_root

from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.market import align_qty
from cross_arbitrage.utils.exchange import create_exchange
from cross_arbitrage.utils.order import normalize_exchanges_order_qty, normalize_order_qty
from cross_arbitrage.utils.symbol_mapping import init_symbol_mapping_from_file


@pytest.fixture()
def config():
    yield get_config(
        file_path=[
            join(get_project_root(), "tests/fixtures/common_config.json"),
            join(get_project_root(), "tests/fixtures/symbols.json"),
            join(get_project_root(), "tests/fixtures/order_config.json"),
        ],
        env="test",
    )

def test_normalize_order_qty(config):
    symbol = "APE/USDT:USDT"
    qty = "12.7"

    okex = create_exchange(config.exchanges['okex'])
    binance = create_exchange(config.exchanges['binance'])

    okex_amount = normalize_order_qty(okex, symbol, qty)
    binance_amount = normalize_order_qty(binance, symbol, qty)

    assert str(okex_amount) == "12.7"
    assert str(binance_amount) == "12"

def test_align_qty(config):
    symbol = "APE/USDT"

    init_symbol_mapping_from_file(join(get_project_root(), "tests/fixtures/symbols.json"))
    # okex = create_exchange(config.exchanges['okex'])
    binance = create_exchange(config.exchanges['binance'])
    binance.load_markets()

    # okex_amount = normalize_order_qty(okex, symbol, qty)
    binance_amount = align_qty(binance, symbol, Decimal('12.7'))

    assert str(binance_amount[0]) == "12"
    bnb_amount = align_qty(binance, "BNB/USDT", Decimal('0.31'))
    assert bnb_amount[0] == Decimal('0.31')

def test_normalize_exchange_order_qty(config):
    symbol = "APE/USDT:USDT"
    qty = "12.7"

    okex = create_exchange(config.exchanges['okex'])
    binance = create_exchange(config.exchanges['binance'])

    amount = normalize_exchanges_order_qty([okex, binance], symbol, qty)

    assert str(amount) == "12"

    bnb_amount = normalize_exchanges_order_qty([okex, binance], "BNB/USDT:USDT", '0.31')
    assert str(bnb_amount) == "0.31"
