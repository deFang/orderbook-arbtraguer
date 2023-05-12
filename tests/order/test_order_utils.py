import logging
from decimal import Decimal
from os.path import join

import pytest

from cross_arbitrage.fetch.utils.common import get_project_root
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.market import align_qty
from cross_arbitrage.order.model import OrderType, normalize_binance_ws_order
from cross_arbitrage.utils.exchange import (create_exchange,
                                            get_symbol_min_amount)
from cross_arbitrage.utils.order import (get_last_funding_rate,
                                         normalize_exchanges_order_qty,
                                         normalize_order_qty)
from cross_arbitrage.utils.symbol_mapping import (
    init_symbol_mapping_from_file, symbol_mapping)


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
    symbol = "APE/USDT"
    qty = "12.7"

    init_symbol_mapping_from_file(
        join(get_project_root(), "tests/fixtures/symbols.json")
    )

    okex = create_exchange(config.exchanges["okex"])
    binance = create_exchange(config.exchanges["binance"])

    okex_amount = normalize_order_qty(okex, symbol, qty)
    binance_amount = normalize_order_qty(binance, symbol, qty)

    assert str(okex_amount) == "12.7"
    assert str(binance_amount) == "12.0"


def test_align_qty(config):
    init_symbol_mapping_from_file(
        join(get_project_root(), "tests/fixtures/symbols.json")
    )

    okex = create_exchange(config.exchanges['okex'])
    okex.load_markets()

    binance = create_exchange(config.exchanges["binance"])
    binance.load_markets()

    # okex_amount = normalize_order_qty(okex, symbol, qty)
    binance_amount = align_qty(binance, "APE/USDT", Decimal("12.7"))
    assert str(binance_amount[0]) == "12"
    bnb_amount = align_qty(binance, "BNB/USDT", Decimal("0.31"))
    assert bnb_amount[0] == Decimal("0.31")

    # bug for PEPE in OKEX
    pepe_amount = align_qty(okex, "PEPE/USDT", Decimal("29660000"))
    assert pepe_amount[0] == Decimal("20000000")

    ape_amount = align_qty(okex, "APE/USDT", Decimal("11.39"))
    assert ape_amount[0] == Decimal("11.3")


def test_normalize_exchange_order_qty(config):
    symbol = "APE/USDT"
    qty = "12.7"

    init_symbol_mapping_from_file(
        join(get_project_root(), "tests/fixtures/symbols.json")
    )

    okex = create_exchange(config.exchanges["okex"])
    binance = create_exchange(config.exchanges["binance"])

    amount = normalize_exchanges_order_qty([okex, binance], symbol, qty)

    assert str(amount) == "12.0"

    bnb_amount = normalize_exchanges_order_qty(
        [okex, binance], "BNB/USDT", "0.31"
    )
    assert str(bnb_amount) == "0.31"
    ape_amount = normalize_exchanges_order_qty(
        [okex, binance], "APE/USDT", "12.5"
    )
    assert ape_amount == Decimal("12")


def test_get_symbol_min_amount(config):
    symbol = "AR/USDT"

    init_symbol_mapping_from_file(
        join(get_project_root(), "tests/fixtures/symbols.json")
    )

    okex = create_exchange(config.exchanges["okex"])
    binance = create_exchange(config.exchanges["binance"])

    min_amount = get_symbol_min_amount(
        {"okex": okex, "binance": binance}, symbol
    )

    assert str(min_amount) == "0.1"


def test_get_last_funding_rate(config):
    symbol = "BNB/USDT"
    res = get_last_funding_rate("okex", symbol, config)
    assert float(res["funding_rate"]) < 0.001

    res = get_last_funding_rate("binance", symbol, config)
    assert float(res["funding_rate"]) < 0.001


def test_normalize_binance_ws_order():
    order_info = {
        "e": "ORDER_TRADE_UPDATE",
        "T": 1683619707880,
        "E": 1683619707883,
        "o": {
            "s": "1000PEPEUSDT",
            "c": "ios_6Bfwp8Kpti6hzFF1FB6p",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "17",
            "p": "0.0041",
            "ap": "0",
            "sp": "0",
            "x": "NEW",
            "X": "NEW",
            "i": 138112379,
            "l": "0",
            "z": "0",
            "L": "0",
            "n": "0",
            "N": "USDT",
            "T": 1683619707880,
            "t": 0,
            "b": "2160",
            "a": "0",
            "m": False,
            "R": False,
            "wt": "CONTRACT_PRICE",
            "ot": "LIMIT",
            "ps": "BOTH",
            "cp": False,
            "rp": "0",
            "pP": False,
            "si": 0,
            "ss": 0,
        },
    }
    res = normalize_binance_ws_order(order_info)
    assert res.symbol == 'PEPE/USDT'
    assert res.type == OrderType.limit
    assert res.amount == "17000"
    assert res.price == "0.0000041"
