from decimal import Decimal
from os.path import join

import pytest

from cross_arbitrage.fetch.utils.common import get_project_root
from cross_arbitrage.order.config import get_config
from cross_arbitrage.order.order_book import OrderSignal
from cross_arbitrage.order.position_status import (PositionDirection,
                                                   PositionStatus)
from cross_arbitrage.order.signal_dealer import should_cancel_makeonly_order
from cross_arbitrage.utils.context import CancelContext


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


def test_should_cancel_makeonly_order(config):
    assert config.env == "test"
    ctx = CancelContext()
    maker_ob = {
        "ts": 1681900800000,
        "asks": [
            ["325.32", 9],
            ["325.33", 5],
            ["325.34", 2],
        ],
        "bids": [
            ["325.31", 5],
            ["325.30", 2],
            ["325.29", 6],
        ],
    }
    taker_ob = {"ts": 1681900800000, "asks": [], "bids": []}
    maker_symbol_position = PositionStatus(
        direction=PositionDirection.long, qty=Decimal(8.2)
    )
    signal = OrderSignal(
        symbol="BNB/USDT",
        maker_side="buy",
        maker_exchange="okex",
        maker_price=Decimal("325.31"),
        maker_qty=Decimal(5.0),
        taker_side="sell",
        taker_exchange="binance",
        taker_price=Decimal("324.40"),
        orderbook_ts=maker_ob["ts"],
        cancel_order_threshold=0.00002,
        maker_position=maker_symbol_position,
        is_reduce_position=False,
    )

    assert (
        should_cancel_makeonly_order(
            ctx,
            config,
            signal,
            taker_ob,
            need_depth_qty=Decimal(100),
            bag_size=Decimal(1),
        )
        == True
    )
    taker_ob = {
        "ts": 1681900800000,
        "asks": [
            ["325.39", 19],
            ["325.38", 7],
            ["325.37", 6],
        ],
        "bids": [
            ["325.36", 7],
            ["325.35", 5],
            ["325.34", 8],
            # ----------- threshold line
        ],
    }
    assert (
        should_cancel_makeonly_order(
            ctx,
            config,
            signal,
            taker_ob,
            need_depth_qty=Decimal(100),
            bag_size=Decimal(1),
        )
        == False
    )

    taker_ob = {
        "ts": 1681900800000,
        "asks": [
            ["325.39", 19],
            ["325.38", 7],
            ["325.37", 6],
        ],
        "bids": [
            ["325.36", 2],
            ["325.35", 1.2],
            ["325.34", 1],
            ["325.33", 1],
            ["325.32", 1],
            ["325.31", 1],
            # ----------- threshold line
            ["325.30", 1],
            ["325.29", 1],
        ],
    }
    assert (
        should_cancel_makeonly_order(
            ctx,
            config,
            signal,
            taker_ob,
            need_depth_qty=Decimal(str(7.2)),
            bag_size=Decimal(1),
        )
        == False
    )
    assert (
        should_cancel_makeonly_order(
            ctx,
            config,
            signal,
            taker_ob,
            need_depth_qty=Decimal(str(7.3)),
            bag_size=Decimal(1),
        )
        == True
    )
