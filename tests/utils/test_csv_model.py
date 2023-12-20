import os
from typing import NamedTuple
from cross_arbitrage.utils.csv import CSVModel


def test_csv_model_write_obj():
    tmp_path = 'tmp'
    class TestModel(CSVModel):
        name: str
        age: int
        address: str | None
        data: dict | None = None

    path = tmp_path + "/test.csv"
    if os.path.exists(path):
        os.remove(path)
    data = [
        TestModel(name="Alice", age=20, address="Shanghai", data={"a": 123}),
        TestModel(name="Bob", age=30, address=None),
    ]

    header = TestModel.flatten_header(data[0])
    assert header == ['name', 'age', 'address', 'data']

    row = TestModel.flatten_data(data[0])
    assert row == {'name': 'Alice', 'age': 20, 'address': 'Shanghai', 'data': '{"a":123}'}
    row = TestModel.flatten_data(data[1])
    assert row == {'name': 'Bob', 'age': 30, 'address': None, 'data': None}

    TestModel.write_objs(path, data)
    
    assert os.path.exists(path)


def test_csv_model_write_obj2():
    tmp_path = 'tmp'
    class TestModel(CSVModel):
        age: int
        address: str | None
        data: dict | None = None

    class NamedTupleModel(NamedTuple):
        msg: str

    class People(CSVModel):
        name: str
        info: TestModel
        info2: NamedTupleModel

    path = tmp_path + "/test2.csv"
    if os.path.exists(path):
        os.remove(path)
    
    data = [
        People(name="Alice", info=TestModel(age=20, address="Shanghai", data={"a": 123}), info2=NamedTupleModel(msg="hello")),
        People(name="Bob", info=TestModel(age=30, address=None), info2=NamedTupleModel(msg="world")),
    ]

    header = TestModel.flatten_header(data[0])
    assert header == ['name', 'info.age', 'info.address', 'info.data', 'info2.msg']

    row = TestModel.flatten_data(data[0])
    assert row == {'name': 'Alice', 'info.age': 20, 'info.address': 'Shanghai', 'info.data': '{"a":123}', 'info2.msg': 'hello'}
    row = TestModel.flatten_data(data[1])
    assert row == {'name': 'Bob', 'info.age': 30, 'info.address': None, 'info.data': None, 'info2.msg': 'world'}

    for m in data:
        m.write(path)

    TestModel.write_objs(path, data)
    
    assert os.path.exists(path)


def test_csv_model_stat():
    from cross_arbitrage.order.signal_dealer import _Status, OrderDataModel, OrderSignal
    from decimal import Decimal

    tmp_path = 'tmp'
    path = tmp_path + "/test3.csv"
    if os.path.exists(path):
        os.remove(path)
    
    data: list[OrderDataModel] = [
        OrderDataModel(signal=OrderSignal('BTCUSDT', 'long', 'a', Decimal(1), Decimal(2), 'short', 'b', Decimal(3), 123, 1.1, None, False), 
        status=_Status(status='ok')),
    ]

    header = OrderDataModel.flatten_header(data[0])
    assert header == [
        'signal.symbol',
        'signal.maker_side',
        'signal.maker_exchange',
        'signal.maker_price',
        'signal.maker_qty',
        'signal.taker_side',
        'signal.taker_exchange',
        'signal.taker_price',
        'signal.orderbook_ts',
        'signal.cancel_order_threshold',
        'signal.maker_position.direction',
        'signal.maker_position.qty',
        'signal.maker_position.avg_price',
        'signal.maker_position.mark_price',
        'signal.is_reduce_position',
        'status.timestamp',
        'status.status',
        'status.order_id',
        'status.post_qty',
        'status.filled_qty',
        'status.post_price',
        'status.followed_qty',
        'status.processing_seconds',
    ]

    row = OrderDataModel.flatten_data(data[0])
    del row['status.timestamp']
    assert row == {
        'signal.symbol': 'BTCUSDT',
        'signal.maker_side': 'long',
        'signal.maker_exchange': 'a',
        'signal.maker_price': Decimal('1'),
        'signal.maker_qty': Decimal('2'),
        'signal.taker_side': 'short',
        'signal.taker_exchange': 'b',
        'signal.taker_price': Decimal('3'),
        'signal.orderbook_ts': 123,
        'signal.cancel_order_threshold': 1.1,
        'signal.maker_position.direction': None,
        'signal.maker_position.qty': None,
        'signal.maker_position.avg_price': None,
        'signal.maker_position.mark_price': None,
        'signal.is_reduce_position': False,
        'status.status': 'ok',
        'status.order_id': None,
        'status.post_qty': None,
        'status.filled_qty': None,
        'status.post_price': None,
        'status.followed_qty': None,
        'status.processing_seconds': None,
    }
