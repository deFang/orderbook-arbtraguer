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