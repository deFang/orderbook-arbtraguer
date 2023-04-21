from decimal import Decimal
import json
import os
import csv
import sys
from typing import Type
import orjson

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import pydantic


class ParseError(Exception):
    pass


class CSVModel(pydantic.BaseModel):

    @classmethod
    def flatten_header(cls, data: pydantic.BaseModel) -> list[str]:
        return cls._flatten_header_from_type(type(data))

    @classmethod
    def _flatten_header_from_type(cls, typ: Type[pydantic.BaseModel]) -> list[str]:
        fields = []
        if issubclass(typ, pydantic.BaseModel):
            for key, field in typ.__fields__.items():
                if issubclass(field.type_, pydantic.BaseModel):
                    fields.extend(
                        [f"{key}.{f}" for f in cls._flatten_header_from_type(field.type_)])
                else:
                    fields.append(key)
        else:
            raise ParseError(
                f"flatten_header: not supported type {typ}")
        return fields

    @classmethod
    def flatten_data(cls, data: pydantic.BaseModel) -> dict:
        row = {}

        if isinstance(data, pydantic.BaseModel):
            for key, field in data.__fields__.items():
                value = getattr(data, key)
                match value:
                    case pydantic.BaseModel():
                        row.update(
                            {f"{key}.{f}": x for f, x in cls.flatten_data(value).items()})
                    case list() | dict():
                        row[key] = orjson.dumps(
                            value, default=_orjson_default).decode()
                    case _:
                        row[key] = value
        else:
            raise ParseError(
                f"flatten_data: not supported type {type(data)}")

        return row

    @classmethod
    def write_objs(cls,
                   path,
                   datas: list[pydantic.BaseModel],
                   fields: list[str] | None = None,
                   mkdir=True,
                   write_header_when_create: bool = True,
                   write_header_when_empty: bool = True):

        if len(datas) == 0:
            return

        if mkdir and not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        write_header = False
        if write_header_when_create and not os.path.exists(path):
            write_header = True

        if fields is None:
            fields = cls.flatten_header(datas[0])

        with open(path, 'a+', encoding='utf-8') as f:
            if write_header_when_empty and os.stat(path).st_size == 0:
                write_header = True
            writer = csv.DictWriter(f, fields, lineterminator='\n')
            if write_header:
                writer.writeheader()
            rows = [cls.flatten_data(data) for data in datas]
            writer.writerows(rows)

    def write(self, path, fields: list[str] | None = None, mkdir=True, write_header_when_create: bool = True, write_header_when_empty: bool = True):
        self.write_objs(path, [self], fields, mkdir,
                        write_header_when_create, write_header_when_empty)


def _orjson_default(obj):
    match obj:
        case Decimal():
            return float(obj)
        case _:
            raise TypeError
