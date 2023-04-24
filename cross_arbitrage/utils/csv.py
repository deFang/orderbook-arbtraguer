from collections import namedtuple
from decimal import Decimal
import json
import os
import csv
import sys
from typing import NamedTuple, Type
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
    def _flatten_header_from_type(cls, typ) -> list[str]:
        fields = []

        fields_types = {}
        if issubclass(typ, pydantic.BaseModel):
            fields_types = {k: t.type_ for k, t in typ.__fields__.items()}
        elif issubclass(typ, tuple) and hasattr(typ, '_fields'):
            # NamedTuple
            fields_types = typ.__annotations__
        else:
            raise ParseError(
                f"_flatten_header_from_type: not supported type {typ}")

        for key, field in fields_types.items():
            if issubclass(field, pydantic.BaseModel) or (issubclass(field, tuple) and hasattr(field, '_fields')):
                fields.extend(
                    [f"{key}.{f}" for f in cls._flatten_header_from_type(field)])
            else:
                fields.append(key)

        return fields

    @classmethod
    def flatten_data(cls, data) -> dict:
        row = {}

        if isinstance(data, pydantic.BaseModel) or (isinstance(data, tuple) and hasattr(data, '_fields')):
            fields = {}
            match data:
                case pydantic.BaseModel():
                    fields = {k: getattr(data, k) for k in data.__fields__.keys()}
                case t if isinstance(t, tuple) and hasattr(t, '_fields'):
                    t: NamedTuple
                    fields = t._asdict()
                case _:
                    raise ParseError(
                        f"flatten_data: not supported type {type(data)}")

            for key, field in fields.items():
                match field:
                    case pydantic.BaseModel():
                        row.update(
                            {f"{key}.{f}": x for f, x in cls.flatten_data(field).items()})
                    case t if isinstance(t, tuple) and hasattr(t, '_fields'):
                        row.update(
                            {f"{key}.{f}": x for f, x in cls.flatten_data(t).items()})
                    case list() | dict():
                        row[key] = orjson.dumps(
                            field, default=_orjson_default).decode()
                    case _:
                        row[key] = field
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
