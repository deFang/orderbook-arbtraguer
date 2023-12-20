import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from os.path import abspath, basename, dirname, exists, join, splitext
from typing import Dict


# == lang
def merge_dict(dest: Dict, *args):
    for obj in args:
        for key, value in obj.items():
            if type(value) == dict:
                dest[key] = merge_dict(dest.get(key, {}), value)
            else:
                dest[key] = value
    return dest


# reduce orders/trades
def reduce_with_float_field(field, dict_list=[]):
    return float(
        reduce(
            lambda a, b: Decimal(str(a)) + Decimal(str(b[field])),
            dict_list,
            Decimal("0"),
        )
    )


# merge dict with list item id supoort
def merge_dict_with_list_item_id(dest: Dict, *args):
    for obj in args:
        for key, value in obj.items():
            if type(value) == dict:
                dest[key] = merge_dict(dest.get(key, {}), value)
            elif type(value) == list:
                if len(
                    list(
                        filter(
                            lambda x: type(x) == dict and x.get("id") != None,
                            value,
                        )
                    )
                ) == len(value):
                    for item in value:
                        index = -1
                        for idx, i in enumerate(dest[key]):
                            if item["id"] == i["id"]:
                                index = idx
                                break
                        if index == -1:
                            dest[key].append(item)
                        else:
                            dest[key][index] = merge_dict(
                                dest[key][index], item
                            )
                else:
                    dest[key].extend(value)
            else:
                dest[key] = value
    return dest


# == date time
def now_ms():
    return int(time.time() * 1000)


def now_s():
    return int(time.time())

def now():
    return time.time()

def to_utc_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def from_timestamp(ts: int):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def utc_now():
    return datetime.now(timezone.utc)


def to_utc(dt: datetime):
    return dt.astimezone(timezone.utc)


def ts_to_str(ts):
    return to_utc_str(from_timestamp(ts))

# def now_str(ts: float = None):
#     if ts is not None:
#         dt = datetime.datetime.fromtimestamp(ts).astimezone()
#     else:
#         dt = datetime.datetime.now()
#     return dt.strftime('%Y-%m-%d %H:%M:%S %z')

# == file and dir
def ensure_dir(file_path):
    if file_path == "":
        return
    if not exists(file_path):
        logging.info(f">> create dir: {file_path}")
        os.makedirs(file_path, exist_ok=True)


def ensure_file(file_path, content=""):
    ensure_dir(dirname(file_path))
    if not exists(file_path):
        logging.info(f">> create file: {file_path}")
        with open(file_path, "w") as f:
            f.write(content)


def base_name(file_path):
    # basename with removed file extension
    return splitext(basename(file_path))[0]


def get_project_root():
    file_path = abspath(dirname(__file__))
    res = file_path
    # file_path_parts = file_path.split(os.path.sep)
    while len(list(filter(lambda p: p != "", res.split(os.path.sep)))) > 1:
        if (not exists(join(res, "requirements.txt"))) and (
            not exists(join(res, "pyproject.toml"))
        ):
            res = dirname(res)
            continue
        else:
            return res
    raise Exception(
        f"get_root_root(): unable to find the project root for {file_path}"
    )


# load json file with line comment '//' support
def load_json_file(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
        new_lines = list(
            filter(lambda l: not l.strip().startswith("//"), lines)
        )
        contents = "\n".join(new_lines)
        return json.loads(contents)


def save_dictlist_to_csv(file_path, headers, dictlist, file_mode="a"):
    ensure_dir(dirname(file_path))
    file_exists = False
    if exists(file_path) and not file_mode.startswith('w'):
        file_exists = True
    filtered_dictlist = [
        dict(((key, obj[key]) for key in obj.keys() if key in headers))
        for obj in dictlist
    ]
    with open(file_path, file_mode) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        for rowdata in filtered_dictlist:
            writer.writerow(rowdata)


def save_to_json(file_path, obj, file_mode="a"):
    ensure_dir(dirname(file_path))
    with open(file_path, file_mode) as f:
        # ignore write for empty object like [] and {}
        if obj == [] or obj == {}:
            return
        json.dump(obj, f, indent=2)

