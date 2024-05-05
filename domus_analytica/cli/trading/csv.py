"""
不動産価格記録データを導入する
"""

import codecs
import logging
from _csv import QUOTE_ALL
from pathlib import Path

import click
import numpy as np
import pandas as pd
from pymongo import MongoClient

log = logging.getLogger(__name__)
FIELDS = [  # Thanks for ChatGPT!
    {"field": "種類", "unix_name": "type"},
    {"field": "価格情報区分", "unix_name": "price_info_type"},
    {"field": "地域", "unix_name": "region"},
    {"field": "市区町村コード", "unix_name": "city_code"},
    {"field": "都道府県名", "unix_name": "prefecture_name"},
    {"field": "市区町村名", "unix_name": "city_name"},
    {"field": "地区名", "unix_name": "district_name"},
    {"field": "最寄駅：名称", "unix_name": "nearest_station_name"},
    {"field": "最寄駅：距離（分）", "unix_name": "nearest_station_distance"},
    {"field": "取引価格（総額）", "unix_name": "total_transaction_price"},
    {"field": "坪単価", "unix_name": "price_per_tsubo"},
    {"field": "間取り", "unix_name": "layout"},
    {"field": "面積（㎡）", "unix_name": "area_in_sqm"},
    {"field": "取引価格（㎡単価）", "unix_name": "price_per_sqm"},
    {"field": "土地の形状", "unix_name": "land_shape"},
    {"field": "間口", "unix_name": "frontage"},
    {"field": "延床面積（㎡）", "unix_name": "floor_area_in_sqm"},
    {"field": "建築年", "unix_name": "year_built"},
    {"field": "建物の構造", "unix_name": "building_structure"},
    {"field": "用途", "unix_name": "usage"},
    {"field": "今後の利用目的", "unix_name": "future_usage"},
    {"field": "前面道路：方位", "unix_name": "road_orientation"},
    {"field": "前面道路：種類", "unix_name": "road_type"},
    {"field": "前面道路：幅員（ｍ）", "unix_name": "road_width_in_meters"},
    {"field": "都市計画", "unix_name": "urban_planning"},
    {"field": "建ぺい率（％）", "unix_name": "coverage_ratio"},
    {"field": "容積率（％）", "unix_name": "floor_area_ratio"},
    {"field": "取引時期", "unix_name": "transaction_period"},
    {"field": "改装", "unix_name": "renovation"},
    {"field": "取引の事情等", "unix_name": "transaction_conditions"},
]


@click.option(
    "--file",
    "-f",
    required=True,
    type=str,
    help="File or dir for trading CSV files",
)
@click.option(
    "--mongo-uri",
    required=True,
    type=str,
    help="MongoDB URI to connect to MongoDB database",
    envvar="MONGO_URI",
)
@click.option(
    "--mongo-db",
    default="domus",
    type=str,
    help="MongoDB database for saving data",
    envvar="MONGO_DB_NAME",
)
@click.option(
    "--mongo-coll",
    default="japan_trading",
    type=str,
    help="MongoDB collection for saving data",
)
def import_trading_record(file: str, mongo_uri: str, mongo_db: str, mongo_coll: str):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    coll.delete_many({})

    _path = Path(file)
    if _path.is_file():
        paths = [_path]
    elif _path.is_dir():
        paths = list(_path.glob("*.csv"))
    else:
        raise ValueError(f"Can't handle file/path {file}")

    def csv_reader(path: Path):
        with codecs.open(str(path), "rb", "cp932") as fp:
            # noinspection PyTypeChecker
            df = (
                pd.read_csv(
                    fp,
                    on_bad_lines="skip",
                )
                .rename(
                    columns={field["field"]: field["unix_name"] for field in FIELDS}
                )
                .replace([np.nan], [None])
            )
        for _, row in df.iterrows():
            yield {k: v for k, v in row.to_dict().items() if v is not None}

    for p in paths:
        log.info(f"Importing CSV file: {p}")
        if not coll.insert_many(csv_reader(p)).acknowledged:
            log.warning(f"Failed to insert CSV file: {p}")
