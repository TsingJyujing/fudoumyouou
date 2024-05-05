"""
不動産価格記録データを導入する
"""

import logging
import time

import click
import pymongo
import requests
from pymongo import MongoClient

log = logging.getLogger(__name__)


@click.option(
    "--reinfolib-api-key",
    required=True,
    type=str,
    help="API Key for https://www.reinfolib.mlit.go.jp/",
    envvar="REINFOLIB_API_KEY",
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
    default="japan_trading_api",
    type=str,
    help="MongoDB collection for saving data",
)
def download_trading_record(
    reinfolib_api_key: str, mongo_uri: str, mongo_db: str, mongo_coll: str
):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    coll.create_index([("param_year", pymongo.ASCENDING)])
    coll.create_index([("param_area", pymongo.ASCENDING)])
    coll.delete_many({})

    def download_data(area_code: str, year: int):
        resp = requests.get(
            "https://www.reinfolib.mlit.go.jp/ex-api/external/XIT001",
            params={"year": f"{year:04d}", "area": area_code},
            headers={"Ocp-Apim-Subscription-Key": reinfolib_api_key},
        )
        resp.raise_for_status()
        statues = resp.json()["status"]
        if statues != "OK":
            raise ValueError(f"Response status is not OK but {statues}")

        return coll.insert_many(
            (
                dict(param_year=year, param_area=area_code, **doc)
                for doc in resp.json()["data"]
            )
        ).acknowledged

    def retry_download(area_code: str, year: int):
        ex = Exception("Unknown error")
        for i in range(10):
            try:
                return download_data(area_code=area_code, year=year)
            except Exception as ex:
                log.error("Failed to download data", exc_info=ex)
                time.sleep(5)
        raise ex

    for y in range(2010, 2024):
        for a in (f"{x+1:02d}" for x in range(47)):
            log.info(f"Downloading data for area={a} year={y}")
            retry_download(a, y)
