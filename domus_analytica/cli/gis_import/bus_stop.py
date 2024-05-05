import logging
from pathlib import Path

import click
import geojson
import pymongo
from pymongo import MongoClient

log = logging.getLogger(__name__)
# https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P11-2022.html
properties2field = {
    "P11_001": "station_name",
    "P11_002": "operation_company",
    "P11_005": "comment",
    # P11_003_01～35 バス路線の系統番号・系統名。
    # P11_004_01～35 バス路線の運行形態による区分
}


@click.option(
    "--file",
    "-f",
    required=True,
    type=str,
    help="Filepath of geojson file containing station and passengers data",
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
    default="japan_gis_poi",
    type=str,
    help="MongoDB collection for saving data",
)
def import_bus_stops(file: str, mongo_uri: str, mongo_db: str, mongo_coll: str):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    coll.create_index(
        [
            ("category", pymongo.ASCENDING),
            ("loc", pymongo.GEOSPHERE),
        ]
    )
    category = "bus_stop"

    def document_loader(current_file: Path):
        with open(current_file, "r") as fp:
            for geo_feature in geojson.load(fp).features:
                routes = []
                for i in range(35):
                    route_name_field = f"P11_003_{i+1:02d}"
                    route_type_field = f"P11_004_{i+1:02d}"
                    if geo_feature.properties[route_name_field]:
                        routes.append(
                            [
                                {"route_name": route_name, "route_type": route_type}
                                for route_name, route_type in zip(
                                    geo_feature.properties[route_name_field].split(","),
                                    geo_feature.properties[route_type_field].split(","),
                                )
                            ]
                        )

                yield {
                    "category": category,
                    "loc": geo_feature.geometry,
                    "raw_data": geo_feature.properties,
                    "data": dict(
                        **{
                            field_name: geo_feature.properties[k]
                            for k, field_name in properties2field.items()
                        },
                        routes=routes,
                    ),
                }

    coll.delete_many({"category": category})
    for f in Path(file).glob("*/*.geojson"):
        log.info(f"Importing data from {f}")
        coll.insert_many(document_loader(f))
