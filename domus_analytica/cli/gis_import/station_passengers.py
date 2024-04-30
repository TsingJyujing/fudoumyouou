import click
import geojson
import numpy as np
import pymongo
from pymongo import MongoClient

# https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-S12-2021.html
properties2field = {
    "S12_001": "station_name",
    "S12_001c": "station_code",
    "S12_002": "operation_company",
    "S12_003": "route_name",
    "S12_004": "railway_class_code",  # https://nlftp.mlit.go.jp/ksj/gml/codelist/RailwayClassCd.html
    "S12_005": "institution_type_code",  # https://nlftp.mlit.go.jp/ksj/gml/codelist/InstitutionTypeCd.html
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
)
@click.option(
    "--mongo-coll",
    default="japan_gis_poi",
    type=str,
    help="MongoDB collection for saving data",
)
def import_station_passengers(
    file: str, mongo_uri: str, mongo_db: str, mongo_coll: str
):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    coll.create_index(
        [
            ("category", pymongo.ASCENDING),
            ("loc", pymongo.GEOSPHERE),
        ]
    )
    category = "station_passengers"

    def document_loader():
        with open(file, "r") as fp:
            for geo_feature in geojson.load(fp).features:
                yield {
                    "category": category,
                    "loc": {
                        "type": "Point",
                        "coordinates": np.array(geo_feature.geometry.coordinates)
                        .mean(axis=0)
                        .tolist(),  # NOTE just a rough estimation
                    },
                    "raw_data": geo_feature.properties,
                    "data": dict(
                        **{
                            field_name: geo_feature.properties[k]
                            for k, field_name in properties2field.items()
                        },
                        **{
                            f"passengers_count_{year}": geo_feature.properties[
                                data_field
                            ]
                            for year, data_exist_field, data_field in (
                                (2011 + x, f"S12_{7+4*x:03d}", f"S12_{9+4*x:03d}")
                                for x in range(11)
                            )
                            if geo_feature.properties.get(data_exist_field) == 1
                        },
                    ),
                }

    coll.insert_many(document_loader())
