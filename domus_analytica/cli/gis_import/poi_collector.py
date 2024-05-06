import logging
import time

import click
import googlemaps
import pymongo
from geojson import Point
from pymongo import MongoClient

log = logging.getLogger(__name__)


@click.option(
    "--points",
    "-p",
    required=True,
    type=str,
    help="Key points for searching, lng0,lat0;lng1,lat1;lng2,lat2;...",
)
@click.option(
    "--types",
    "-t",
    required=True,
    type=str,
    help="Types for searching, please check https://developers.google.com/places/supported_types",
)
@click.option(
    "--category",
    "-c",
    required=True,
    type=str,
    help="Category for saving to MongoDB",
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
@click.option(
    "--google-api-key",
    required=True,
    type=str,
    help="Google API key for Google Map",
    envvar="GOOGLE_API_KEY",
)
@click.option(
    "--radius",
    default=10000,
    type=int,
    help="Radius for searching",
)
def import_google_poi(
    points: str,
    types: str,
    category: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    google_api_key: str,
    radius: int,
):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    gmaps = googlemaps.Client(key=google_api_key)
    coll.create_index(
        [
            ("category", pymongo.ASCENDING),
            ("loc", pymongo.GEOSPHERE),
        ]
    )

    def places_nearby(**kwargs):
        result = gmaps.places_nearby(**kwargs)
        yield from result["results"]
        if kwargs.get("radius") is not None:
            while "next_page_token" in result:
                try:
                    result = gmaps.places_nearby(page_token=result["next_page_token"])
                    yield from result["results"]
                except Exception as e:
                    if str(e).find("INVALID_REQUEST") != -1:
                        time.sleep(1.0)
                    else:
                        raise e

    def doc_generator():
        location_types = [t for t in types.split(",") if t]
        if len(location_types) <= 0:
            raise ValueError("Should specify types")
        for lon, lat in (
            (float(x[0]), float(x[1]))
            for x in (s.split(",") for s in points.split(";"))
        ):
            log.info(f"Searching location {location_types} for ({lon},{lat})")
            for doc in places_nearby(
                location=(lat, lon),
                type=location_types,
                radius=radius,
                language="ja",
            ):
                yield doc["place_id"], {
                    "category": category,
                    "loc": Point(
                        coordinates=[
                            doc["geometry"]["location"]["lng"],
                            doc["geometry"]["location"]["lat"],
                        ]
                    ),
                    "data": doc,
                }

    docs_dict = dict(doc_generator())
    coll.delete_many({"category": category})
    coll.insert_many(docs_dict.values())
