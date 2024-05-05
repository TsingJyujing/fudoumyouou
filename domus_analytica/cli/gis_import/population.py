import logging
from pathlib import Path

import click
import jismesh.utils as ju
import numpy as np
import pandas as pd
import pymongo
from geojson import Point
from pymongo import MongoClient

log = logging.getLogger(__name__)
FIELDS = [
    {"field": "T001142001", "unix_name": "total_population"},
    {"field": "T001142002", "unix_name": "total_population_male"},
    {"field": "T001142003", "unix_name": "total_population_female"},
    {"field": "T001142004", "unix_name": "population_0_14_total"},
    {"field": "T001142005", "unix_name": "population_0_14_male"},
    {"field": "T001142006", "unix_name": "population_0_14_female"},
    {"field": "T001142007", "unix_name": "population_15_above_total"},
    {"field": "T001142008", "unix_name": "population_15_above_male"},
    {"field": "T001142009", "unix_name": "population_15_above_female"},
    {"field": "T001142010", "unix_name": "population_15_64_total"},
    {"field": "T001142011", "unix_name": "population_15_64_male"},
    {"field": "T001142012", "unix_name": "population_15_64_female"},
    {"field": "T001142013", "unix_name": "population_18_above_total"},
    {"field": "T001142014", "unix_name": "population_18_above_male"},
    {"field": "T001142015", "unix_name": "population_18_above_female"},
    {"field": "T001142016", "unix_name": "population_20_above_total"},
    {"field": "T001142017", "unix_name": "population_20_above_male"},
    {"field": "T001142018", "unix_name": "population_20_above_female"},
    {"field": "T001142019", "unix_name": "population_65_above_total"},
    {"field": "T001142020", "unix_name": "population_65_above_male"},
    {"field": "T001142021", "unix_name": "population_65_above_female"},
    {"field": "T001142022", "unix_name": "population_75_above_total"},
    {"field": "T001142023", "unix_name": "population_75_above_male"},
    {"field": "T001142024", "unix_name": "population_75_above_female"},
    {"field": "T001142025", "unix_name": "population_85_above_total"},
    {"field": "T001142026", "unix_name": "population_85_above_male"},
    {"field": "T001142027", "unix_name": "population_85_above_female"},
    {"field": "T001142028", "unix_name": "population_95_above_total"},
    {"field": "T001142029", "unix_name": "population_95_above_male"},
    {"field": "T001142030", "unix_name": "population_95_above_female"},
    {"field": "T001142031", "unix_name": "foreign_population_total"},
    {"field": "T001142032", "unix_name": "foreign_population_male"},
    {"field": "T001142033", "unix_name": "foreign_population_female"},
    {"field": "T001142034", "unix_name": "total_households"},
    {"field": "T001142035", "unix_name": "general_households"},
    {"field": "T001142036", "unix_name": "one_person_households_general"},
    {"field": "T001142037", "unix_name": "two_person_households_general"},
    {"field": "T001142038", "unix_name": "three_person_households_general"},
    {"field": "T001142039", "unix_name": "four_person_households_general"},
    {"field": "T001142040", "unix_name": "five_person_households_general"},
    {"field": "T001142041", "unix_name": "six_person_households_general"},
    {"field": "T001142042", "unix_name": "seven_person_above_households_general"},
    {"field": "T001142043", "unix_name": "family_only_households_general"},
    {"field": "T001142044", "unix_name": "nuclear_family_households_general"},
    {"field": "T001142045", "unix_name": "non_nuclear_family_households_general"},
    {"field": "T001142046", "unix_name": "households_with_under_6_years_general"},
    {"field": "T001142047", "unix_name": "households_with_65_above_members_general"},
    {"field": "T001142048", "unix_name": "one_person_households_20_29_general"},
    {"field": "T001142049", "unix_name": "elderly_living_alone_households_general"},
    {"field": "T001142050", "unix_name": "elderly_couple_households_general"},
]


@click.option(
    "--file",
    "-f",
    required=True,
    type=str,
    help="Filepath of txt file containing population data",
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
def import_population_grid_data(
    file: str, mongo_uri: str, mongo_db: str, mongo_coll: str
):

    _path = Path(file)
    if _path.is_file():
        paths = [_path]
    elif _path.is_dir():
        paths = list(_path.glob("*.txt"))
    else:
        raise ValueError(f"Can't handle file/path {file}")

    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    coll.create_index(
        [
            ("category", pymongo.ASCENDING),
            ("loc", pymongo.GEOSPHERE),
        ]
    )
    category = "population"

    def document_loader(file_path: Path):
        df = (
            pd.read_csv(file_path, encoding="cp932", skiprows=[1])
            .rename(columns={r["field"]: r["unix_name"] for r in FIELDS})
            .replace({np.nan: None, "*": None})
        )
        number_fields = {x["unix_name"] for x in FIELDS}
        for _, row in df.iterrows():
            try:
                lat, lng = ju.to_meshpoint(int(row["KEY_CODE"]), 0.5, 0.5)
                yield {
                    "loc": Point((lng, lat)),
                    "category": category,
                    "data": {
                        k: (int(v) if k in number_fields and v is not None else v)
                        for k, v in row.to_dict().items()
                    },
                }
            except ValueError as e:
                log.error(f"failed to process GPS in row: {row}", exc_info=e)
                raise e

    log.info("Cleaning expired data...")
    rows_deleted = coll.delete_many({"category": category}).deleted_count
    log.info(f"{rows_deleted} rows were deleted")
    for p in paths:
        log.info(f"Importing from file {p}")
        if not coll.insert_many(document_loader(p)).acknowledged:
            log.warning(f"Failed to load data from {p}")
