import re
from typing import Optional

import numpy as np
import pandas as pd
import pymongo

from domus_analytica.config import DomusSettings
from domus_analytica.geopoint import GeoPoint


def extract_info_to_table(config: DomusSettings, suumo_filter: dict) -> pd.DataFrame:
    """
    The spider only did the basic information extraction, we need to convert them to usable values
    :param config: DomusSettings instance
    :param suumo_filter: To filter the data you want to use in suumo_details
    :return:
    """
    domus_db = pymongo.MongoClient(config.mongo_uri).get_database(config.mongo_db_name)
    suumo_details = domus_db.get_collection("suumo_details")

    mafia_points = [
        GeoPoint.from_lat_lng(**doc["geometry"]["location"])
        for doc in domus_db.get_collection("japan_mafia").find({}, {"geometry": 1})
    ]
    cemetery_points = [
        GeoPoint.from_lat_lng(**doc["geometry"]["location"])
        for doc in domus_db.get_collection("google_cemetery_related").find(
            {}, {"geometry": 1}
        )
    ]
    japan_gis_poi = domus_db.get_collection("japan_gis_poi")

    table_data = []

    for doc in suumo_details.find(suumo_filter):
        # Extract fields from doc
        id_url = doc["search_details"]["url"]
        result_doc = {"id": id_url}
        content_details = {d["type"]: d["content"] for d in doc["content_details"]}

        def get_first(regexp: str) -> Optional[str]:
            for d in doc["content_details"]:
                if re.match(regexp, d["type"]) is not None:
                    return d["content"]
            return None

        result_doc["name"] = content_details["物件名"]
        result_doc["address"] = content_details["住所"].split("\n")[0]

        if "gps" in doc:
            lat, lon = doc["gps"]["latitude"], doc["gps"]["longitude"]
            result_doc["lat"] = lat
            result_doc["lon"] = lon
        if "価格" in content_details:
            result_doc["price"] = float(
                re.findall("([+-]?([0-9]*[.])?[0-9]+)万円", content_details["価格"])[0][
                    0
                ]
            )

        if "専有面積" in content_details:
            sr = re.findall(
                "([+-]?([0-9]*[.])?[0-9]+)(m2|㎡)", content_details["専有面積"]
            )
            if sr:
                result_doc["exclusive_area"] = float(sr[0][0])
            else:
                raise ValueError(
                    "Can't get area from {}".format(content_details["専有面積"])
                )
        else:
            print(f"専有面積 can not be found in content_details of {id_url}")

        if "その他面積" in content_details:
            result_doc["common_area"] = sum(
                float(sr[0])
                for sr in re.findall(
                    "([+-]?([0-9]*[.])?[0-9]+)(m2|㎡)", content_details["その他面積"]
                )
            )
        else:
            print(f"その他面積 can not be found in content_details of {id_url}")

        completion_date = get_first(".*?(完成時期|築年月).*?")
        if completion_date:
            try:
                cd = re.findall(r"(\d{4})年(\d+)月", completion_date)[0]
                result_doc["completion_date"] = f"{int(cd[0])}-{int(cd[1]):02d}-01"
            except Exception as ex:
                print(f"Can't get time from {completion_date} in {content_details}")
                raise ex

        layout = content_details.get("間取り")
        if layout:
            result_doc["layout_main"] = re.findall(r"(\d(L|D|K)+)", layout)[0][0]
            storage_room = re.findall(r"\+(\d{0,1})S", layout)
            if len(storage_room) > 0:
                if storage_room[0] == "":
                    result_doc["layout_storage_room"] = 1
                else:
                    result_doc["layout_storage_room"] = int(storage_room[0])
            else:
                result_doc["layout_storage_room"] = 0

        direction = content_details.get("向き")
        if direction:
            result_doc["direction"] = direction

        result_doc["pet"] = (
            re.match("ペット", doc["search_details"]["title"]) is not None
        )
        the_floor = content_details.get("所在階", get_first("所在階"))
        if the_floor:
            result_doc["floor"] = int(re.findall("(\d+)階", the_floor)[0])

        total_floors = get_first(".*?階建.*?")
        if total_floors:
            try:
                result_doc["total_floors"] = int(
                    re.findall("(\d+)階建", total_floors)[0]
                )
            except Exception as ex:
                print(f"Can't parse {total_floors}")
                raise ex

        build_type = get_first(".*?構造.*?")
        if build_type:
            if build_type.find("木造") >= 0:
                result_doc["build_type"] = "wood"
            elif build_type.find("RC") >= 0:
                result_doc["build_type"] = "RC"
            else:
                result_doc["build_type"] = "unknown"

        for t in [
            "restaurant",
            "supermarket",
            "convenience_store",
            "drugstore",
            "park",
            "cafe",
            "bus_station",
            "primary_school",
        ]:
            if f"nearby_{t}" in doc:
                result_doc[f"{t}_count"] = len(doc[f"nearby_{t}"]["results"])

        if "gps" in doc:
            # Nearest Station
            this_location = GeoPoint.parse_obj(doc["gps"])
            if "nearby_train_station" in doc:
                station_location = doc["nearby_train_station"]["results"][0][
                    "geometry"
                ]["location"]
                result_doc["distance_to_nearest_station"] = (
                    GeoPoint(
                        latitude=station_location["lat"],
                        longitude=station_location["lng"],
                    )
                    - this_location
                )
            for p, n in [
                (
                    GeoPoint(latitude=33.59118086094799, longitude=130.398581611983),
                    "tenjin",
                ),
                (
                    GeoPoint(latitude=33.5873955705478, longitude=130.41968891935684),
                    "hakata",
                ),
                (
                    GeoPoint(latitude=33.59030230439562, longitude=130.37888950301377),
                    "ohori_park",
                ),
            ]:
                result_doc[f"distance_to_{n}"] = this_location - p

            result_doc["min_distance_to_mafia"] = min(
                this_location - p for p in mafia_points
            )
            result_doc["min_distance_to_cemetery"] = min(
                this_location - p for p in cemetery_points
            )

            # find nearest station and passenger count
            nearest_station = japan_gis_poi.find_one(
                {
                    "category": "station_passengers",
                    "loc": {
                        "$near": {
                            "$geometry": {
                                "type": "Point",
                                "coordinates": [
                                    this_location.longitude,
                                    this_location.latitude,
                                ],
                            },
                            "$maxDistance": 2000,
                        }
                    },
                }
            )
            if nearest_station and "passengers_count_2021" in nearest_station["data"]:
                station_location = GeoPoint(
                    longitude=nearest_station["loc"]["coordinates"][0],
                    latitude=nearest_station["loc"]["coordinates"][1],
                )
                result_doc["nearest_station_distance"] = (
                    this_location - station_location
                )
                result_doc["nearest_station_passengers"] = nearest_station["data"][
                    "passengers_count_2021"
                ]
                if (
                    "passengers_count_2019" in nearest_station["data"]
                    and nearest_station["data"]["passengers_count_2019"] > 0
                ):
                    result_doc["nearest_station_covid_ratio"] = (
                        nearest_station["data"]["passengers_count_2021"]
                        / nearest_station["data"]["passengers_count_2019"]
                    )
            # Estimate population density
            population_raw = np.array(
                [
                    doc["total_population"]
                    for doc in japan_gis_poi.find(
                        {
                            "category": "population",
                            "loc": {
                                "$near": {
                                    "$geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            this_location.longitude,
                                            this_location.latitude,
                                        ],
                                    },
                                    "$maxDistance": 1000,
                                }
                            },
                        }
                    )
                ]
            )
            result_doc["population_estimation_mean"] = population_raw.mean()
            result_doc["population_estimation_median"] = np.median(population_raw)

        def get_monthly_fee(key):
            text = content_details[key]
            total_value = 0
            for r in re.findall("((\d+)万){0,1}(\d+)円／月", text):
                total_value += float(r[2])
                if r[1] != "":
                    total_value += 10000 * float(r[1])
            return total_value

        result_doc["monthly_fee_manage"] = get_monthly_fee("管理費")
        result_doc["monthly_fee_repair"] = get_monthly_fee("修繕積立金")
        result_doc["monthly_fee_repair_fund"] = get_monthly_fee("修繕積立基金")
        result_doc["monthly_fee_others"] = get_monthly_fee("諸費用")
        result_doc["monthly_fee_total"] = sum(
            [
                result_doc["monthly_fee_manage"],
                result_doc["monthly_fee_repair"],
                result_doc["monthly_fee_repair_fund"],
                result_doc["monthly_fee_others"],
            ]
        )

        table_data.append(result_doc)

    return pd.DataFrame(table_data)
