import logging
import time

import click
import googlemaps
import pandas as pd
import pymongo
from geojson import Point
from pymongo import MongoClient

log = logging.getLogger(__name__)

mafia_df = pd.DataFrame(
    {
        "名称": {
            1: "六代目山口組",
            2: "稲川会",
            3: "住吉会",
            4: "五代目工藤會",
            5: "旭琉會",
            6: "七代目会津小鉄会",
            7: "六代目共政会",
            8: "七代目合田一家",
            9: "四代目小桜一家",
            10: "五代目浅野組",
            11: "道仁会",
            12: "二代目親和会",
            13: "双愛会",
            14: "三代目侠道会",
            15: "太州会",
            16: "十代目酒梅組",
            17: "極東会",
            18: "二代目東組",
            19: "松葉会",
            20: "四代目福博会",
            21: "浪川会\u3000\u3000\u3000",
            22: "神戸山口組",
            23: "絆會",
            24: "関東関根組\xa0",
            25: "池田組",
        },
        "主たる事務所の所在地": {
            1: "兵庫県神戸市灘区篠原本町４－３－１",
            2: "東京都港区六本木７－８－４",
            3: "東京都港区赤坂６－４－２１",
            4: "福岡県北九州市小倉北区宇佐町１－８－８",
            5: "沖縄県中頭郡北中城村字島袋１３６２",
            6: "京都府京都市左京区一乗寺塚本町２１－４",
            7: "広島県広島市南区南大河町１８－１０",
            8: "山口県下関市竹崎町３－１３－６",
            9: "鹿児島県鹿児島市甲突町９－２４",
            10: "岡山県笠岡市笠岡６１５－１１",
            11: "福岡県久留米市京町２４７－６",
            12: "香川県高松市塩上町２－１４－４",
            13: "千葉県市原市潤井戸１３４３－８",
            14: "広島県尾道市山波町３０２５－１",
            15: "福岡県田川市大字弓削田１３１４－１",
            16: "大阪府大阪市西成区太子１－３－１７",
            17: "東京都新宿区歌舞伎町２－１８－１２",
            18: "大阪府大阪市西成区山王１－１１－８",
            19: "東京都台東区西浅草２－９－８",
            20: "福岡県福岡市博多区千代５－１８－１５",
            21: "福岡県大牟田市八江町３８－１",
            22: "兵庫県神戸市中央区二宮町３－１０－７",
            23: "兵庫県尼崎市戸ノ内町３－３２－６",
            24: "茨城県土浦市桜町４－１０－１３",
            25: "岡山県岡山市北区田町２－１２－２",
        },
        "代表する者": {
            1: "篠田\u3000建市",
            2: "辛\u3000炳圭",
            3: "小川\u3000修",
            4: "野村\u3000悟",
            5: "永山\u3000克博",
            6: "金\u3000元",
            7: "荒瀬\u3000進",
            8: "金\u3000教煥",
            9: "平岡\u3000喜榮",
            10: "中岡\u3000豊",
            11: "小林\u3000哲治",
            12: "𠮷良\u3000博文",
            13: "椎塚\u3000宣",
            14: "池澤\u3000望",
            15: "日高\u3000博",
            16: "李\u3000正秀",
            17: "髙橋\u3000仁",
            18: "滝本\u3000博司",
            19: "伊藤\u3000義克",
            20: "金\u3000國泰",
            21: "朴\u3000政浩",
            22: "井上\u3000邦雄",
            23: "金\u3000禎紀",
            24: "大塚\u3000逸男",
            25: "金\u3000孝志",
        },
    }
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
def import_mafia(
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    google_api_key: str,
):
    coll = MongoClient(mongo_uri).get_database(mongo_db).get_collection(mongo_coll)
    gmaps = googlemaps.Client(key=google_api_key)
    coll.create_index(
        [
            ("category", pymongo.ASCENDING),
            ("loc", pymongo.GEOSPHERE),
        ]
    )
    category = "mafia"

    def doc_generator():
        for index, row in mafia_df.iterrows():
            address = row["主たる事務所の所在地"]
            for doc in gmaps.geocode(address=address, language="ja", region="ja"):
                yield {
                    "category": category,
                    "loc": Point(
                        coordinates=[
                            doc["geometry"]["location"]["lng"],
                            doc["geometry"]["location"]["lat"],
                        ]
                    ),
                    "data": doc,
                }

    coll.delete_many({"category": category})
    coll.insert_many(doc_generator())
