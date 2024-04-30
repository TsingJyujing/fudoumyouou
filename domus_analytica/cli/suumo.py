import logging
from datetime import datetime
from typing import Iterable
from urllib.parse import parse_qs, urlparse

import click
import pymongo

from domus_analytica.config import DomusSettings
from domus_analytica.spider import SuumoSpider, MongoDBPageCache

log = logging.getLogger(__name__)


@click.option(
    "--search-url",
    required=True,
    type=str,
    help="URL for searching the houses",
)
@click.option(
    "--wait-interval",
    type=float,
    default=3.0,
    show_default=True,
    help="Wait x seconds before calling the API",
)
@click.option("--detailed", is_flag=True, help="Download detailed data")
@click.option("--use-cache", is_flag=True, help="Use cache for downloading data")
def download_from_suumo(
    search_url: str, wait_interval: float, detailed: bool, use_cache: bool
):
    config = DomusSettings()
    domus_db = pymongo.MongoClient(config.mongo_uri).get_database(config.mongo_db_name)
    suumo_search = domus_db.get_collection("suumo_search")
    suumo_details = domus_db.get_collection("suumo_details")
    spider = SuumoSpider(
        cache=MongoDBPageCache(config.mongo_uri, db_name=config.mongo_db_name),
        wait_interval=wait_interval,
        use_cache=use_cache,
    )
    search_url_parse = urlparse(search_url)
    # Query:
    query = parse_qs(search_url_parse.query)
    search_time = datetime.now()

    def _iter_details(current_page_id: int) -> Iterable[dict]:
        _content = spider.read_search_page(
            search_url_parse.path, query, page_id=current_page_id, page_size=100
        )
        total_pages = spider.get_page_count(_content)
        yield from spider.parse_search_page(_content)
        if total_pages > current_page_id:
            yield from _iter_details(current_page_id + 1)

    for i, item_detail in enumerate(_iter_details(1)):
        query_details = {
            "search_url": search_url_parse.path,
            "search_args": query,
            "rank_order": i,
            "create_time": datetime.now(),
            "search_time": search_time,
        }
        if detailed:
            item_detail_page_parsed = spider.parse_detail_page(
                spider.read_detail_page(item_detail["url"])
            )
            item_detail_page_parsed["search_details"] = item_detail
            item_detail_page_parsed.update(query_details)
            suumo_details.insert_one(item_detail_page_parsed)
        else:
            item_detail_save = item_detail.copy()
            item_detail_save.update(query_details)
            suumo_search.insert_one(item_detail_save)
