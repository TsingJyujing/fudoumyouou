import json
import logging
import re
import time
from abc import ABC
from typing import Optional, Dict, Any, Iterable
from urllib.parse import urljoin

import pymongo
import requests
from bs4 import BeautifulSoup
from gridfs import GridFS

from domus_analytica.constants import USER_AGENT, XML_PARSER

log = logging.getLogger(__name__)


class BasePageCache(ABC):
    def get_cache(self, filename: str) -> Optional[bytes]:
        raise NotImplementedError("Please implement get_cache")

    def set_cache(self, filename: str, data: bytes):
        raise NotImplementedError("Please implement set_cache")


class MongoDBPageCache(BasePageCache):
    def __init__(self, mongo_uri: str, db_name: str):
        self.fs = GridFS(pymongo.MongoClient(mongo_uri).get_database(db_name))

    def get_cache(self, filename: str) -> Optional[bytes]:
        f = self.fs.find_one(dict(filename=filename))
        if f:
            return f.read()

    def set_cache(self, filename: str, data: bytes):
        self.fs.put(data, filename=filename)


class SuumoSpider:
    def __init__(
        self, cache: BasePageCache, wait_interval: float = 3.0, use_cache: bool = False
    ):
        self.use_cache = use_cache
        self.wait_interval = wait_interval
        self.cache = cache
        self.session = requests.session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.base_url = "https://suumo.jp"
        self._last_request = 0

    def get(self, url: str, *args, **kwargs):
        sleep_time = (self._last_request + self.wait_interval) - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)
        self._last_request = time.time()
        return self.session.get(urljoin(self.base_url, url), *args, **kwargs)

    def read_search_page(
        self,
        url: str,
        args: Dict[str, Any],
        page_id: int = 1,
        page_size: int = 100,
    ) -> bytes:
        # NOTE do not use cache unless debugging
        use_args = args.copy()
        use_args["pc"] = page_size
        use_args["page"] = page_id
        log.info(
            f"Reading searching result from suumo: {url} with params: {json.dumps(use_args)}"
        )
        resp = self.get(url, params=use_args)
        resp.raise_for_status()
        return resp.content

    def parse_search_page(self, content: bytes) -> Iterable[dict]:
        soup = BeautifulSoup(content, XML_PARSER)
        for property_unit in soup.find_all("div", attrs={"class": "property_unit"}):
            title = property_unit.find("h2", attrs={"class": "property_unit-title"})
            url = title.find("a").get("href")
            properties = [
                {
                    "key": kv_pair.find("dt").get_text(),
                    "value": kv_pair.find("dd").get_text(),
                }
                for kv_pair in property_unit.find_all("dl")
            ]
            yield {"url": url, "title": title.get_text(), "properties": properties}

    def get_page_count(self, content: bytes) -> int:
        soup = BeautifulSoup(content, XML_PARSER)
        numbers = [
            int(pagination_button.get_text())
            for pagination_button in soup.find(
                "ol", attrs={"class": "pagination-parts"}
            ).find_all("a")
        ]
        return max(numbers)

    def read_detail_page(self, url: str) -> bytes:
        """
        Read page content
        :param url: example: /ms/chuko/fukuoka/sc_fukuokashihakata/nc_74582921/
        :return:
        """
        if self.use_cache:
            cache_data = self.cache.get_cache(url)
            if cache_data:
                log.debug(f"Request to {url} hit cache.")
                return cache_data
            else:
                log.info(
                    f"Request to {url} missed cache, will query from server directly."
                )
        resp = self.get(url)
        resp.raise_for_status()
        self.cache.set_cache(url, resp.content)
        return resp.content

    def parse_detail_page(self, content: bytes) -> dict:
        content_text = content.decode()
        soup = BeautifulSoup(content, XML_PARSER)
        # TODO parse all info here
        # Get GPS data
        gps = {
            "latitude": re.findall(
                ",initIdo?.*?([+-]?([0-9]*[.])?[0-9]+)", content_text
            )[0][0],
            "longitude": re.findall(
                ",initKeido?.*?([+-]?([0-9]*[.])?[0-9]+)", content_text
            )[0][0],
        }
        nearby_places = [
            {
                "type": nearby.find_all("div", attrs={"class": "bgGreen"})[
                    0
                ].get_text(),
                "content": nearby.find_all("div", attrs={"class": "lh15"})[
                    0
                ].get_text(),
            }
            for nearby in soup.find_all("li", attrs={"class": "cf dibz vat"})
        ]
        detail_table = soup.find(
            "table", attrs={"class": "mt15 bdGrayT bdGrayL bgWhite pCell10 bdclps wf"}
        )
        content_details = []
        for th, td in zip(detail_table.find_all("th"), detail_table.find_all("td")):
            type_div = th.find("div", attrs={"class": "fl"})
            if type_div:
                _type = type_div.get_text()
            else:
                _type = th.get_text()

            content_details.append({"type": _type, "content": td.get_text().strip()})
        return {
            "gps": gps,
            "nearby_places": nearby_places,
            "content_details": content_details,
        }
