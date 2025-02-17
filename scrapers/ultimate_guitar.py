import json
import logging
import time
from collections.abc import Generator
from datetime import timedelta

import httpx
from bs4 import BeautifulSoup

from scrapers import models
from scrapers.utils import http_get

logger = logging.getLogger(__name__)
URL_PATTERN = "https://www.ultimate-guitar.com/explore?order=hitstotal_desc&page={page_number}&tuning[]=1&type[]=Chords"


def parse() -> Generator[models.SongChordsLink, None, None]:
    number_of_chords = 0
    with httpx.Client() as client:
        for page_number in range(1, 101):
            parsed_data_attribute = _get_page_data(client=client, page_number=page_number)
            chords: list[dict] = parsed_data_attribute["store"]["page"]["data"]["data"]["tabs"]
            hits: list[dict] = parsed_data_attribute["store"]["page"]["data"]["data"]["hits"]
            views = {hit["id"]: hit["hits"] for hit in hits}
            for chord in chords:
                number_of_chords += 1
                yield models.SongChordsLink(
                    artist=chord["artist_name"],
                    title=chord["song_name"],
                    url=chord["tab_url"],
                    version=chord["version"],
                    rating=chord["rating"],
                    votes=chord["votes"],
                    difficulty=chord["difficulty"] if chord["difficulty"] else None,
                    tonality_name=chord["tonality_name"] if chord["tonality_name"] else None,
                    views=views.get(chord["id"]),
                )
    logger.info("Number of urls to chords: %d", number_of_chords)


def _get_page_data(
    client: httpx.Client,
    page_number: int,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> dict:
    logger.info("Checking page number: %d (try: %d/%d)", page_number, try_number, max_tries)
    try:
        response = http_get(client=client, url=URL_PATTERN.format(page_number=page_number))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="html.parser")
        data_attribute = soup.find("body").select_one(".js-store")["data-content"]
        parsed_data_attribute: dict = json.loads(data_attribute)
        if not parsed_data_attribute["store"]["page"]["data"]["data"]:
            raise Exception("No data in response")
        return parsed_data_attribute
    except Exception as e:
        if try_number >= max_tries:
            raise e
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        return _get_page_data(page_number, try_number + 1, max_tries, wait_period)
