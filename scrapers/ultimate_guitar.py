import dataclasses
import json
import logging
import sqlite3
import time
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path
from string import ascii_lowercase

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from scrapers import models
from scrapers.utils import http_get

logger = logging.getLogger(__name__)
ARTIST_PAGES = (
    "0-9",
    *ascii_lowercase,
)
BANDS_URL_PATTERN = "https://www.ultimate-guitar.com/bands/{prefix}{page_number}.htm"


def parse(db_path: Path | None = None) -> Generator[models.SongChordsLink, None, None]:
    logger.info("Starting...")
    conn_path = db_path if db_path is not None else ":memory:"
    db = sqlite3.connect(conn_path, isolation_level=None)
    try:
        cursor = db.execute("pragma journal_mode=wal")
        cursor.execute("CREATE TABLE IF NOT EXISTS urls(url TEXT NOT NULL, parsed TEXT)")
        cursor.execute("SELECT count(*) as cnt FROM urls")
        numbers_of_urls_in_db = cursor.fetchone()[0]
        if numbers_of_urls_in_db > 0:
            logger.info("Number of urls in db: %d", numbers_of_urls_in_db)
        else:
            logger.info("No urls in db. Starting from scratch.")
            cursor.execute("BEGIN")
            for prefix in tqdm(ARTIST_PAGES, unit="prefix", desc="Artist pages prefixes"):
                artist_pages_urls = _get_artist_pages_urls(prefix=prefix)
                for url in artist_pages_urls:
                    cursor.execute("INSERT INTO urls(url) VALUES (?)", (url,))
            db.commit()
            cursor.execute("SELECT count(*) as cnt FROM urls")
            numbers_of_urls_in_db = cursor.fetchone()[0]
            logger.info("Number of urls in db: %d", numbers_of_urls_in_db)
        cursor.execute("SELECT count(*) as cnt FROM urls WHERE parsed IS NULL")
        numbers_of_urls_to_parse = cursor.fetchone()[0]
        if numbers_of_urls_to_parse > 0:
            logger.info("Number of urls to parse: %d", numbers_of_urls_to_parse)
            cursor.execute("SELECT url, rowid FROM urls WHERE parsed IS NULL")
            urls_to_parse = cursor.fetchall()
            cursor.execute("BEGIN")
            with httpx.Client(follow_redirects=True) as client:
                for i, row in tqdm(enumerate(urls_to_parse), total=len(urls_to_parse), unit="url", desc="Parsing urls"):
                    url, rowid = row
                    parsed = [dataclasses.asdict(c) for c in _get_chord_page(client=client, artist_page_url=url)]
                    cursor.execute("UPDATE urls SET parsed = ? WHERE rowid = ?", (json.dumps(parsed), rowid))
                    if i % 1_000 == 0:
                        logger.debug("Committing. (i: %d)", i)
                        db.commit()
                db.commit()
        cursor.execute("SELECT parsed FROM urls")
        logger.info("Yielding content from db...")
        for _ in tqdm(range(numbers_of_urls_in_db), unit="row"):
            content = cursor.fetchone()[0]
            parsed_content: list[dict] = json.loads(content)
            for entry in parsed_content:
                tab = models.SongChordsLink(**entry)
                yield tab
        logger.info("Done.")
    finally:
        db.close()


def _get_chord_page(client: httpx.Client, artist_page_url: str) -> Generator[models.SongChordsLink, None, None]:
    data = _get_page_data(client=client, url=artist_page_url)
    data = data["store"]["page"]["data"]
    for tab in data["other_tabs"]:
        if (
            tab.get("marketing_type", "") not in ("TabPro", "official")
            and tab["type"] == "Chords"
            and tab["tuning"] == "Standard"
        ):
            yield models.SongChordsLink(
                artist=tab["artist_name"],
                title=tab["song_name"],
                url=tab["tab_url"],
                version=tab["version"],
                rating=tab["rating"] if tab["rating"] else None,
                votes=tab["votes"] if tab["votes"] else None,
                difficulty=tab["difficulty"] if tab["difficulty"] else None,
                tonality_name=tab["tonality_name"] if tab["tonality_name"] else None,
            )


def _get_artist_pages_urls(prefix: str) -> Generator[str, None]:
    with httpx.Client(follow_redirects=True) as client:
        for url in _get_list_of_artist_pages_urls(client=client, prefix=prefix):
            data = _get_page_data(client=client, url=url)
            data = data["store"]["page"]["data"]
            for artist in data["artists"]:
                if artist["tabscount"] > 0:
                    artist_page_url = f"https://www.ultimate-guitar.com{artist['artist_url']}"
                    yield artist_page_url


def _get_list_of_artist_pages_urls(client: httpx.Client, prefix: str) -> Generator[str, None]:
    first_page_url = BANDS_URL_PATTERN.format(prefix=prefix, page_number="")
    data = _get_page_data(client=client, url=first_page_url)
    data = data["store"]["page"]["data"]
    num_pages = data["page_count"]
    yield first_page_url
    for page_number in tqdm(
        iterable=range(2, num_pages + 1),
        unit="page",
        desc=f"Artist pages for prefix: [{prefix}]",
        total=num_pages,
        initial=1,
    ):
        yield BANDS_URL_PATTERN.format(prefix=prefix, page_number=page_number)


def _get_page_data(
    client: httpx.Client,
    url: str,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> dict:
    logger.debug("Checking page: %s (try: %d/%d)", url, try_number, max_tries)
    try:
        response = http_get(client=client, url=url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="html.parser")
        data_attribute: str = soup.find("body").select_one(".js-store")["data-content"]  # type: ignore  # noqa: PGH003
        parsed_data_attribute: dict = json.loads(data_attribute)
        if not parsed_data_attribute["store"]["page"]["data"]:
            raise Exception("No data in response")
        return parsed_data_attribute
    except Exception as e:
        if try_number >= max_tries:
            raise e
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        return _get_page_data(
            client=client,
            url=url,
            try_number=try_number + 1,
            max_tries=max_tries,
            wait_period=wait_period,
        )
