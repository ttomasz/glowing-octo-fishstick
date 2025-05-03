import asyncio
import json
import logging
from collections.abc import AsyncGenerator, AsyncIterable, Generator
from datetime import timedelta
from string import ascii_lowercase

import httpx
from bs4 import BeautifulSoup
from tqdm.asyncio import tarange

from scrapers import models
from scrapers.utils import http_get_async

logger = logging.getLogger(__name__)
ARTIST_PAGES = (
    "0-9",
    *ascii_lowercase,
)
BANDS_URL_PATTERN = "https://www.ultimate-guitar.com/bands/{prefix}{page_number}.htm"


def parse() -> Generator[models.SongChordsLink, None, None]:
    logger.info("Starting...")
    results = asyncio.run(_gather_all())
    logger.info("Number of urls to chords: %d", len(results))
    yield from results
    logger.info("Done.")


async def _gather_all() -> list[models.SongChordsLink]:
    results = []
    semaphore = asyncio.Semaphore(4)  # Limit concurrent requests
    tasks = [_gather_chord_pages(semaphore=semaphore, prefix=prefix) for prefix in ARTIST_PAGES]
    results = [await x for x in asyncio.as_completed(tasks)]
    return [chord_page for group in results for chord_page in group]


async def _gather_chord_pages(semaphore: asyncio.Semaphore, prefix: str) -> list[models.SongChordsLink]:
    async with semaphore:
        artist_pages_urls = _get_artist_pages_urls(prefix=prefix)
        chord_pages = _get_chord_pages(artist_pages_urls=artist_pages_urls)
        results = [chord_page async for chord_page in chord_pages]
        return results


async def _get_chord_pages(artist_pages_urls: AsyncIterable[str]) -> AsyncGenerator[models.SongChordsLink, None]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async for url in artist_pages_urls:
            data = await _get_page_data_async(client=client, url=url)
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


async def _get_artist_pages_urls(prefix: str) -> AsyncGenerator[str, None]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async for url in _get_list_of_artist_pages_urls(client=client, prefix=prefix):
            data = await _get_page_data_async(client=client, url=url)
            data = data["store"]["page"]["data"]
            for artist in data["artists"]:
                if artist["tabscount"] > 0:
                    artist_page_url = f"https://www.ultimate-guitar.com{artist['artist_url']}"
                    yield artist_page_url


async def _get_list_of_artist_pages_urls(client: httpx.AsyncClient, prefix: str) -> AsyncGenerator[str, None]:
    first_page_url = BANDS_URL_PATTERN.format(prefix=prefix, page_number="")
    data = await _get_page_data_async(client=client, url=first_page_url)
    data = data["store"]["page"]["data"]
    num_pages = data["page_count"]
    yield first_page_url
    for page_number in tarange(2, num_pages + 1, unit="page", desc=f"Artist pages for prefix: [{prefix}]"):
        yield BANDS_URL_PATTERN.format(prefix=prefix, page_number=page_number)


async def _get_page_data_async(
    client: httpx.AsyncClient,
    url: str,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> dict:
    logger.debug("Checking page: %s (try: %d/%d)", url, try_number, max_tries)
    try:
        response = await http_get_async(client=client, url=url)
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
        await asyncio.sleep(wait_period.total_seconds())
        return await _get_page_data_async(
            client=client,
            url=url,
            try_number=try_number + 1,
            max_tries=max_tries,
            wait_period=wait_period,
        )
