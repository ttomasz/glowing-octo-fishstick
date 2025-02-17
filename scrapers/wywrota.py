import logging
import string
import time
from collections.abc import Generator
from datetime import timedelta
from http import HTTPStatus

import httpx
from bs4 import BeautifulSoup

from scrapers import models

logger = logging.getLogger(__name__)
ARTISTS_URL_PATTERN = "https://spiewnik.wywrota.pl/country/PL/letter/{letter}/artists"


def parse() -> Generator[models.SongChordsLink, None, None]:
    for letter in string.ascii_uppercase:
        for artist_name, artist_page_url in _get_artists_pages(letter=letter):
            songs_with_tabs = 0
            songs_without_tabs = 0
            for instrument_icon, song_name, song_url in _get_songs_pages(artist_page_url=artist_page_url):
                if instrument_icon == "Gitara":
                    songs_with_tabs += 1
                    yield models.SongChordsLink(
                        artist=artist_name,
                        title=song_name,
                        url=song_url,
                    )
                else:
                    songs_without_tabs += 1
            logger.info(
                "Artist: %s - songs with tabs: %d, songs without tabs: %d",
                artist_name,
                songs_with_tabs,
                songs_without_tabs,
            )


def _get_artists_pages(
    letter: str,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> Generator[tuple[str, str], None, None]:
    logger.info("Checking artists starting with letter: %s (try: %d/%d)", letter, try_number, max_tries)
    try:
        response = httpx.get(
            url=ARTISTS_URL_PATTERN.format(letter=letter),
            timeout=30,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="html.parser")
        section = soup.find("body").find("section").find_all("div", class_="row")
        artists = section[1].find_all("a")
        if not artists:
            raise Exception("No artists in response")
        for artist in artists:
            artist_name = artist.text.strip()
            artist_page_url = artist["href"].strip()
            yield artist_name, artist_page_url
    except Exception as e:
        if try_number > max_tries:
            raise e
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            retry_after = e.response.headers.get("Retry-After")
            if retry_after:
                logger.error("Server sent 429 status code with Retry-After header value: %s", retry_after)
                wait_period = timedelta(seconds=int(retry_after))
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        yield from _get_artists_pages(
            letter=letter, try_number=try_number + 1, max_tries=max_tries, wait_period=wait_period,
        )


def _get_songs_pages(
    artist_page_url: str, try_number: int = 1, max_tries: int = 5, wait_period: timedelta = timedelta(seconds=30),
) -> Generator[tuple[str, str, str], None, None]:
    try:
        response_artist_page = httpx.get(
            url=artist_page_url,
            timeout=30,
        )
        response_artist_page.raise_for_status()
        soup_artist_page = BeautifulSoup(response_artist_page.text, features="html.parser")
        songs_list = soup_artist_page.select_one(".song-list-group").find_all("li")
        for song in songs_list:
            instrument_icon = song.find("span")["title"]
            song_name = song.find("a").text.strip()
            song_url = song.find("a")["href"]
            yield instrument_icon, song_name, song_url
    except Exception as e:
        if try_number >= max_tries:
            raise e
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            retry_after = e.response.headers.get("Retry-After")
            if retry_after:
                logger.error("Server sent 429 status code with Retry-After header value: %s", retry_after)
                wait_period = timedelta(seconds=int(retry_after))
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        yield from _get_songs_pages(
            artist_page_url=artist_page_url, try_number=try_number + 1, max_tries=max_tries, wait_period=wait_period,
        )
