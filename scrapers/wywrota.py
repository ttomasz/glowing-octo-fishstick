import logging
import string
import time
from collections.abc import Generator
from datetime import timedelta
from typing import NamedTuple

import httpx
from bs4 import BeautifulSoup

from scrapers import models
from scrapers.utils import http_get

logger = logging.getLogger(__name__)
ARTISTS_URL_PATTERN = "https://spiewnik.wywrota.pl/country/PL/letter/{letter}/artists"


class ArtistPage(NamedTuple):
    artist_name: str
    artist_page_url: str


class SongPage(NamedTuple):
    instrument_icon: str
    song_name: str
    song_url: str


def parse() -> Generator[models.SongChordsLink, None, None]:
    with httpx.Client() as client:
        for letter in string.ascii_uppercase:
            for ap in _get_artists_pages(client=client, letter=letter):
                songs_with_tabs = 0
                songs_without_tabs = 0
                songs_pages = _get_songs_pages(client=client, artist_page_url=ap.artist_page_url)
                for sp in songs_pages:
                    if sp.instrument_icon == "Gitara":
                        songs_with_tabs += 1
                        yield models.SongChordsLink(
                            artist=ap.artist_name,
                            title=sp.song_name,
                            url=sp.song_url,
                        )
                    else:
                        songs_without_tabs += 1
                logger.info(
                    "Artist: %s - songs with tabs: %d, songs without tabs: %d",
                    ap.artist_name,
                    songs_with_tabs,
                    songs_without_tabs,
                )


def _get_artists_pages(
    client: httpx.Client,
    letter: str,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> Generator[ArtistPage, None, None]:
    logger.info("Checking artists starting with letter: %s (try: %d/%d)", letter, try_number, max_tries)
    try:
        response = http_get(client=client, url=ARTISTS_URL_PATTERN.format(letter=letter))
        soup = BeautifulSoup(response.text, features="html.parser")
        section = soup.find("body").find("section").find_all("div", class_="row")
        artists = section[1].find_all("a")
        if not artists:
            raise Exception("No artists in response")
        for artist in artists:
            artist_name = artist.text.strip()
            artist_page_url = artist["href"].strip()
            yield ArtistPage(artist_name=artist_name, artist_page_url=artist_page_url)
    except Exception as e:
        if try_number > max_tries:
            raise e
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        yield from _get_artists_pages(
            letter=letter, try_number=try_number + 1, max_tries=max_tries, wait_period=wait_period,
        )


def _get_songs_pages(
    client: httpx.Client,
    artist_page_url: str,
    try_number: int = 1,
    max_tries: int = 5,
    wait_period: timedelta = timedelta(seconds=30),
) -> Generator[SongPage, None, None]:
    try:
        response_artist_page = http_get(client=client, url=artist_page_url)
        soup_artist_page = BeautifulSoup(response_artist_page.text, features="html.parser")
        songs_list = soup_artist_page.select_one(".song-list-group").find_all("li")
        for song in songs_list:
            instrument_icon = song.find("span")["title"]
            song_name = song.find("a").text.strip()
            song_url = song.find("a")["href"]
            yield SongPage(instrument_icon=instrument_icon, song_name=song_name, song_url=song_url)
    except Exception as e:
        if try_number >= max_tries:
            raise e
        logger.exception("Error while processing page: %s. Retrying in %s.", e, wait_period)
        time.sleep(wait_period.total_seconds())
        yield from _get_songs_pages(
            artist_page_url=artist_page_url, try_number=try_number + 1, max_tries=max_tries, wait_period=wait_period,
        )
