import logging
from collections.abc import Generator
from os import getenv

import dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

from scrapers import models

logger = logging.getLogger(__name__)


def extract() -> Generator[models.Song, None, None]:
    dotenv.load_dotenv(verbose=True)
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=getenv("SPOTIPY_CLIENT_ID"),
            client_secret=getenv("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=getenv("SPOTIPY_REDIRECT_URI"),
            scope="user-library-read",
        ),
    )
    yield from _get_liked_songs(client=sp)


def _get_liked_songs(client: spotipy.Spotify, offset: int = 0, limit: int = 50) -> Generator[models.Song, None, None]:
    results = client.current_user_saved_tracks(limit=limit, offset=offset)
    for item in results["items"]:
        track = item["track"]
        yield models.Song(
            artist=track["artists"][0]["name"],
            title=track["name"],
        )
    # if we had some results continue requesting next pages
    if len(results["items"]) > 0:
        yield from _get_liked_songs(client=client, offset=offset + limit, limit=limit)
