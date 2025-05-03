import logging
import re
from collections.abc import Generator
from os import getenv

import dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm

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
    logger.info("Retrieving liked songs from Spotify...")
    total_songs = sp.current_user_saved_tracks(limit=1)["total"] # type: ignore  # noqa: PGH003
    limit = 50
    for i in tqdm(range(0, total_songs + 1, limit), desc="Fetching liked songs", unit="batch"):
        yield from _get_liked_songs(client=sp, offset=i, limit=limit)
    logger.info("Done.")


def _get_liked_songs(client: spotipy.Spotify, offset: int = 0, limit: int = 50) -> Generator[models.Song, None, None]:
    logger.debug("Requesting liked songs from Spotify (offset=%d, limit=%d)...", offset, limit)
    results = client.current_user_saved_tracks(limit=limit, offset=offset)
    assert results is not None  # noqa: S101
    for item in results["items"]:
        track = item["track"]
        title: str = track["name"]
        title = re.sub(r" \(?\d{4} Remaster\)?", "", title)
        title = re.sub(r" - Remastered \d{4}", "", title)
        title = re.sub(r" - Version \d{4}", "", title)
        title = re.sub(r" - \d{4} Version", "", title)
        title = re.sub(r" - \d{4} Remix", "", title)
        title = re.sub(r" - \d{4} Digital Remaster", "", title)
        title = re.sub(r" - \d{4} Remastered Version", "", title)
        title = re.sub(r" \(feat\. [\w &,\.]+\)", "", title, flags=re.IGNORECASE)
        title = re.sub(" -ed$", "", title)
        title = (
            title
            .replace(' - 12" Version', "")
            .replace(' - Special 12" Dance Mix', "")
            .replace(" - (Original Single Mono Version)", "")
            .replace(" - 7 inch", "")
            .replace(" - Acoustic", "")
            .replace(" - Edit", "")
            .replace(" - Extended Version", "")
            .replace(" - Full Length Version", "")
            .replace(" - Instrumental Version", "")
            .replace(" - Live", "")
            .replace(" - New Stereo Mix", "")
            .replace(" - Original Album Version", "")
            .replace(" - Original Mix", "")
            .replace(" - Radio Edit", "")
            .replace(" - Radio Version", "")
            .replace(" - Re-mastered", "")
            .replace(" - Remaster", "")
            .replace(" - Remastered Version", "")
            .replace(" - Remastered", "")
            .replace(" - Remix", "")
            .replace(" - Single Edit", "")
            .replace(" - Single Mix", "")
            .replace(" - Single Version", "")
            .replace(" - Soundtrack Version", "")
            .replace(" -ed Version", "")
            .replace(" (Avicii Remix)", "")
            .replace(" (Digitally Remastered)", "")
            .replace(" (Live)", "")
            .replace(" (Radio Edit)", "")
            .replace(" (Single Version)", "")
            .replace(" [Radio Edit]", "")
            .replace(" Radio Edit", "")
            .strip()
            .rstrip("-")
            .rstrip()
        )
        if title:
            for a in track["artists"]:
                artist: str = a["name"]
                yield models.Song(
                    artist=artist,
                    title=title,
                )
