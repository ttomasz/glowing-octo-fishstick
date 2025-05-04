import csv
import dataclasses
import logging
import sys
from collections.abc import Callable, Generator
from functools import partial
from pathlib import Path

import scrapers.models
import scrapers.spotify
import scrapers.ultimate_guitar
import scrapers.wywrota

logger = logging.getLogger()
DATA_DIR = Path("data")


def scrape_and_save(
    output_file: Path,
    scraper: Callable[..., Generator[dataclasses.dataclass, None, None]],
    fieldnames: list[str],
) -> int:
    """Scrape data using provided scraper function and save it to a CSV file. Returns number of rows written."""

    rows_written = 0
    with output_file.open(mode="w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for chord in scraper():
            writer.writerow(dataclasses.asdict(chord))
            rows_written += 1
    return rows_written


def main(args: list[str]) -> None:
    scrape_wywrota = "--scrape-wywrota" in args
    scrape_ultimate_guitar = "--scrape-ultimate-guitar" in args
    scrape_spotify = "--scrape-spotify" in args
    if not any([scrape_wywrota, scrape_ultimate_guitar, scrape_spotify]):
        raise ValueError(
            "Please provide at least one scraper to run "
            "(one of: --scrape-wywrota, --scrape-ultimate-guitar, --scrape-spotify)"  # noqa: COM812
        )
    logger.info("Scraping started. Results will be saved in %s. Parameters: %s", DATA_DIR.absolute(), args[1:])
    DATA_DIR.mkdir(exist_ok=True)

    if scrape_ultimate_guitar:
        logger.info("Scraping started for Ultimate-Guitar")
        ug_file_path = DATA_DIR / "ultimate_guitar.csv"
        ug_db_path = DATA_DIR / "ultimate_guitar.db"
        parser = partial(scrapers.ultimate_guitar.parse, db_path=ug_db_path)
        ug_rows_written = scrape_and_save(
            output_file=ug_file_path,
            scraper=parser,
            fieldnames=[field.name for field in dataclasses.fields(scrapers.models.SongChordsLink)],
        )
        logger.info("Scraping finished for Ultimate-Guitar. %d rows written to %s", ug_rows_written, ug_file_path)

    if scrape_wywrota:
        logger.info("Scraping started for Wywrota")
        wywrota_file_path = DATA_DIR / "wywrota.csv"
        wywrota_rows_written = scrape_and_save(
            output_file=wywrota_file_path,
            scraper=scrapers.wywrota.parse,
            fieldnames=[field.name for field in dataclasses.fields(scrapers.models.SongChordsLink)],
        )
        logger.info("Scraping finished for Wywrota. %d rows written to %s", wywrota_rows_written, wywrota_file_path)

    if scrape_spotify:
        logger.info("Scraping started for Spotify")
        spotify_file_path = DATA_DIR / "spotify.csv"
        spotify_rows_written = scrape_and_save(
            output_file=spotify_file_path,
            scraper=scrapers.spotify.extract,
            fieldnames=[field.name for field in dataclasses.fields(scrapers.models.Song)],
        )
        logger.info("Scraping finished for Spotify. %d rows written to %s", spotify_rows_written, spotify_file_path)

    logger.info("Done.")


if __name__ == "__main__":
    # set up logging
    logger.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    # ---

    main(args=sys.argv)
