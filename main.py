import csv
import dataclasses
import logging
import sys
from collections.abc import Callable
from pathlib import Path

import scrapers.models
import scrapers.ultimate_guitar
import scrapers.wywrota

logger = logging.getLogger()
DATA_DIR = Path("data")


def scrape_and_save(output_file: Path, scraper: Callable, model: dataclasses.dataclass) -> int:
    rows_written = 0
    with output_file.open(mode="w", encoding="utf-8", newline="") as file:
        fieldnames = [field.name for field in dataclasses.fields(model)]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for chord in scraper():
            writer.writerow(dataclasses.asdict(chord))
            rows_written += 1
    return rows_written


def main() -> None:
    logger.info("Scraping started. Results will be saved in %s", DATA_DIR.absolute())
    DATA_DIR.mkdir(exist_ok=True)

    ug_file_path = DATA_DIR / "ultimate_guitar.csv"
    ug_rows_written = scrape_and_save(
        output_file=ug_file_path,
        scraper=scrapers.ultimate_guitar.parse,
        model=scrapers.models.SongChordsLink,
    )
    logger.info("Scraping finished for Ultimate-Guitar. %d rows written to %s", ug_rows_written, ug_file_path)

    wywrota_file_path = DATA_DIR / "wywrota.csv"
    wywrota_rows_written = scrape_and_save(
        output_file=wywrota_file_path,
        scraper=scrapers.wywrota.parse,
        model=scrapers.models.SongChordsLink,
    )
    logger.info("Scraping finished for Wywrota. %d rows written to %s", wywrota_rows_written, wywrota_file_path)


if __name__ == "__main__":
    # set up logging
    logger.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    # ---

    main()
