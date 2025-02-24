import logging
import sys
from pathlib import Path

import duckdb

logger = logging.getLogger()
data_dir = Path("data")
query_path = Path(__file__).parent / "db.sql"
wywrota_file_path = data_dir / "wywrota.csv"
ultimate_guitar_file_path = data_dir / "ultimate_guitar.csv"
spotify_file_path = data_dir / "spotify.csv"
output_file_path = data_dir / "chords.parquet"


def main() -> None:
    # figure out what should be in this script later
    # for now just do these hardcoded things
    if output_file_path.exists():
        logger.info("Removing existing output file %s", output_file_path)
        output_file_path.unlink()

    with duckdb.connect(
        database=":memory:",
        config={"preserve_insertion_order": False},
    ) as connection:
        logger.info("Preparing dataset...")
        connection.execute(
            query=f"""
COPY (
{query_path.read_text()}
) TO '{output_file_path.absolute().as_posix()}' WITH (format parquet, compression zstd, compression_level 22)
            """.strip(),
            parameters={
                "ultimate_guitar_file_path": ultimate_guitar_file_path.absolute().as_posix(),
                "wywrota_file_path": wywrota_file_path.absolute().as_posix(),
                "spotify_file_path": spotify_file_path.absolute().as_posix(),
            },
        )
    logger.info("Done.")


if __name__ == "__main__":
    # set up logging
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    # ---

    main()
