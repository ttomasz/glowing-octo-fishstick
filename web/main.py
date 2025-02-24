from collections.abc import Generator
from dataclasses import astuple, dataclass

import duckdb
from pyscript import document  # type: ignore
from tabulate import tabulate


@dataclass(frozen=True, slots=True)
class Chord:
    version: int
    url: str
    rating: float
    votes: int
    difficulty: str
    tonality_name: str
    views: int


@dataclass(frozen=True, slots=True)
class Song:
    artist: str
    title: str
    chords: list[Chord]

    def as_html(self) -> str:
        pass


class SongTable:
    def __init__(self, results_element_id: str) -> None:
        self.columns = ["Artist", "Title", "Version", "URL", "Rating", "Votes", "Difficulty", "Tonality", "Views"]
        self.data: list[Song] = []
        self.results_element_id = results_element_id
        self.results_html_element = document.getElementById(results_element_id)
        self.results_html_element.innerHTML = self._data_as_html_table()

    def _flat_data(self) -> Generator[tuple, None, None]:
        for song in self.data:
            for idx, chord in enumerate(song.chords):
                if idx == 0:
                    yield (song.artist, song.title, *astuple(chord))
                else:
                    yield ("", "", *astuple(chord))

    def _data_as_html_table(self) -> str:
        if len(self.data) == 0:
            return ""
        url_col_idx = self.columns.index("URL")
        data_with_urls_as_links = (
            t[:url_col_idx] + (f'<a href="{t[url_col_idx]}" target="_blank">Link ↗️</a>',) + t[url_col_idx + 1 :]
            for t in self._flat_data()
        )
        return tabulate(
            tabular_data=data_with_urls_as_links,
            headers=self.columns,
            tablefmt="unsafehtml",
            showindex=False,
            floatfmt=".2f",
            intfmt=",",
        )

    def set_data(self, data: list[Song]) -> None:
        self.data = data
        self.results_html_element.innerHTML = self._data_as_html_table()


# init db
conn = duckdb.connect(":memory:")
conn.execute("CREATE TABLE chords AS SELECT * FROM 'chords.parquet'")
conn.execute("SELECT count(*) FROM chords")
number_of_songs = conn.fetchone()[0]
document.getElementById("span-count").textContent = number_of_songs
# table managing singleton
table = SongTable("div-results")


def new_shuffle(*args, **kwargs) -> None:
    conn.execute("SELECT artist, title, chords FROM chords ORDER BY random() desc LIMIT 10")
    data = conn.fetchall()
    songs = [Song(artist=row[0], title=row[1], chords=[Chord(**chord) for chord in row[2]]) for row in data]
    table.set_data(songs)
