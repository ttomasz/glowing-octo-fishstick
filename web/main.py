from collections.abc import Generator
from dataclasses import astuple, dataclass

import duckdb
from pyscript import document  # type: ignore
from tabulate import tabulate


@dataclass(frozen=True, slots=True)
class Chord:
    version: int
    url: str
    rating: float | None
    votes: int | None
    difficulty: str | None
    tonality_name: str | None
    views: int | None


@dataclass(frozen=True, slots=True)
class Song:
    artist: str
    title: str
    chords: list[Chord]
    liked_on_spotify: bool


class ShuffleButton:
    def __init__(self, button_element_id: str) -> None:
        self.button_element_id = button_element_id
        self.button_html_element = document.getElementById(self.button_element_id)

    def __enter__(self) -> None:
        self.button_html_element.setAttribute("disabled", "")

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.button_html_element.removeAttribute("disabled")


class SongTable:
    def __init__(self, results_element_id: str) -> None:
        self.columns = [
            "Artist",
            "Title",
            "Version",
            "URL",
            "Rating",
            "Votes",
            "Difficulty",
            "Tonality",
            "Views",
            "Liked on Spotify",
        ]
        self.data: list[Song] = []
        self.results_element_id = results_element_id
        self.results_html_element = document.getElementById(results_element_id)
        self.results_html_element.innerHTML = self._data_as_html_table()

    def _flat_data(self) -> Generator[tuple, None, None]:
        for song in self.data:
            for idx, chord in enumerate(song.chords):
                if idx == 0:
                    yield (song.artist, song.title, *astuple(chord), song.liked_on_spotify)
                else:
                    yield ("", "", *astuple(chord), song.liked_on_spotify)

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
conn.execute("""
             SELECT
                count(*) as number_of_songs,
                sum(has_ug_tabs) as ug_tabs,
                sum(has_wywrota_tabs) as wywrota_tabs
             FROM chords
             """)
number_of_songs, ug_tabs, wywrota_tabs = conn.fetchone()
document.getElementById("span-count").textContent = f"{number_of_songs:,}"
document.getElementById("span-count-wywrota").textContent = f"{wywrota_tabs:,}"
document.getElementById("span-count-ug").textContent = f"{ug_tabs:,}"
# table managing singleton
table = SongTable("div-results")
shuffle_button_manager = ShuffleButton("button-shuffle")


def new_shuffle(*args, **kwargs) -> None:
    with shuffle_button_manager:
        conn.execute("""
            SELECT
                artist,
                title,
                chords,
                CASE liked_on_spotify WHEN true THEN '❤️' ELSE '' END liked_on_spotify
            FROM chords
            ORDER BY (
                random()
                + CASE liked_on_spotify WHEN true THEN 0.01 ELSE 0 END
            ) desc
            LIMIT 10
        """)
        data = conn.fetchall()
        songs = [
            Song(artist=row[0], title=row[1], chords=[Chord(**chord) for chord in row[2]], liked_on_spotify=row[3])
            for row in data
        ]
        table.set_data(songs)
