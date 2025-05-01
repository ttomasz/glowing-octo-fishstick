from collections.abc import Generator, Iterable
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


@dataclass(frozen=True, slots=True)
class Stats:
    number_of_songs: int
    ug_tabs: int
    wywrota_tabs: int


@dataclass(frozen=True, slots=True)
class Modifiers:
    liked_songs_modifier: float
    ug_url_modifier: float
    wywrota_url_modifier: float


class DataStore:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("CREATE TABLE chords AS SELECT * FROM 'chords.parquet'")
        self.conn.execute("CREATE TABLE previous_songs (artist TEXT NOT NULL, title TEXT NOT NULL)")
        self.get_songs_query = """
            SELECT
                artist,
                title,
                chords,
                CASE liked_on_spotify WHEN true THEN '❤️' ELSE '' END liked_on_spotify
            FROM chords
            ANTI JOIN previous_songs USING(artist, title)
            ORDER BY (
                random()
                + CASE liked_on_spotify WHEN true THEN ? ELSE 0 END
                + CASE has_ug_tabs WHEN 1 THEN ? ELSE 0 END
                + CASE has_wywrota_tabs WHEN 1 THEN ? ELSE 0 END
            ) desc
            LIMIT 10
        """

    def get_stats(self) -> Stats:
        self.conn.execute("""
            SELECT
                count(*) as number_of_songs,
                sum(has_ug_tabs) as ug_tabs,
                sum(has_wywrota_tabs) as wywrota_tabs
            FROM chords
        """)
        number_of_songs, ug_tabs, wywrota_tabs = self.conn.fetchone()
        return Stats(
            number_of_songs=number_of_songs,
            ug_tabs=ug_tabs,
            wywrota_tabs=wywrota_tabs,
        )

    def get_songs(
        self,
        liked_songs_modifier: float = 0.0,
        ug_url_modifier: float = 0.0,
        wywrota_url_modifier: float = 0.0,
        previous_songs: Iterable[tuple[str, str]] | None = None,
    ) -> list[Song]:
        if previous_songs:
            self.conn.execute("TRUNCATE previous_songs")
            self.conn.executemany("INSERT INTO previous_songs VALUES (?, ?)", previous_songs)
        self.conn.execute(
            query=self.get_songs_query,
            parameters=[
                liked_songs_modifier,
                ug_url_modifier,
                wywrota_url_modifier,
            ],
        )
        data = self.conn.fetchall()
        songs = [
            Song(artist=row[0], title=row[1], chords=[Chord(**chord) for chord in row[2]], liked_on_spotify=row[3])
            for row in data
        ]
        return songs


class ButtonManager:
    def __init__(self, element_id: str, *, disabled: bool) -> None:
        self.element_id = element_id
        self.html_element = document.getElementById(self.element_id)
        self.disabled = disabled
        if self.disabled:
            self.html_element.setAttribute("disabled", "")

    def disable(self) -> None:
        if not self.disabled:
            self.html_element.setAttribute("disabled", "")
            self.disabled = True

    def enable(self) -> None:
        if self.disabled:
            self.html_element.removeAttribute("disabled")
            self.disabled = False

    def __enter__(self) -> None:
        self.disable()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.enable()


class Settings:
    def __init__(
        self,
        liked_songs_modifier_element_id: str,
        ug_url_modifier_element_id: str,
        wywrota_url_modifier_element_id: str,
    ) -> None:
        self.liked_songs_modifier_element = document.getElementById(liked_songs_modifier_element_id)
        self.ug_url_modifier_element = document.getElementById(ug_url_modifier_element_id)
        self.wywrota_url_modifier_element = document.getElementById(wywrota_url_modifier_element_id)

    def get_modifiers(self) -> Modifiers:
        return Modifiers(
            liked_songs_modifier=float(self.liked_songs_modifier_element.value),
            ug_url_modifier=float(self.ug_url_modifier_element.value),
            wywrota_url_modifier=float(self.wywrota_url_modifier_element.value),
        )


class SongTable:
    def __init__(self, results_element_id: str, history_size: int = 10) -> None:
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
        self.previous_data: list[list[Song]] = []
        self.previous_data_limit = history_size
        self.results_element_id = results_element_id
        self.results_html_element = document.getElementById(self.results_element_id)

    def _flat_data(self) -> Generator[tuple, None, None]:
        for song in self.data:
            for idx, chord in enumerate(song.chords):
                if idx == 0:
                    yield (song.artist, song.title, *astuple(chord), song.liked_on_spotify)
                else:
                    yield ("", "", *astuple(chord), song.liked_on_spotify)

    @staticmethod
    def _site_symbol(url: str) -> str:
        if "wywrota" in url:
            return '<img src="./wywrota-icon.png" alt="(W)" width="16" height="16"/>'
        if "ultimate-guitar" in url:
            return '<img src="./ug-icon.ico" alt="(UG)" width="16" height="16"/>'
        return "🔗"

    def _data_as_html_table(self) -> str:
        if len(self.data) == 0:
            return ""
        url_col_idx = self.columns.index("URL")
        data_with_urls_as_links = (
            t[:url_col_idx]
            + (f'<a href="{t[url_col_idx]}" target="_blank">{self._site_symbol(t[url_col_idx])} Link ↗️</a>',)
            + t[url_col_idx + 1 :]
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
        if len(self.previous_data) >= self.previous_data_limit:
            self.previous_data.pop(0)
        self.previous_data.append(self.data)
        self.data = data
        self.results_html_element.innerHTML = self._data_as_html_table()

    def load_previous(self) -> int:
        if not self.previous_data:
            raise ValueError("No previous data to show")
        self.data = self.previous_data.pop()
        self.results_html_element.innerHTML = self._data_as_html_table()
        return len(self.previous_data)


# init db
data_store = DataStore()
# update header
stats = data_store.get_stats()
document.getElementById("span-count").textContent = f"{stats.number_of_songs:,}"
document.getElementById("span-count-wywrota").textContent = f"{stats.wywrota_tabs:,}"
document.getElementById("span-count-ug").textContent = f"{stats.ug_tabs:,}"
# table managing singleton
table = SongTable(results_element_id="div-results")
# buttons
shuffle_button_manager = ButtonManager(element_id="button-shuffle", disabled=False)
back_button_manager = ButtonManager(element_id="button-back", disabled=True)
# settings element
settings = Settings(
    liked_songs_modifier_element_id="liked-modifier",
    ug_url_modifier_element_id="ug-modifier",
    wywrota_url_modifier_element_id="wywrota-modifier",
)


def new_shuffle(*args, **kwargs) -> None:
    previous_songs = {(song.artist, song.title) for entry in table.previous_data for song in entry}
    modifiers = settings.get_modifiers()
    with shuffle_button_manager, back_button_manager:
        songs = data_store.get_songs(
            liked_songs_modifier=modifiers.liked_songs_modifier,
            ug_url_modifier=modifiers.ug_url_modifier,
            wywrota_url_modifier=modifiers.wywrota_url_modifier,
            previous_songs=previous_songs,
        )
        table.set_data(songs)


def load_previous_songs(*args, **kwargs) -> None:
    how_many_previous_available = table.load_previous()
    if how_many_previous_available == 0:
        back_button_manager.disable()
