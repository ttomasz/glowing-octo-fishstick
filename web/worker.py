import time
from collections.abc import Generator, Iterable
from dataclasses import astuple, dataclass
from datetime import timedelta

import duckdb
from pyscript import window  # type: ignore
from tabulate import tabulate

# Global variables to be initialized later
data: "DataStore" = None  # type: ignore
table: "SongTable" = None  # type: ignore


@dataclass(frozen=True, slots=True)
class Chord:
    version: int
    url: str
    rating: float | None
    votes: int | None
    difficulty: str | None
    tonality_name: str | None


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
        window.console.log("Initializing database...")
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("""
            PRAGMA disable_print_progress_bar;
            PRAGMA disable_progress_bar;
            SET preserve_insertion_order = false;
        """)
        self.conn.execute("""
            CREATE TABLE chords AS
            SELECT
                *,
                strip_accents(lower(artist)) as search_col_a,
                strip_accents(lower(title)) as search_col_t,
                strip_accents(lower(concat(artist, ' ', title))) as search_col_at
            FROM 'chords.parquet'
        """)
        self.conn.execute("CREATE TABLE previous_songs (artist TEXT NOT NULL, title TEXT NOT NULL)")
        self.conn.execute("ANALYZE")
        self.get_songs_query = """
            WITH
            sample AS (
                SELECT
                    artist,
                    title,
                    chords,
                    liked_on_spotify,
                    has_ug_tabs,
                    has_wywrota_tabs
                FROM chords TABLESAMPLE BERNOULLI(2%)
            )
            SELECT
                artist,
                title,
                chords,
                CASE liked_on_spotify WHEN true THEN '❤️' ELSE '' END liked_on_spotify
            FROM sample
            ANTI JOIN previous_songs USING(artist, title)
            ORDER BY (
                random()
                + CASE liked_on_spotify WHEN true THEN ? ELSE 0 END
                + CASE has_ug_tabs WHEN 1 THEN ? ELSE 0 END
                + CASE has_wywrota_tabs WHEN 1 THEN ? ELSE 0 END
            ) desc
            LIMIT 10
        """
        self.search_songs_query_exact_match = """
            WITH
            params as (SELECT strip_accents(lower(?)) as search_term)
            SELECT
                artist,
                title,
                chords,
                CASE liked_on_spotify WHEN true THEN '❤️' ELSE '' END liked_on_spotify
            FROM chords
            WHERE
                search_col_a = (select search_term from params)
                or search_col_t = (select search_term from params)
        """
        self.search_songs_query = """
            WITH
            params as (SELECT strip_accents(lower(?)) as search_term)
            SELECT
                artist,
                title,
                chords,
                CASE liked_on_spotify WHEN true THEN '❤️' ELSE '' END liked_on_spotify
            FROM chords
            WHERE
                search_col_at LIKE concat('%', replace((select search_term from params), ' ', '%'), '%')
                or search_col_a LIKE concat((select search_term from params), '%')
                or search_col_t LIKE concat((select search_term from params), '%')
            ORDER BY greatest(
                jaro_similarity(search_col_at, (select search_term from params)), -- 1.0 is exact match, 0.0 is no match
                jaro_similarity(search_col_a, (select search_term from params)),
                jaro_similarity(search_col_t, (select search_term from params))
            ) desc
            LIMIT 10
        """
        window.console.log("Finished initializing database.")

    def get_stats(self) -> Stats:
        start_time = time.perf_counter()
        self.conn.execute("""
            SELECT
                count(*) as number_of_songs,
                sum(has_ug_tabs) as ug_tabs,
                sum(has_wywrota_tabs) as wywrota_tabs
            FROM chords
        """)
        number_of_songs, ug_tabs, wywrota_tabs = self.conn.fetchone()  # type: ignore
        end_time = time.perf_counter()
        delta = timedelta(seconds=(end_time - start_time))
        window.console.log(f"Getting stats from database took {delta}")
        return Stats(
            number_of_songs=number_of_songs,
            ug_tabs=ug_tabs,
            wywrota_tabs=wywrota_tabs,
        )

    def search_songs(self, search_term: str) -> list[Song]:
        start_time = time.perf_counter()
        self.conn.execute(self.search_songs_query_exact_match, [search_term])
        data = self.conn.fetchall()
        if len(data) == 0:
            search_type = "fuzzy"
            self.conn.execute(self.search_songs_query, [search_term])
            data = self.conn.fetchall()
        else:
            search_type = "exact"
        end_time = time.perf_counter()
        delta = timedelta(seconds=(end_time - start_time))
        window.console.log(f"Searching songs for term: {search_term} in database took {delta}. Search type: {search_type}")  # noqa: E501
        songs = [
            Song(artist=row[0], title=row[1], chords=[Chord(**chord) for chord in row[2]], liked_on_spotify=row[3])
            for row in data
        ]
        return songs

    def get_songs(
        self,
        liked_songs_modifier: float = 0.0,
        ug_url_modifier: float = 0.0,
        wywrota_url_modifier: float = 0.0,
        previous_songs: Iterable[tuple[str, str]] | None = None,
    ) -> list[Song]:
        """Gets a batch of random songs from the database, applying given modifiers and excluding previous songs."""
        start_time = time.perf_counter()
        if previous_songs:
            self.conn.execute("TRUNCATE previous_songs")
            self.conn.executemany("INSERT INTO previous_songs VALUES (?, ?)", previous_songs)
        delta = timedelta(seconds=(time.perf_counter() - start_time))
        window.console.log(f"Inserting previous songs into the database took {delta}")
        window.console.log(f"Sending get_songs query with modifiers: liked_songs_modifier={liked_songs_modifier}, ug_url_modifier={ug_url_modifier}, wywrota_url_modifier={wywrota_url_modifier}")  # noqa: E501
        start_time = time.perf_counter()
        self.conn.execute(
            query=self.get_songs_query,
            parameters=[
                liked_songs_modifier,
                ug_url_modifier,
                wywrota_url_modifier,
            ],
        )
        window.console.log("Executed get_songs_query")
        data = self.conn.fetchall()
        delta = timedelta(seconds=(time.perf_counter() - start_time))
        window.console.log(f"Getting a batch of random songs from database took {delta}")
        songs = [
            Song(artist=row[0], title=row[1], chords=[Chord(**chord) for chord in row[2]], liked_on_spotify=row[3])
            for row in data
        ]
        return songs


class SongTable:
    def __init__(self, history_size: int = 10) -> None:
        self.columns = [
            "Artist",
            "Title",
            "Version",
            "URL",
            "Rating",
            "Votes",
            "Difficulty",
            "Tonality",
            "Liked on Spotify",
        ]
        self.data: list[Song] = []
        self.previous_data: list[list[Song]] = []
        self.previous_data_limit = history_size

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
        start_time = time.perf_counter()
        if len(self.data) == 0:
            return ""
        url_col_idx = self.columns.index("URL")
        data_with_urls_as_links = (
            t[:url_col_idx]
            + (f'<a href="{t[url_col_idx]}" target="_blank">{self._site_symbol(t[url_col_idx])} Link ↗️</a>',)
            + t[url_col_idx + 1 :]
            for t in self._flat_data()
        )
        table = tabulate(
            tabular_data=data_with_urls_as_links,
            headers=self.columns,
            tablefmt="unsafehtml",
            showindex=False,
            floatfmt=".2f",
            intfmt=",",
        )
        end_time = time.perf_counter()
        delta = timedelta(seconds=(end_time - start_time))
        window.console.log(f"Creating HTML table took {delta}")
        return table

    def set_data(self, data: list[Song]) -> str:
        """
        Sets new data, saves current data to previous_data history, and returns HTML table representation of new data.
        """
        if len(self.previous_data) >= self.previous_data_limit:
            self.previous_data.pop(0)
        self.previous_data.append(self.data)
        self.data = data
        return self._data_as_html_table()

    def load_previous(self) -> tuple[int, str]:
        """Loads previous data from history and returns tuple of:
        (how many previous entries are still available, HTML table representation of loaded data).
        """
        if not self.previous_data:
            raise ValueError("No previous data to show")
        self.data = self.previous_data.pop()
        return len(self.previous_data), self._data_as_html_table()


def init_data_store_and_table(history_size: int = 10) -> None:
    global data  # noqa: PLW0603
    global table  # noqa: PLW0603
    data = DataStore()
    table = SongTable(history_size=history_size)


def get_stats() -> tuple[int, int, int]:
    """Gets number of songs in the database."""
    return astuple(data.get_stats())


def new_shuffle(
    liked_songs_modifier: float = 0.0,
    ug_url_modifier: float = 0.0,
    wywrota_url_modifier: float = 0.0,
    *args,
    **kwargs,
) -> str:
    """Generates a new batch of random songs applying given modifiers and returns HTML table representation of the table."""  # noqa: E501
    previous_songs = {(song.artist, song.title) for entry in table.previous_data for song in entry}
    songs = data.get_songs(
        liked_songs_modifier=liked_songs_modifier,
        ug_url_modifier=ug_url_modifier,
        wywrota_url_modifier=wywrota_url_modifier,
        previous_songs=previous_songs,
    )
    html_table = table.set_data(songs)
    return html_table


def load_previous_songs(*args, **kwargs) -> tuple[int, str]:
    """Loads previous data from history and returns tuple of: (how many previous entries are still available, HTML table representation of loaded data)."""  # noqa: E501
    how_many_previous_available, html_table = table.load_previous()
    return how_many_previous_available, html_table


def new_search(search_term: str, *args, **kwargs) -> str:
    """Searches songs by the given search term and returns HTML table representation of the results or message."""
    if not search_term:
        return "<p>Type search phrase</p>"
    songs = data.search_songs(search_term)
    if not songs:
        return "<p>No results found</p>"
    html_table = table.set_data(songs)
    return html_table


# Define what to export to the main thread.
__export__ = [
    "new_shuffle",
    "load_previous_songs",
    "new_search",
    "init_data_store_and_table",
    "get_stats",
]
