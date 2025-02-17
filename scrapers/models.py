from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SongChordsLink:
    artist: str
    title: str
    url: str
    version: int = 1
    rating: float | None = None
    votes: float | None = None
    difficulty: str | None = None
    tonality_name: str | None = None
    views: int | None = None
