from dataclasses import dataclass
from enum import Enum


class TimeRange(str, Enum):
    SHORT = "short_term"
    MEDIUM = "medium_term"
    LONG = "long_term"


@dataclass
class Settings:
    db_host: str
    db_name: str
    db_user: str
    db_pass: str


@dataclass
class TopArtist:
    id: str
    position: int


@dataclass
class TopTrack:
    id: str
    position: int


@dataclass
class TopGenre:
    name: str
    count: int


@dataclass
class TopEmotion:
    name: str
    percentage: float


@dataclass
class TopArtistsData:
    top_artists: list[TopArtist]
    time_range: TimeRange


@dataclass
class TopTracksData:
    top_tracks: list[TopTrack]
    time_range: TimeRange


@dataclass
class TopGenresData:
    top_genres: list[TopGenre]
    time_range: TimeRange


@dataclass
class TopEmotionsData:
    top_emotions: list[TopEmotion]
    time_range: TimeRange


@dataclass
class UserSpotifyData:
    refresh_token: str
    top_artists_data: list[TopArtistsData]
    top_tracks_data: list[TopTracksData]
    top_genres_data: list[TopGenresData]
    top_emotions_data: list[TopEmotionsData]
