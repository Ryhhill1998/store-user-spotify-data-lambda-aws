from dataclasses import dataclass
from enum import Enum


class ItemType(Enum):
    ARTIST = "artist"
    TRACK = "track"


class TimeRange(Enum):
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
class TopItem:
    id: str
    position: int


@dataclass
class TopItemsData:
    top_items: list[TopItem]
    item_type: ItemType
    time_range: TimeRange


@dataclass
class UserSpotifyData:
    user_id: str
    refresh_token: str | None
    all_top_items_data: list[TopItemsData]
