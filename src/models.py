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
class TopItem:
    id: str
    position: int
