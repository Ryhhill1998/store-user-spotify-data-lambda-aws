from datetime import datetime
from enum import Enum

import mysql.connector
from loguru import logger

from src.models import TimeRange, TopArtist, TopTrack, TopGenre, TopEmotion


class ItemType(str, Enum):
    ARTIST = "artist"
    TRACK = "track"


class DBServiceException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class DBService:
    def __init__(self, connection: mysql.connector.pooling.PooledMySQLConnection):
        self.connection = connection

    def update_refresh_token(self, user_id: str, refresh_token: str):
        cursor = self.connection.cursor()

        try:
            update_statement = (
                "UPDATE spotify_user "
                "SET refresh_token = %s "
                "WHERE id = %s;"
            )
            cursor.execute(update_statement, (user_id, refresh_token))
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to update user's refresh token"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    def store_top_artists(
            self,
            user_id: str,
            top_artists: list[TopArtist],
            time_range: TimeRange,
            collected_date: datetime
    ):
        insert_statement = (
            "INSERT INTO top_artist ("
            "spotify_user_id, "
            "artist_id, "
            "collected_date, "
            "time_range, "
            "position"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        values = [(user_id, artist.id, collected_date, time_range.value, artist.position) for artist in top_artists]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_statement, values)
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to store top artists"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    def store_top_tracks(
            self,
            user_id: str,
            top_tracks: list[TopTrack],
            time_range: TimeRange,
            collected_date: datetime
    ):
        insert_statement = (
            "INSERT INTO top_track ("
            "spotify_user_id, "
            "track_id, "
            "collected_date, "
            "time_range, "
            "position"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        values = [(user_id, track.id, collected_date, time_range.value, track.position) for track in top_tracks]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_statement, values)
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to store top tracks"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    def store_top_genres(
            self,
            user_id: str,
            top_genres: list[TopGenre],
            time_range: TimeRange,
            collected_date: datetime
    ):
        insert_statement = (
            "INSERT INTO top_genre ("
            "spotify_user_id, "
            "genre_name, "
            "collected_date, "
            "time_range, "
            "count"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        values = [(user_id, genre.name, collected_date, time_range.value, genre.count) for genre in top_genres]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_statement, values)
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to store top genres"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    def store_top_emotions(
            self,
            user_id: str,
            top_emotions: list[TopEmotion],
            time_range: TimeRange,
            collected_date: datetime
    ):
        insert_statement = (
            "INSERT INTO top_emotion ("
            "spotify_user_id, "
            "emotion_name, "
            "collected_date, "
            "time_range, "
            "percentage"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        values = [
            (
                user_id,
                emotion.name,
                collected_date,
                time_range.value,
                emotion.percentage
            )
            for emotion in top_emotions
        ]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_statement, values)
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to store top emotions"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()
