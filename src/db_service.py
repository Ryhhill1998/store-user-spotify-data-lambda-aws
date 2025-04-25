from dataclasses import asdict
from datetime import timedelta, datetime
from enum import Enum

import mysql.connector
import numpy as np
from loguru import logger
import pandas as pd

from src.models import TopItem, TimeRange, ComparisonIntervalDays


class ItemType(str, Enum):
    ARTIST = "artist"
    TRACK = "track"


class DBServiceException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class DBService:
    def __init__(
            self,
            connection: mysql.connector.pooling.PooledMySQLConnection,
            comparison_interval_days: ComparisonIntervalDays
    ):
        self.connection = connection
        self.comparison_interval_days = comparison_interval_days

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

    def _get_top_items(
            self,
            user_id: str,
            item_type: ItemType,
            time_range: TimeRange,
            collected_date: str
    ) -> list[TopItem]:
        cursor = self.connection.cursor(dictionary=True)

        try:
            select_statement = (
                f"SELECT * FROM top_{item_type.value} "
                "WHERE spotify_user_id = %s "
                "AND time_range = %s "
                "AND collected_date = %s "
                "ORDER BY position ASC;"
            )
            cursor.execute(select_statement, (user_id, time_range.value, collected_date))
            results = cursor.fetchall()
            top_items = [TopItem(id=entry[f"{item_type.value}_id"], position=entry["position"]) for entry in results]
            return top_items
        except mysql.connector.Error as e:
            error_message = f"Failed to get top artists. User ID: {user_id}, time range: {time_range.value}"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    def _store_top_items(
            self,
            user_id: str,
            top_items: list[TopItem],
            item_type: ItemType,
            time_range: TimeRange,
            collected_date: str
    ):
        insert_statement = (
            f"INSERT INTO top_{item_type.value} ("
                "spotify_user_id, "
                f"{item_type.value}_id, "
                "collected_date, "
                "position, "
                "position_change, "
                "is_new, "
                "time_range"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        values = [
            (
                user_id,
                item.id,
                collected_date,
                item.position,
                item.position_change,
                item.is_new,
                time_range.value
            )
            for item in top_items
        ]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_statement, values)
            self.connection.commit()
        except mysql.connector.Error as e:
            self.connection.rollback()
            error_message = "Failed to store top items"
            logger.error(f"{error_message} - {e}")
            raise DBServiceException(error_message)
        finally:
            cursor.close()

    @staticmethod
    def _calculate_position_changes(items_current: list[TopItem], items_prev: list[TopItem]):
        latest_df = pd.DataFrame([asdict(item) for item in items_current])
        prev_df = pd.DataFrame([asdict(item) for item in items_prev])
        merged_df = pd.merge(
            latest_df,
            prev_df[["id", "position"]],
            on=["id"],
            how="left",
            suffixes=("", "_prev")
        )
        merged_df["position_change"] = merged_df["position_prev"] - merged_df["position"]
        merged_df["is_new"] = pd.isna(merged_df["position_change"])
        records = merged_df[["id", "position", "position_change", "is_new"]].replace({np.nan: None}).to_dict("records")
        items_with_position_changes = [TopItem(**record) for record in records]
        return items_with_position_changes

    def _store_top_items_with_position_changes(
            self,
            user_id: str,
            top_items: list[TopItem],
            item_type: ItemType,
            time_range: TimeRange,
            collected_date: datetime
    ):
        days = getattr(self.comparison_interval_days, time_range.value)
        prev_date = (collected_date - timedelta(days=days)).strftime("%Y-%m-%d")
        top_items_prev = self._get_top_items(
            user_id=user_id,
            item_type=item_type,
            time_range=time_range,
            collected_date=prev_date
        )

        if top_items_prev:
            top_items_to_store = self._calculate_position_changes(items_current=top_items, items_prev=top_items_prev)
        else:
            top_items_to_store = top_items

        self._store_top_items(
            user_id=user_id,
            top_items=top_items_to_store,
            item_type=item_type,
            time_range=time_range,
            collected_date=collected_date.strftime("%Y-%m-%d")
        )

    def store_top_artists(
            self,
            user_id: str,
            top_artists: list[TopItem],
            time_range: TimeRange,
            collected_date: datetime
    ):
        self._store_top_items_with_position_changes(
            user_id=user_id,
            top_items=top_artists,
            item_type=ItemType.ARTIST,
            time_range=time_range,
            collected_date=collected_date
        )

    def store_top_tracks(
            self,
            user_id: str,
            top_tracks: list[TopItem],
            time_range: TimeRange,
            collected_date: datetime
    ):
        self._store_top_items_with_position_changes(
            user_id=user_id,
            top_items=top_tracks,
            item_type=ItemType.TRACK,
            time_range=time_range,
            collected_date=collected_date
        )
