import json
import os
from datetime import datetime, timezone

import mysql.connector

from src.db_service import DBService
from src.models import TopItemsData, UserSpotifyData, TopItem, TimeRange, Settings


def get_settings() -> Settings:
    db_host = os.environ["DB_HOST"]
    db_name = os.environ["DB_NAME"]
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]

    settings = Settings(db_host=db_host, db_name=db_name, db_user=db_user, db_pass=db_pass)

    return settings


def extract_user_spotify_data_from_event(event: dict) -> UserSpotifyData:
    record = event["Records"][0]
    data = json.loads(record["body"])

    user_id = data["user_id"]
    refresh_token = data["refresh_token"]

    def parse_top_items_data(top_items_data_raw: dict) -> TopItemsData:
        top_items = [TopItem(id=item["id"], position=item["position"]) for item in top_items_data_raw["top_items"]]
        time_range = TimeRange(top_items_data_raw["time_range"])
        top_items_data = TopItemsData(top_items=top_items, time_range=time_range)
        return top_items_data

    top_artists_data = list(map(parse_top_items_data, data["top_artists_data"]))
    top_tracks_data = list(map(parse_top_items_data, data["top_tracks_data"]))

    user_spotify_data = UserSpotifyData(
        user_id=user_id,
        refresh_token=refresh_token,
        top_artists_data=top_artists_data,
        top_tracks_data=top_tracks_data
    )
    return user_spotify_data


def lambda_handler(event, context):
    settings = get_settings()

    # 1. Extract user_id, refresh_token and spotify_data from event records
    user_spotify_data = extract_user_spotify_data_from_event(event)

    # create db connection
    with mysql.connector.connect(
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_pass
    ) as connection:
        # create db service object
        db_service = DBService(connection)

        # 2. Update user's spotify refresh_token in DB
        if user_spotify_data.refresh_token is not None:
            db_service.update_refresh_token(
                user_id=user_spotify_data.user_id,
                refresh_token=user_spotify_data.refresh_token
            )

        # 3. Add user's spotify top items to DB
        collected_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # 3a. Add top artists
        for entry in user_spotify_data.top_artists_data:
            db_service.store_top_artists(
                user_id=user_spotify_data.user_id,
                top_artists=entry.top_items,
                time_range=entry.time_range,
                collected_date=collected_date
            )

        # 3a. Add top tracks
        for entry in user_spotify_data.top_tracks_data:
            db_service.store_top_tracks(
                user_id=user_spotify_data.user_id,
                top_tracks=entry.top_items,
                time_range=entry.time_range,
                collected_date=collected_date
            )
