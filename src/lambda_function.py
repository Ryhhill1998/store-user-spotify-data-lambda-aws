import json
import os
from datetime import datetime, timezone

import mysql.connector

from src.db_service import DBService
from src.models import TopItemsData, UserSpotifyData, TopItem, ItemType, TimeRange

# Extract environment variables
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]


def extract_user_spotify_data_from_event(event: dict) -> UserSpotifyData:
    record = event["Records"][0]
    data = json.loads(record["body"])

    user_id = data["id"]
    refresh_token = data["refresh_token"]

    all_top_items_data = []

    for entry in data["all_top_items_data"]:
        top_items = [TopItem(id=item["id"], position=item["position"]) for item in entry["top_items"]]
        item_type = ItemType(entry["item_type"])
        time_range = TimeRange(entry["time_range"])
        top_items_data = TopItemsData(top_items=top_items, item_type=item_type, time_range=time_range)
        all_top_items_data.append(top_items_data)

    user_spotify_data = UserSpotifyData(
        user_id=user_id,
        refresh_token=refresh_token,
        all_top_items_data=all_top_items_data
    )
    return user_spotify_data


def lambda_handler(event, context):
    # create db connection
    connection = mysql.connector.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

    try:
        # create db service object
        db_service = DBService(connection)

        # 1. Extract user_id, refresh_token and spotify_data from event records
        user_spotify_data = extract_user_spotify_data_from_event(event)

        # 2. Update user's spotify refresh_token in DB
        if user_spotify_data.refresh_token is not None:
            db_service.update_refresh_token(
                user_id=user_spotify_data.user_id,
                refresh_token=user_spotify_data.refresh_token
            )

        # 3. Add user's spotify top items to DB
        collected_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        for top_items_data in user_spotify_data.all_top_items_data:
            db_service.store_top_items(
                user_id=user_spotify_data.user_id,
                top_items=top_items_data.top_items,
                item_type=top_items_data.item_type,
                time_range=top_items_data.time_range,
                collected_date=collected_date
            )
    except Exception as e:
        print(f"Something went wrong - {e}")
    finally:
        connection.close()
