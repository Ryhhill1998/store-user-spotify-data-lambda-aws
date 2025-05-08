import json
import os
from datetime import datetime, timezone

import mysql.connector
from loguru import logger

from src.db_service import DBService
from src.models import UserSpotifyData, Settings, TopArtist, TopArtistsData, TopTracksData, TopTrack, \
    TopGenresData, TopGenre, TopEmotion, TopEmotionsData


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
    
    top_artists_data_raw = data["top_artists_data_raw"]
    top_tracks_data_raw = data["top_tracks_data_raw"]
    top_genres_data_raw = data["top_genres_data_raw"]
    top_emotions_data_raw = data["top_emotions_data_raw"]
    
    top_artists_data = []
    for entry in top_artists_data_raw:
        time_range = entry["time_range"]
        top_artists_raw = entry["top_artists"]
        top_artists = [TopArtist(id=artist["id"], position=artist["position"]) for artist in top_artists_raw]
        top_artists_data.append(TopArtistsData(top_artists=top_artists, time_range=time_range))

    top_tracks_data = []
    for entry in top_tracks_data_raw:
        time_range = entry["time_range"]
        top_tracks_raw = entry["top_tracks"]
        top_tracks = [TopTrack(id=track["id"], position=track["position"]) for track in top_tracks_raw]
        top_tracks_data.append(TopTracksData(top_tracks=top_tracks, time_range=time_range))

    top_genres_data = []
    for entry in top_genres_data_raw:
        time_range = entry["time_range"]
        top_genres_raw = entry["top_genres"]
        top_genres = [TopGenre(name=genre["name"], count=genre["count"]) for genre in top_genres_raw]
        top_genres_data.append(TopGenresData(top_genres=top_genres, time_range=time_range))

    top_emotions_data = []
    for entry in top_emotions_data_raw:
        time_range = entry["time_range"]
        top_emotions_raw = entry["top_emotions"]
        top_emotions = [
            TopEmotion(
                name=emotion["name"],
                percentage=emotion["percentage"]
            )
            for emotion in top_emotions_raw
        ]
        top_emotions_data.append(TopEmotionsData(top_emotions=top_emotions, time_range=time_range))

    user_spotify_data = UserSpotifyData(
        user_id=user_id,
        refresh_token=refresh_token,
        top_artists_data=top_artists_data,
        top_tracks_data=top_tracks_data,
        top_genres_data=top_genres_data,
        top_emotions_data=top_emotions_data
    )
    return user_spotify_data


def lambda_handler(event, context):
    settings = get_settings()

    # 1. Extract user_id, refresh_token and spotify_data from event records
    user_spotify_data = extract_user_spotify_data_from_event(event)

    # create db connection
    connection = mysql.connector.connect(
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_pass
    )

    try:
        # create db service object
        db_service = DBService(connection)

        # 2. Update user's spotify refresh_token in DB
        if user_spotify_data.refresh_token is not None:
            db_service.update_refresh_token(
                user_id=user_spotify_data.user_id,
                refresh_token=user_spotify_data.refresh_token
            )

        # 3. Add user's spotify top items to DB
        collected_date = datetime.now(timezone.utc)

        # 3a. Add top artists
        for entry in user_spotify_data.top_artists_data:
            db_service.store_top_artists(
                user_id=user_spotify_data.user_id,
                top_artists=entry.top_artists,
                time_range=entry.time_range,
                collected_date=collected_date
            )

        # 3b. Add top tracks
        for entry in user_spotify_data.top_tracks_data:
            db_service.store_top_tracks(
                user_id=user_spotify_data.user_id,
                top_tracks=entry.top_tracks,
                time_range=entry.time_range,
                collected_date=collected_date
            )

        # 3c. Add top genres
        for entry in user_spotify_data.top_genres_data:
            db_service.store_top_genres(
                user_id=user_spotify_data.user_id,
                top_genres=entry.top_genres,
                time_range=entry.time_range,
                collected_date=collected_date
            )

        # 3d. Add top emotions
        for entry in user_spotify_data.top_emotions_data:
            db_service.store_top_emotions(
                user_id=user_spotify_data.user_id,
                top_emotions=entry.top_emotions,
                time_range=entry.time_range,
                collected_date=collected_date
            )
    except Exception as e:
        error_message = "Something went wrong"
        logger.error(f"{error_message} - {e}")
        raise
    finally:
        connection.close()
