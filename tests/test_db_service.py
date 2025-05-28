from datetime import datetime
from unittest.mock import Mock

import mysql.connector
import pytest

from src.db_service import DBService, DBServiceException, ItemType
from src.models import TimeRange, TopArtist, TopTrack, TopGenre, TopEmotion


# 1. Test update_refresh_token raises DBServiceException if mysql.connector.Error occurs.
# 2. Test update_refresh_token calls execute with expected params.
# 3. Test _store_top_items raises DBServiceException if mysql.connector.Error occurs.
# 4. Test _store_top_items calls executemany with expected params.
# 5. Test store_top_artists calls _store_top_items with expected params.
# 6. Test store_top_tracks calls _store_top_items with expected params.
# 7. Test store_top_genres calls _store_top_items with expected params.
# 8. Test store_top_emotions calls _store_top_items with expected params.


@pytest.fixture
def mock_cursor() -> Mock:
    mock_cur = Mock()
    return mock_cur

@pytest.fixture
def mock_db_connection(mock_cursor) -> Mock:
    mock_con = Mock()
    mock_con.cursor.return_value = mock_cursor
    return mock_con


@pytest.fixture
def db_service(mock_db_connection) -> DBService:
    return DBService(mock_db_connection)


# 1. Test update_refresh_token raises DBServiceException if mysql.connector.Error occurs.
def test_update_refresh_token_raises_db_service_exception_if_mysql_error_occurs(db_service, mock_cursor):
    mock_cursor.execute.side_effect = mysql.connector.Error

    with pytest.raises(DBServiceException) as e:
        db_service.update_refresh_token(user_id="", refresh_token="")

    assert "Failed to update user's refresh token" in str(e.value)


# 2. Test update_refresh_token calls execute with expected params.
def test_update_refresh_token_calls_execute_with_expected_params(db_service, mock_cursor):
    db_service.update_refresh_token(user_id="123", refresh_token="abc")

    update_statement = "UPDATE spotify_user SET refresh_token = %s WHERE id = %s;"
    mock_cursor.execute.assert_called_once_with(update_statement, ("123", "abc"))


# 3. Test _store_top_items raises DBServiceException if mysql.connector.Error occurs.
def test__store_top_items_raises_db_service_exception_if_mysql_error_occurs(db_service, mock_cursor):
    mock_cursor.executemany.side_effect = mysql.connector.Error

    with pytest.raises(DBServiceException) as e:
        db_service._store_top_items(item_type=ItemType.ARTISTS, insert_statement="", values=[])

    assert "Failed to store top artists" in str(e.value)


# # 4. Test _store_top_items calls executemany with expected params.
def test__store_top_items_calls_executemany_with_expected_params(db_service, mock_cursor):
    db_service._store_top_items(item_type=ItemType.ARTISTS, insert_statement="", values=[])

    mock_cursor.executemany.assert_called_once_with("", [])


# 5. Test store_top_artists calls _store_top_items with expected params.
def test_store_top_artists_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    collected_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

    db_service.store_top_artists(
        user_id="1",
        top_artists=[TopArtist(id="1", position=1), TopArtist(id="2", position=2)],
        time_range=TimeRange.SHORT,
        collected_date=collected_date
    )

    expected_insert_statement = "INSERT INTO top_artist (spotify_user_id, artist_id, collected_date, time_range, position) VALUES (%s, %s, %s, %s, %s);"
    expected_values = [
        ("1", "1", collected_date, "short_term", 1),
        ("1", "2", collected_date, "short_term", 2)
    ]
    mock__store_top_items.assert_called_once_with(
        item_type=ItemType.ARTISTS,
        insert_statement=expected_insert_statement,
        values=expected_values
    )


# 6. Test store_top_tracks calls _store_top_items with expected params.
def test_store_top_tracks_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    collected_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

    db_service.store_top_tracks(
        user_id="1",
        top_tracks=[TopTrack(id="1", position=1), TopTrack(id="2", position=2)],
        time_range=TimeRange.SHORT,
        collected_date=collected_date
    )

    expected_insert_statement = "INSERT INTO top_track (spotify_user_id, track_id, collected_date, time_range, position) VALUES (%s, %s, %s, %s, %s);"
    expected_values = [
        ("1", "1", collected_date, "short_term", 1),
        ("1", "2", collected_date, "short_term", 2)
    ]
    mock__store_top_items.assert_called_once_with(
        item_type=ItemType.TRACKS,
        insert_statement=expected_insert_statement,
        values=expected_values
    )


# 7. Test store_top_genres calls _store_top_items with expected params.
def test_store_top_genres_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    collected_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

    db_service.store_top_genres(
        user_id="1",
        top_genres=[TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)],
        time_range=TimeRange.SHORT,
        collected_date=collected_date
    )

    expected_insert_statement = "INSERT INTO top_genre (spotify_user_id, genre_name, collected_date, time_range, count) VALUES (%s, %s, %s, %s, %s);"
    expected_values = [
        ("1", "genre1", collected_date, "short_term", 3),
        ("1", "genre2", collected_date, "short_term", 1)
    ]
    mock__store_top_items.assert_called_once_with(
        item_type=ItemType.GENRES,
        insert_statement=expected_insert_statement,
        values=expected_values
    )


# 8. Test store_top_emotions calls _store_top_items with expected params.
def test_store_top_emotions_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    collected_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

    db_service.store_top_emotions(
        user_id="1",
        top_emotions=[
            TopEmotion(
                name="emotion1",
                percentage=0.3,
                track_id="1"
            ),
            TopEmotion(
                name="emotion2",
                percentage=0.1,
                track_id="2"
            )
        ],
        time_range=TimeRange.SHORT,
        collected_date=collected_date
    )

    expected_insert_statement = "INSERT INTO top_emotion (spotify_user_id, emotion_name, track_id, collected_date, time_range, percentage) VALUES (%s, %s, %s, %s, %s, %s);"
    expected_values = [
        ("1", "emotion1", "1", collected_date, "short_term", 0.3),
        ("1", "emotion2", "2", collected_date, "short_term", 0.1)
    ]
    mock__store_top_items.assert_called_once_with(
        item_type=ItemType.EMOTIONS,
        insert_statement=expected_insert_statement,
        values=expected_values
    )
