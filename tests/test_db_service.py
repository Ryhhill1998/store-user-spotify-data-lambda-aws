import uuid
from unittest.mock import Mock

import mysql.connector
import pytest

from src.db_service import DBService, DBServiceException, ItemType
from src.models import TopItem, TimeRange


@pytest.fixture
def mock_cursor() -> Mock:
    mock = Mock()
    return mock


@pytest.fixture
def mock_db_connection(mock_cursor) -> Mock:
    mock = Mock()
    mock.cursor.return_value = mock_cursor
    return mock


@pytest.fixture
def db_service(mock_db_connection) -> DBService:
    return DBService(mock_db_connection)


def test_update_refresh_token_raises_db_service_exception_if_mysql_error_occurs(
        db_service,
        mock_db_connection,
        mock_cursor
):
    mock_cursor.execute.side_effect = mysql.connector.Error

    with pytest.raises(DBServiceException) as e:
        db_service.update_refresh_token(user_id="", refresh_token="")

    assert "Failed to update user's refresh token" in str(e.value)
    mock_db_connection.rollback.assert_called_once()


def test_update_refresh_token_calls_expected_mysql_methods_with_expected_params(
        db_service,
        mock_db_connection,
        mock_cursor
):
    update_statement = "UPDATE spotify_user SET refresh_token = %s WHERE id = %s;"

    db_service.update_refresh_token(user_id="123", refresh_token="abc")

    mock_cursor.execute.assert_called_once_with(update_statement, ("123", "abc"))
    mock_db_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()



def test_store_top_artists_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    user_id = str(uuid.uuid4())
    top_artists = [
        TopItem(id=str(uuid.uuid4()), position=1),
        TopItem(id=str(uuid.uuid4()), position=2),
        TopItem(id=str(uuid.uuid4()), position=3)
    ]
    time_range = TimeRange.SHORT
    collected_date = "2024-01-01"

    db_service.store_top_artists(
        user_id=user_id,
        top_artists=top_artists,
        time_range=time_range,
        collected_date=collected_date
    )

    mock__store_top_items.assert_called_once_with(
        user_id=user_id,
        top_items=top_artists,
        item_type=ItemType.ARTIST,
        time_range=time_range,
        collected_date=collected_date
    )


def test_store_top_tracks_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items = Mock()
    db_service._store_top_items = mock__store_top_items
    user_id = str(uuid.uuid4())
    top_tracks = [
        TopItem(id=str(uuid.uuid4()), position=1),
        TopItem(id=str(uuid.uuid4()), position=2),
        TopItem(id=str(uuid.uuid4()), position=3)
    ]
    time_range = TimeRange.SHORT
    collected_date = "2024-01-01"

    db_service.store_top_tracks(
        user_id=user_id,
        top_tracks=top_tracks,
        time_range=time_range,
        collected_date=collected_date
    )

    mock__store_top_items.assert_called_once_with(
        user_id=user_id,
        top_items=top_tracks,
        item_type=ItemType.TRACK,
        time_range=time_range,
        collected_date=collected_date
    )


def test__store_top_items_raises_db_service_exception_if_mysql_error_occurs(
        db_service,
        mock_db_connection,
        mock_cursor
):
    user_id = str(uuid.uuid4())
    top_items = [
        TopItem(id=str(uuid.uuid4()), position=1),
        TopItem(id=str(uuid.uuid4()), position=2),
        TopItem(id=str(uuid.uuid4()), position=3)
    ]
    time_range = TimeRange.SHORT
    collected_date = "2024-01-01"
    mock_cursor.executemany.side_effect = mysql.connector.Error

    with pytest.raises(DBServiceException) as e:
        db_service._store_top_items(
            user_id=user_id,
            top_items=top_items,
            item_type=ItemType.TRACK,
            time_range=time_range,
            collected_date=collected_date
        )

    assert "Failed to store top items" in str(e.value)
    mock_db_connection.rollback.assert_called_once()


def test__store_top_items_calls_expected_mysql_methods_with_expected_params(
        db_service,
        mock_db_connection,
        mock_cursor
):
    user_id = str(uuid.uuid4())
    top_items = [TopItem(id="1", position=1), TopItem(id="2", position=2), TopItem(id="3", position=3)]
    time_range = TimeRange.SHORT
    collected_date = "2024-01-01"
    insert_statement = "INSERT INTO top_track (spotify_user_id, track_id, collected_date, position, position_change, position_status, time_range) VALUES (%s, %s, %s, %s, %s);"

    db_service._store_top_items(
        user_id=user_id,
        top_items=top_items,
        item_type=ItemType.TRACK,
        time_range=time_range,
        collected_date=collected_date
    )

    mock_cursor.executemany.assert_called_once_with(
        insert_statement,
        [
            (user_id, "1", collected_date, 1, "short_term"),
            (user_id, "2", collected_date, 2, "short_term"),
            (user_id, "3", collected_date, 3, "short_term")
        ]
    )
    mock_db_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
