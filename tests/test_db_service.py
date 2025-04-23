import uuid
from datetime import datetime
from unittest.mock import Mock

import mysql.connector
import pytest

from src.db_service import DBService, DBServiceException, ItemType
from src.models import TopItem, TimeRange

# 1. Test update_refresh_token raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
# 2. Test update_refresh_token raises calls execute with expected params and closes cursor.
# 3. Test _get_top_items raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
# 4. Test _get_top_items returns calls execute with expected params and closes cursor.
# 5. Test _get_top_items returns expected top items.
# 6. Test _store_top_items raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
# 7. Test _store_top_items calls executemany with expected params and closes cursor.
# 8. Test _calculate_position_changes returns expected top items.
# 9. Test _store_top_items_with_position_changes calls _store_top_items with expected params.
# 10. Test store_top_artists calls _store_top_items_with_position_changes with expected params.
# 11. Test store_top_tracks calls _store_top_items_with_position_changes with expected params.


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


# 1. Test update_refresh_token raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
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


# 2. Test update_refresh_token raises calls execute with expected params and closes cursor.
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


# 3. Test _get_top_items raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
def test_get_top_items_raises_db_service_exception_if_mysql_error_occurs():
    pass


# 4. Test _get_top_items returns calls execute with expected params and closes cursor.
def test_get_top_items_calls_expected_mysql_methods_with_expected_params():
    pass


# 5. Test _get_top_items returns expected top items.
def test_get_top_items_returns_expected_top_items():
    pass


# 6. Test _store_top_items raises DBServiceException, closes cursor and rolls back if mysql.connector.Error occurs.
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


# 7. Test _store_top_items calls executemany with expected params and closes cursor.
def test__store_top_items_calls_expected_mysql_methods_with_expected_params(
        db_service,
        mock_db_connection,
        mock_cursor
):
    user_id = str(uuid.uuid4())
    top_items = [TopItem(id="1", position=1), TopItem(id="2", position=2), TopItem(id="3", position=3)]
    time_range = TimeRange.SHORT
    collected_date = "2024-01-01"
    insert_statement = "INSERT INTO top_track (spotify_user_id, track_id, collected_date, position, position_change, is_new, time_range) VALUES (%s, %s, %s, %s, %s, %s, %s);"

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
            (user_id, "1", collected_date, 1, None, False, "short_term"),
            (user_id, "2", collected_date, 2, None, False, "short_term"),
            (user_id, "3", collected_date, 3, None, False, "short_term")
        ]
    )
    mock_db_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()


# 8. Test _calculate_position_changes returns expected top items.
def test_calculate_position_changes_returns_expected_top_items(db_service):
    items_current = [TopItem(id="1", position=1), TopItem(id="2", position=2), TopItem(id="3", position=3)]
    items_prev = [TopItem(id="1", position=3), TopItem(id="2", position=2), TopItem(id="4", position=3)]

    items_with_position_changes = db_service._calculate_position_changes(
        items_current=items_current,
        items_prev=items_prev
    )

    expected_items_with_position_changes = [
        TopItem(id="1", position=1, position_change=2, is_new=False),
        TopItem(id="2", position=2, position_change=0, is_new=False),
        TopItem(id="3", position=3, position_change=None, is_new=True)
    ]
    assert items_with_position_changes == expected_items_with_position_changes


# 9. Test _store_top_items_with_position_changes calls _store_top_items with expected params.
def test__store_top_items_with_position_changes_calls__store_top_items_with_expected_params():
    pass


# 10. Test store_top_artists calls _store_top_items_with_position_changes with expected params.
def test_store_top_artists_calls__store_top_items_with_position_changes_with_expected_params(db_service):
    top_artists = [TopItem(id="1", position=1), TopItem(id="2", position=2), TopItem(id="3", position=3)]
    mock__store_top_items_with_position_changes = Mock()
    db_service._store_top_items_with_position_changes = mock__store_top_items_with_position_changes
    user_id = str(uuid.uuid4())
    time_range = TimeRange.SHORT
    collected_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

    db_service.store_top_artists(
        user_id=user_id,
        top_artists=top_artists,
        time_range=time_range,
        collected_date=collected_date
    )

    mock__store_top_items_with_position_changes.assert_called_once_with(
        user_id=user_id,
        top_items=top_artists,
        item_type=ItemType.ARTIST,
        time_range=time_range,
        collected_date=collected_date
    )


# 11. Test store_top_tracks calls _store_top_items_with_position_changes with expected params.
def test_store_top_tracks_calls__store_top_items_with_expected_params(db_service):
    mock__store_top_items_with_position_changes = Mock()
    db_service._store_top_items_with_position_changes = mock__store_top_items_with_position_changes
    user_id = str(uuid.uuid4())
    top_tracks = [TopItem(id="1", position=1), TopItem(id="2", position=2), TopItem(id="3", position=3)]
    time_range = TimeRange.SHORT
    collected_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

    db_service.store_top_tracks(
        user_id=user_id,
        top_tracks=top_tracks,
        time_range=time_range,
        collected_date=collected_date
    )

    mock__store_top_items_with_position_changes.assert_called_once_with(
        user_id=user_id,
        top_items=top_tracks,
        item_type=ItemType.TRACK,
        time_range=time_range,
        collected_date=collected_date
    )
