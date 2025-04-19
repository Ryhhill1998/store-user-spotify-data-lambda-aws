from unittest.mock import Mock

import mysql.connector
import pytest

from src.db_service import DBService, DBServiceException


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
def mock_db_service(mock_db_connection) -> DBService:
    return DBService(mock_db_connection)


def test_update_refresh_token_raises_db_service_exception_if_mysql_error_occurs(mock_db_service, mock_cursor):
    mock_cursor.execute.side_effect = mysql.connector.Error

    with pytest.raises(DBServiceException) as e:
        mock_db_service.update_refresh_token(user_id="", refresh_token="")

    assert "Failed to update user's refresh token" in str(e.value)


def test_update_refresh_token_calls_expected_mysql_methods_with_expected_params(mock_db_service, mock_cursor):
    update_statement = "UPDATE spotify_user SET refresh_token = (%s) WHERE user_id = (%s);"

    mock_db_service.update_refresh_token(user_id="123", refresh_token="abc")

    mock_cursor.execute.assert_called_once_with(update_statement, ("123", "abc"))


def test_store_top_artists(mock_db_service):
    pass


def test_store_top_tracks(mock_db_service):
    pass


def test__store_top_items(mock_db_service):
    pass
