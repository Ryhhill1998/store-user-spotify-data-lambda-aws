import json
import os
import sys
import uuid
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import Mock, call

import pytest
from loguru import logger

from src.lambda_function import get_settings, extract_user_spotify_data_from_event, lambda_handler
from src.models import Settings, UserSpotifyData, TopItemsData, TopItem, TimeRange

# 1. Test get_settings raises KeyError if any environment variables are missing.
# 2. Test get_settings returns expected settings.
# 3. Test extract_user_spotify_data_from_event raises KeyError if Records missing from event.
# 16. Test lambda_handler closes connection to db even if Exception occurs.


@pytest.fixture
def mock_settings(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "DB_HOST": "DB_HOST",
            "DB_NAME": "DB_NAME",
            "DB_USER": "DB_USER",
            "DB_PASS": "DB_PASS"
        }
        for key, value in envvars.items():
            monkeypatch.setenv(key, value)

        yield


# 1. Test get_settings raises KeyError if any environment variables are missing.
@pytest.mark.parametrize("env_var_to_delete", ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS"])
def test_get_settings_raises_key_error_if_any_env_var_missing(mock_settings, monkeypatch, env_var_to_delete):
    monkeypatch.delenv(env_var_to_delete)

    with pytest.raises(KeyError):
        get_settings()


# 2. Test get_settings returns expected settings.
def test_get_settings_returns_expected_settings(mock_settings):
    expected_settings = Settings(
        db_host="DB_HOST",
        db_name="DB_NAME",
        db_user="DB_USER",
        db_pass="DB_PASS"
    )

    settings = get_settings()

    assert settings == expected_settings


# 3. Test extract_user_spotify_data_from_event raises KeyError if Records missing from event.
def test_extract_user_spotify_data_from_event_raises_key_error_if_records_missing():
    mock_event = {"test": "test"}

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(mock_event)

    assert "Records" in str(e.value)


# 4. Test extract_user_spotify_data_from_event raises KeyError if body missing from record.
def test_extract_user_spotify_data_from_event_raises_key_error_if_body_missing_from_record():
    mock_event = {"Records": [{"test": "test"}]}

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(mock_event)

    assert "body" in str(e.value)


# 5. Test extract_user_spotify_data_from_event raises KeyError if user_id, refresh_token, top_artists_data or top_tracks_data missing from body.
@pytest.mark.parametrize("item", ["user_id", "refresh_token", "top_artists_data", "top_tracks_data"])
def test_extract_user_spotify_data_from_event_raises_key_error_if_item_missing_from_body(item):
    body = {"user_id": "123", "refresh_token": "abc", "top_artists_data": [], "top_tracks_data": []}
    body.pop(item)
    mock_event = {"Records": [{"body": json.dumps(body)}]}

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(mock_event)

    assert item in str(e.value)


# 6. Test extract_user_spotify_data_from_event raises KeyError if top_items or time_range missing from top_artists_data or top_tracks_data.
@pytest.mark.parametrize(
    "top_items_data, item",
    [
        ("top_artists_data", "top_items"),
        ("top_artists_data", "time_range"),
        ("top_tracks_data", "top_items"),
        ("top_tracks_data", "time_range")
    ]
)
def test_extract_user_spotify_data_from_event_raises_key_error_if_top_items_or_time_range_misssing_from_top_items_data(
        top_items_data,
        item
):
    body = {
        "user_id": "123",
        "refresh_token": "abc",
        "top_artists_data": [{"top_items": [], "time_range": TimeRange.SHORT}],
        "top_tracks_data": [{"top_items": [], "time_range": TimeRange.SHORT}]
    }
    body[top_items_data][0].pop(item)
    mock_event = {"Records": [{"body": json.dumps(body)}]}

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(mock_event)

    assert item in str(e.value)


# 7. Test extract_user_spotify_data_from_event raises KeyError if id or position missing from top_items.
@pytest.mark.parametrize(
    "top_items_data, item",
    [
        ("top_artists_data", "id"),
        ("top_artists_data", "position"),
        ("top_tracks_data", "id"),
        ("top_tracks_data", "position")
    ]
)
def test_extract_user_spotify_data_from_event_raises_key_error_if_id_or_position_missing_from_top_items(
        top_items_data,
        item
):
    body = {
        "user_id": "123",
        "refresh_token": "abc",
        "top_artists_data": [{"top_items": [{"id": "1", "position": 1}], "time_range": TimeRange.SHORT}],
        "top_tracks_data": [{"top_items": [{"id": "1", "position": 1}], "time_range": TimeRange.SHORT}]
    }
    body[top_items_data][0]["top_items"][0].pop(item)
    mock_event = {"Records": [{"body": json.dumps(body)}]}

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(mock_event)

    assert item in str(e.value)


# 8. Test extract_user_spotify_data_from_event returns expected user_spotify_data.
def test_extract_user_spotify_data_from_event_returns_expected_user_spotify_data():
    user_id = str(uuid.uuid4())
    refresh_token = str(uuid.uuid4())
    record_body = {
        "user_id": user_id,
        "refresh_token": refresh_token,
        "top_artists_data": [
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "short_term"
            },
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "medium_term"
            },
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "long_term"
            }
        ],
        "top_tracks_data": [
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "short_term"
            },
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "medium_term"
            },
            {
                "top_items": [
                    {"id": "1", "position": 1},
                    {"id": "2", "position": 2},
                    {"id": "3", "position": 3}
                ],
                "time_range": "long_term"
            }
        ]
    }
    event = {"Records": [{"body": json.dumps(record_body)}]}

    user_spotify_data = extract_user_spotify_data_from_event(event)

    expected_user_spotify_data = UserSpotifyData(
        user_id=user_id,
        refresh_token=refresh_token,
        top_artists_data=[
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.SHORT
            ),
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.MEDIUM
            ),
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.LONG
            ),
        ],
        top_tracks_data=[
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.SHORT
            ),
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.MEDIUM
            ),
            TopItemsData(
                top_items=[
                    TopItem(id="1", position=1),
                    TopItem(id="2", position=2),
                    TopItem(id="3", position=3)
                ],
                time_range=TimeRange.LONG
            ),
        ]
    )
    assert user_spotify_data == expected_user_spotify_data


@pytest.fixture
def mock_user_spotify_data_factory():
    def _create(
            refresh_token: str | None = None,
            top_artists_data: list[TopItemsData] | None = None,
            top_tracks_data: list[TopItemsData] | None = None
    ) -> UserSpotifyData:
        return UserSpotifyData(
            user_id="123",
            refresh_token=refresh_token,
            top_artists_data=top_artists_data if top_artists_data else [],
            top_tracks_data=top_tracks_data if top_tracks_data else []
        )

    return _create


@pytest.fixture
def mock_connection(mocker):
    mock_conn = Mock()
    mocker.patch("src.lambda_function.mysql.connector.connect", return_value=mock_conn)
    return mock_conn


@pytest.fixture
def mock_db_service(mocker) -> Mock:
    mock_db = Mock()
    mocker.patch("src.lambda_function.DBService", return_value=mock_db)
    return mock_db


# 9. Test lambda_handler creates mysql connection with expected params.
def test_lambda_handler_creates_mysql_connection_with_expected_params(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_db_service
):
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory()
    )
    mock_connect = mocker.patch("src.lambda_function.mysql.connector.connect", return_value=Mock())

    lambda_handler("", "")

    mock_connect.assert_called_once_with(host="DB_HOST", database="DB_NAME", user="DB_USER", password="DB_PASS")


# 10. Test lambda_handler logs expected message if Exception occurs.
def test_lambda_handler_logs_expected_message_if_exception_occurs(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service,
        capsys
):
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory(refresh_token="abc")
    )
    mock_db_service.update_refresh_token.side_effect = Exception("")

    with pytest.raises(Exception):
        lambda_handler("", "")

    logs_output = capsys.readouterr().out
    assert "Something went wrong" in logs_output


# 11. Test lambda_handler updates refresh token if it is present in user_spotify_data.
def test_lambda_handler_does_update_refresh_token_if_it_is_not_none(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
):
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory(refresh_token="abc")
    )

    lambda_handler("", "")

    mock_db_service.update_refresh_token.assert_called_with(user_id="123", refresh_token="abc")


# 12. Test lambda_handler does not update refresh token if it is not present in user_spotify_data.
def test_lambda_handler_does_not_update_refresh_token_if_it_is_none(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
):
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory()
    )

    lambda_handler("", "")

    mock_db_service.update_refresh_token.assert_not_called()


@pytest.fixture
def mock_collected_date(mocker):
    mock_coll_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Patch the datetime class in your module
    class MockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return mock_coll_date

    mocker.patch("src.lambda_function.datetime", MockDateTime)
    return mock_coll_date


# 13. Test lambda_handler calls store_top_artists expected number of times and with expected params.
def test_lambda_handler_calls_store_top_artists_as_expected(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service,
        mock_collected_date
):
    top_artists_data = [
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.SHORT
        ),
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.MEDIUM
        ),
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.LONG
        )
    ]
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory(
            refresh_token="abc",
            top_artists_data=top_artists_data
        )
    )

    lambda_handler("", "")

    expected_calls = [
        call(
            user_id="123",
            top_artists=top_artists_data[0].top_items,
            time_range=top_artists_data[0].time_range,
            collected_date=mock_collected_date
        ),
        call(
            user_id="123",
            top_artists=top_artists_data[1].top_items,
            time_range=top_artists_data[1].time_range,
            collected_date=mock_collected_date
        ),
        call(
            user_id="123",
            top_artists=top_artists_data[2].top_items,
            time_range=top_artists_data[2].time_range,
            collected_date=mock_collected_date
        )
    ]
    print(mock_db_service.store_top_artists.call_args_list)
    assert mock_db_service.store_top_artists.call_count == 3
    mock_db_service.store_top_artists.assert_has_calls(calls=expected_calls, any_order=False)


# 14. Test lambda_handler calls store_top_tracks expected number of times and with expected params.
def test_lambda_handler_calls_store_top_tracks_as_expected(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service,
        mock_collected_date
):
    top_tracks_data = [
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.SHORT
        ),
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.MEDIUM
        ),
        TopItemsData(
            top_items=[TopItem(id="1", position=1), TopItem(id="2", position=2)],
            time_range=TimeRange.LONG
        )
    ]
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory(
            refresh_token="abc",
            top_tracks_data=top_tracks_data
        )
    )

    lambda_handler("", "")

    expected_calls = [
        call(
            user_id="123",
            top_tracks=top_tracks_data[0].top_items,
            time_range=top_tracks_data[0].time_range,
            collected_date=mock_collected_date
        ),
        call(
            user_id="123",
            top_tracks=top_tracks_data[1].top_items,
            time_range=top_tracks_data[1].time_range,
            collected_date=mock_collected_date
        ),
        call(
            user_id="123",
            top_tracks=top_tracks_data[2].top_items,
            time_range=top_tracks_data[2].time_range,
            collected_date=mock_collected_date
        )
    ]
    assert mock_db_service.store_top_tracks.call_count == 3
    mock_db_service.store_top_tracks.assert_has_calls(calls=expected_calls, any_order=False)


# 15. Test lambda_handler closes connection to db even if runs is successful.
def test_lambda_handler_closes_db_connection_if_successful_run(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
):
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory()
    )

    lambda_handler("", "")

    mock_connection.close.assert_called_once()


# 16. Test lambda_handler closes connection to db even if Exception occurs.
def test_lambda_handler_closes_db_connection_if_exception_occurs(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
):
    mocker.patch(
        "src.lambda_function.extract_user_spotify_data_from_event",
        return_value=mock_user_spotify_data_factory()
    )
    mocker.patch("src.lambda_function.DBService", side_effect=Exception)

    with pytest.raises(Exception):
        lambda_handler("", "")

    mock_connection.close.assert_called_once()
