import json
import os
import sys
import uuid
from unittest import mock
from unittest.mock import Mock, call

import pytest
from loguru import logger

from src.lambda_function import get_settings, extract_user_spotify_data_from_event, lambda_handler
from src.models import Settings, UserSpotifyData, TopItemsData, TopItem, TimeRange


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


def test_get_settings(mock_settings):
    expected_settings = Settings(
        db_host="DB_HOST",
        db_name="DB_NAME",
        db_user="DB_USER",
        db_pass="DB_PASS"
    )

    settings = get_settings()

    assert settings == expected_settings


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


def test_lambda_handler_closes_db_connection(
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


def test_lambda_handler_calls_store_top_artists_as_expected(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
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
    mock_datetime = mocker.patch("src.lambda_function.datetime")
    mock_collected_date = "2024-01-01"
    mock_datetime_now = Mock()
    mock_datetime_now.strftime.return_value = mock_collected_date
    mock_datetime.now.return_value = mock_datetime_now

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
    mock_db_service.store_top_artists.assert_has_calls(calls=expected_calls, any_order=False)
    assert mock_db_service.store_top_artists.call_count == 3
    
    
def test_lambda_handler_calls_store_top_tracks_as_expected(
        mocker,
        mock_settings,
        mock_user_spotify_data_factory,
        mock_connection,
        mock_db_service
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
    mock_datetime = mocker.patch("src.lambda_function.datetime")
    mock_collected_date = "2024-01-01"
    mock_datetime_now = Mock()
    mock_datetime_now.strftime.return_value = mock_collected_date
    mock_datetime.now.return_value = mock_datetime_now

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
    mock_db_service.store_top_tracks.assert_has_calls(calls=expected_calls, any_order=False)
    assert mock_db_service.store_top_tracks.call_count == 3
