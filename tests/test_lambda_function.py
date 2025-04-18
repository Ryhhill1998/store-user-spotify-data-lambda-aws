import json
import os
import uuid
from unittest import mock

import pytest

from src.lambda_function import get_settings, extract_user_spotify_data_from_event
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
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
                ],
                "time_range": "short_term"
            },
            {
                "top_items": [
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
                ],
                "time_range": "medium_term"
            },
            {
                "top_items": [
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
                ],
                "time_range": "long_term"
            }
        ],
        "top_tracks_data": [
            {
                "top_items": [
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
                ],
                "time_range": "short_term"
            },
            {
                "top_items": [
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
                ],
                "time_range": "medium_term"
            },
            {
                "top_items": [
                    {
                        "id": "1",
                        "position": 1
                    },
                    {
                        "id": "2",
                        "position": 2
                    },
                    {
                        "id": "3",
                        "position": 3
                    }
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


def test_lambda_handler():
    pass
