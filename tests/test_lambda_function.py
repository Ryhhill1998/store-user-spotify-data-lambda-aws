import json
import os
import sys
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import Mock, call

import pytest
from loguru import logger

from src.lambda_function import get_settings, extract_user_spotify_data_from_event, lambda_handler
from src.models import Settings, UserSpotifyData, TimeRange


# 1. Test get_settings raises KeyError if any environment variables are missing.
# 2. Test get_settings returns expected settings.
# 3. Test extract_user_spotify_data_from_event raises KeyError if missing fields in event.
# 4. Test extract_user_spotify_data_from_event returns expected user spotify data.
# 5. Test lambda_handler calls db_service.update_refresh_token if refresh_token not None.
# 6. Test lambda_handler does not call db_service.update_refresh_token if refresh_token is None.
# 7. Test lambda_handler calls expected methods with expected params.
# 8. Test lambda_handler closes db connection if Exception occurs.


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


def delete_field(data: dict, field: str):
    data_copy = deepcopy(data)
    keys = field.split(".")
    current = data_copy

    for key in keys[:-1]:
        if key == "[]":
            current = current[0]
        else:
            current = current[key]

    del current[keys[-1]]
    return data_copy, keys[-1]


def convert_body_to_json_string(event: dict):
    records = event.get("Records")

    if isinstance(records, list):
        record = records[0]

        if "body" in record:
            record["body"] = json.dumps(record["body"])


@pytest.fixture
def mock_event():
    return {
        "Records": [
            {
                "body": {
                    "user_id": "1",
                    "refresh_token": "refresh",
                    "top_artists_data": [
                        {
                            "top_artists": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "short_term"
                        },
                        {
                            "top_artists": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "medium_term"
                        },
                        {
                            "top_artists": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "long_term"
                        }
                    ],
                    "top_tracks_data": [
                        {
                            "top_tracks": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "short_term"
                        },
                        {
                            "top_tracks": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "medium_term"
                        },
                        {
                            "top_tracks": [{"id": "1", "position": 1}, {"id": "2", "position": 2}],
                            "time_range": "long_term"
                        }
                    ],
                    "top_genres_data": [
                        {
                            "top_genres": [{"name": "genre1", "count": 3}, {"name": "genre2", "count": 1}],
                            "time_range": "short_term"
                        },
                        {
                            "top_genres": [{"name": "genre1", "count": 3}, {"name": "genre2", "count": 1}],
                            "time_range": "medium_term"
                        },
                        {
                            "top_genres": [{"name": "genre1", "count": 3}, {"name": "genre2", "count": 1}],
                            "time_range": "long_term"
                        }
                    ],
                    "top_emotions_data": [
                        {
                            "top_emotions": [
                                {"name": "emotion1", "percentage": 0.3},
                                {"name": "emotion2", "percentage": 0.1}
                            ],
                            "time_range": "short_term"
                        },
                        {
                            "top_emotions": [
                                {"name": "emotion1", "percentage": 0.3},
                                {"name": "emotion2", "percentage": 0.1}
                            ],
                            "time_range": "medium_term"
                        },
                        {
                            "top_emotions": [
                                {"name": "emotion1", "percentage": 0.3},
                                {"name": "emotion2", "percentage": 0.1}
                            ],
                            "time_range": "long_term"
                        }
                    ]
                }
            }
        ]
    }


# 3. Test extract_user_spotify_data_from_event raises KeyError if missing field in event.
@pytest.mark.parametrize(
    "missing_field",
    [
        "Records",
        "Records.[].body",
        "Records.[].body.user_id",
        "Records.[].body.refresh_token",
        "Records.[].body.top_artists_data",
        "Records.[].body.top_artists_data.[].top_artists.[].id",
        "Records.[].body.top_artists_data.[].top_artists.[].position",
        "Records.[].body.top_tracks_data",
        "Records.[].body.top_tracks_data.[].top_tracks.[].id",
        "Records.[].body.top_tracks_data.[].top_tracks.[].position",
        "Records.[].body.top_genres_data",
        "Records.[].body.top_genres_data.[].top_genres.[].name",
        "Records.[].body.top_genres_data.[].top_genres.[].count",
        "Records.[].body.top_emotions_data",
        "Records.[].body.top_emotions_data.[].top_emotions.[].name",
        "Records.[].body.top_emotions_data.[].top_emotions.[].percentage",
    ]
)
def test_extract_user_spotify_data_from_event_raises_key_error_if_records_missing(mock_event, missing_field):
    test_event, deleted_field = delete_field(data=mock_event, field=missing_field)
    convert_body_to_json_string(test_event)

    with pytest.raises(KeyError) as e:
        extract_user_spotify_data_from_event(test_event)

    assert deleted_field in str(e.value)


# 4. Test extract_user_spotify_data_from_event returns expected user spotify data.


# 5. Test lambda_handler calls db_service.update_refresh_token if refresh_token not None.


# 6. Test lambda_handler does not call db_service.update_refresh_token if refresh_token is None.


# 7. Test lambda_handler calls expected methods with expected params.


# 8. Test lambda_handler closes db connection if Exception occurs.
