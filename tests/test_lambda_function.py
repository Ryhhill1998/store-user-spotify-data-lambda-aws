import json
import os
from copy import deepcopy
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, call

import pytest

from src.db_service import DBService
from src.lambda_function import get_settings, extract_user_spotify_data_from_event, lambda_handler
from src.models import Settings, UserSpotifyData, TimeRange, TopArtist, TopArtistsData, TopTracksData, TopTrack, \
    TopGenresData, TopGenre, TopEmotionsData, TopEmotion


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
                                {"name": "emotion1", "percentage": 0.3, "track_id": "1"},
                                {"name": "emotion2", "percentage": 0.1, "track_id": "2"}
                            ],
                            "time_range": "short_term"
                        },
                        {
                            "top_emotions": [
                                {"name": "emotion1", "percentage": 0.3, "track_id": "1"},
                                {"name": "emotion2", "percentage": 0.1, "track_id": "2"}
                            ],
                            "time_range": "medium_term"
                        },
                        {
                            "top_emotions": [
                                {"name": "emotion1", "percentage": 0.3, "track_id": "1"},
                                {"name": "emotion2", "percentage": 0.1, "track_id": "2"}
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


@pytest.fixture
def mock_user_spotify_data() -> UserSpotifyData:
    return UserSpotifyData(
        user_id="1",
        refresh_token="refresh",
        top_artists_data=[
            TopArtistsData(
                top_artists=[
                    TopArtist(id="1", position=1), TopArtist(id="2", position=2)
                ],
                time_range=TimeRange.SHORT
            ),
            TopArtistsData(
                top_artists=[
                    TopArtist(id="1", position=1), TopArtist(id="2", position=2)
                ],
                time_range=TimeRange.MEDIUM
            ),
            TopArtistsData(
                top_artists=[
                    TopArtist(id="1", position=1), TopArtist(id="2", position=2)
                ],
                time_range=TimeRange.LONG
            )
        ],
        top_tracks_data=[
            TopTracksData(
                top_tracks=[
                    TopTrack(id="1", position=1), TopTrack(id="2", position=2)
                ],
                time_range=TimeRange.SHORT
            ),
            TopTracksData(
                top_tracks=[
                    TopTrack(id="1", position=1), TopTrack(id="2", position=2)
                ],
                time_range=TimeRange.MEDIUM
            ),
            TopTracksData(
                top_tracks=[
                    TopTrack(id="1", position=1), TopTrack(id="2", position=2)
                ],
                time_range=TimeRange.LONG
            )
        ],
        top_genres_data=[
            TopGenresData(
                top_genres=[
                    TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)
                ],
                time_range=TimeRange.SHORT
            ),
            TopGenresData(
                top_genres=[
                    TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)
                ],
                time_range=TimeRange.MEDIUM
            ),
            TopGenresData(
                top_genres=[
                    TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)
                ],
                time_range=TimeRange.LONG
            )
        ],
        top_emotions_data=[
            TopEmotionsData(
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
                time_range=TimeRange.SHORT
            ),
            TopEmotionsData(
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
                time_range=TimeRange.MEDIUM
            ),
            TopEmotionsData(
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
                time_range=TimeRange.LONG
            )
        ]
    )


# 4. Test extract_user_spotify_data_from_event returns expected user spotify data.
def test_extract_user_spotify_data_from_event_returns_expected_user_spotify_data(mock_event, mock_user_spotify_data):
    convert_body_to_json_string(mock_event)

    user_spotify_data = extract_user_spotify_data_from_event(mock_event)

    expected_user_spotify_data = mock_user_spotify_data
    assert user_spotify_data == expected_user_spotify_data


@pytest.fixture
def mock_get_settings(mocker):
    return mocker.patch(
        "src.lambda_function.get_settings",
        return_value=Settings(
            db_host="",
            db_name="",
            db_user="",
            db_pass=""
        )
    )


@pytest.fixture
def mock_extract_user_spotify_data_from_event(mocker, mock_user_spotify_data):
    return mocker.patch("src.lambda_function.extract_user_spotify_data_from_event", return_value=mock_user_spotify_data)


@pytest.fixture
def mock_connection(mocker):
    mock_conn = Mock()

    mocker.patch("src.lambda_function.mysql.connector.connect", return_value=mock_conn)

    return mock_conn


@pytest.fixture
def mock_db_service(mocker):
    mock_dbs = Mock(spec=DBService)

    mocker.patch("src.lambda_function.DBService", return_value=mock_dbs)

    return mock_dbs


@pytest.fixture
def mock_datetime_now(mocker):
    mock_dt = mocker.patch("src.lambda_function.datetime")
    mock_dt.now.return_value = datetime(2025, 1, 1, 12, 0, 0)


# 5. Test lambda_handler calls db_service.update_refresh_token if refresh_token not None.
def test_lambda_handler_calls_db_service_update_refresh_token_if_refresh_token_not_none(
        mock_get_settings,
        mock_extract_user_spotify_data_from_event,
        mock_connection,
        mock_db_service,
        mock_datetime_now
):
    lambda_handler({}, {})

    mock_db_service.update_refresh_token.assert_called_once_with(user_id="1", refresh_token="refresh")


# 6. Test lambda_handler does not call db_service.update_refresh_token if refresh_token is None.
def test_lambda_handler_does_not_call_db_service_update_refresh_token_if_refresh_token_is_none(
        mocker,
        mock_get_settings,
        mock_user_spotify_data,
        mock_connection,
        mock_db_service,
        mock_datetime_now
):
    mock_user_spotify_data.refresh_token = None
    mocker.patch("src.lambda_function.extract_user_spotify_data_from_event", return_value=mock_user_spotify_data)

    lambda_handler({}, {})

    mock_db_service.update_refresh_token.assert_not_called()


# 7. Test lambda_handler calls expected methods with expected params.
def test_lambda_handler_calls_expected_methods_with_expected_params(
        mock_get_settings,
        mock_extract_user_spotify_data_from_event,
        mock_connection,
        mock_db_service,
        mock_datetime_now
):
    lambda_handler({}, {})

    mock_get_settings.assert_called_once()
    mock_extract_user_spotify_data_from_event.assert_called_once_with({})
    collected_date = datetime(2025, 1, 1, 12, 0, 0)
    expected_store_top_artists_calls = [
        call(
            user_id="1",
            top_artists=[TopArtist(id="1", position=1), TopArtist(id="2", position=2)],
            time_range=TimeRange.SHORT,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_artists=[TopArtist(id="1", position=1), TopArtist(id="2", position=2)],
            time_range=TimeRange.MEDIUM,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_artists=[TopArtist(id="1", position=1), TopArtist(id="2", position=2)],
            time_range=TimeRange.LONG,
            collected_date=collected_date
        )
    ]
    mock_db_service.store_top_artists.assert_has_calls(expected_store_top_artists_calls, any_order=False)
    assert mock_db_service.store_top_artists.call_count == 3
    expected_store_top_tracks_calls = [
        call(
            user_id="1",
            top_tracks=[TopTrack(id="1", position=1), TopTrack(id="2", position=2)],
            time_range=TimeRange.SHORT,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_tracks=[TopTrack(id="1", position=1), TopTrack(id="2", position=2)],
            time_range=TimeRange.MEDIUM,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_tracks=[TopTrack(id="1", position=1), TopTrack(id="2", position=2)],
            time_range=TimeRange.LONG,
            collected_date=collected_date
        )
    ]
    mock_db_service.store_top_tracks.assert_has_calls(expected_store_top_tracks_calls, any_order=False)
    assert mock_db_service.store_top_tracks.call_count == 3
    expected_store_top_genres_calls = [
        call(
            user_id="1",
            top_genres=[TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)],
            time_range=TimeRange.SHORT,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_genres=[TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)],
            time_range=TimeRange.MEDIUM,
            collected_date=collected_date
        ),
        call(
            user_id="1",
            top_genres=[TopGenre(name="genre1", count=3), TopGenre(name="genre2", count=1)],
            time_range=TimeRange.LONG,
            collected_date=collected_date
        )
    ]
    mock_db_service.store_top_genres.assert_has_calls(expected_store_top_genres_calls, any_order=False)
    assert mock_db_service.store_top_genres.call_count == 3
    expected_store_top_emotions_calls = [
        call(
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
        ),
        call(
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
            time_range=TimeRange.MEDIUM,
            collected_date=collected_date
        ),
        call(
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
            time_range=TimeRange.LONG,
            collected_date=collected_date
        )
    ]
    mock_db_service.store_top_emotions.assert_has_calls(expected_store_top_emotions_calls, any_order=False)
    assert mock_db_service.store_top_emotions.call_count == 3
    mock_connection.close.assert_called_once()


# 8. Test lambda_handler closes db connection if Exception occurs.
def test_lambda_handler_closes_db_connection_if_exception_occurs(
        mock_get_settings,
        mock_extract_user_spotify_data_from_event,
        mock_connection,
        mock_db_service,
        mock_datetime_now
):
    mock_db_service.store_top_artists.side_effect = Exception("Test")

    with pytest.raises(Exception):
        lambda_handler({}, {})

    mock_connection.close.assert_called_once()
