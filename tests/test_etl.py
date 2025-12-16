import pytest
from unittest.mock import MagicMock, patch

from tomorrow.etl import run_weather_etl


@pytest.fixture
def base_config(app_config):
    """
    Base ETL config derived from app_config,
    safe to modify per test.
    """
    return {
        "api": app_config["api"],
        "db": app_config["db"],
        "locations": [
            {"lat": 25.9, "lon": -97.4},
            {"lat": 25.8, "lon": -97.5},
        ],
        "rate_limit_sleep_seconds": 0,  # speed up tests
    }


@patch("tomorrow.etl.time.sleep")
@patch("tomorrow.etl.WeatherDB")
@patch("tomorrow.etl.TomorrowAPIClient")
def test_etl_success_happy_path(
    mock_api_cls,
    mock_db_cls,
    mock_sleep,
    base_config,
):
    """
    Happy path:
    - API returns data for each location
    - DB insert succeeds
    - Total records processed is correct
    """

    # --- Mock API client ---
    mock_api = MagicMock()
    mock_api.fetch_weather_data.side_effect = [
        [{"temperature": 10}, {"temperature": 11}],  # location 1
        [{"temperature": 12}],                       # location 2
    ]
    mock_api_cls.return_value = mock_api

    # --- Mock DB client ---
    mock_db = MagicMock()
    mock_db.bulk_insert_weather_data.return_value = None
    mock_db_cls.return_value = mock_db

    total = run_weather_etl(base_config)

    assert total == 3
    assert mock_api.fetch_weather_data.call_count == 2
    assert mock_db.bulk_insert_weather_data.call_count == 2
    assert mock_sleep.call_count == 2

    mock_api.close.assert_called_once()
    mock_db.close.assert_called_once()


@patch("tomorrow.etl.time.sleep")
@patch("tomorrow.etl.WeatherDB")
@patch("tomorrow.etl.TomorrowAPIClient")
def test_etl_skips_invalid_location(
    mock_api_cls,
    mock_db_cls,
    mock_sleep,
    base_config,
):
    """
    Invalid location entries should be skipped safely.
    """

    base_config["locations"].append({"lat": 10.0})  # missing lon

    mock_api = MagicMock()
    mock_api.fetch_weather_data.return_value = [{"temperature": 10}]
    mock_api_cls.return_value = mock_api

    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db

    total = run_weather_etl(base_config)

    assert total == 2  # only valid locations processed
    assert mock_api.fetch_weather_data.call_count == 2
    assert mock_db.bulk_insert_weather_data.call_count == 2


@patch("tomorrow.etl.time.sleep")
@patch("tomorrow.etl.WeatherDB")
@patch("tomorrow.etl.TomorrowAPIClient")
def test_etl_handles_empty_api_response(
    mock_api_cls,
    mock_db_cls,
    mock_sleep,
    base_config,
):
    """
    Empty API responses should not be inserted.
    """

    mock_api = MagicMock()
    mock_api.fetch_weather_data.side_effect = [
        [],
        [{"temperature": 12}],
    ]
    mock_api_cls.return_value = mock_api

    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db

    total = run_weather_etl(base_config)

    assert total == 1
    assert mock_db.bulk_insert_weather_data.call_count == 1


@patch("tomorrow.etl.time.sleep")
@patch("tomorrow.etl.WeatherDB")
@patch("tomorrow.etl.TomorrowAPIClient")
def test_etl_continues_on_location_failure(
    mock_api_cls,
    mock_db_cls,
    mock_sleep,
    base_config,
):
    """
    Failure for one location must not stop the ETL.
    """

    mock_api = MagicMock()
    mock_api.fetch_weather_data.side_effect = [
        RuntimeError("API failure"),
        [{"temperature": 12}],
    ]
    mock_api_cls.return_value = mock_api

    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db

    total = run_weather_etl(base_config)

    assert total == 1
    assert mock_api.fetch_weather_data.call_count == 2
    assert mock_db.bulk_insert_weather_data.call_count == 1


def test_etl_invalid_locations_config(app_config):
    """
    locations must be a list.
    """

    bad_config = {
        "api": app_config["api"],
        "db": app_config["db"],
        "locations": "not-a-list",
    }

    with pytest.raises(RuntimeError):
        run_weather_etl(bad_config)
