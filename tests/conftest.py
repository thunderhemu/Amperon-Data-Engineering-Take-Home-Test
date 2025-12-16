import os
import sys
from unittest.mock import MagicMock
import requests

import pytest
from sqlalchemy import create_engine, text

# Make project importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tomorrow.db import WeatherDB

@pytest.fixture(scope="session")
def app_config():
    """
    Lightweight test config.
    Does NOT call load_config().
    """
    return {
        "api": {
            "base_url": "https://api.tomorrow.io",
            "forecast_endpoint": "/v4/weather/forecast",
            "fields": [
                "temperature",
                "windSpeed",
                "humidity",
                "precipitationType",
            ],
            "timesteps": ["1h"],
            "units": "metric",
            "timeout_seconds": 5,
            "max_retries": 3,
            "retry_backoff_seconds": 2,
            "key": "dummy-api-key",
        },
        "db": {
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "postgres",
            "database": "tomorrow",
        },
        "locations": [
            {"lat": 25.9, "lon": -97.4},
            {"lat": 25.8, "lon": -97.5},
        ],
        "rate_limit_sleep_seconds": 0,
    }


@pytest.fixture
def sample_api_response():
    return {
        "timelines": {
            "hourly": [
                {
                    "time": "2025-12-15T14:00:00Z",
                    "values": {
                        "temperature": 15.5,
                        "windSpeed": 5.0,
                        "humidity": 70,
                        "precipitationType": 0,
                    },
                },
                {
                    "time": "2025-12-15T16:00:00Z",
                    "values": {
                        "temperature": 16.0,
                        "windSpeed": 5.5,
                        "humidity": 68,
                        "precipitationType": 1,
                    },
                },
            ]
        }
    }

@pytest.fixture
def sample_db_data():
    return [
        {
            "latitude": 25.9,
            "longitude": -97.4,
            "time_stamp": "2025-12-15T10:00:00Z",
            "is_forecast": False,
            "temperature": 10,
            "wind_speed": 3.1,
            "humidity": 50,
            "precipitation_type": 0,
        },
        {
            "latitude": 25.9,
            "longitude": -97.4,
            "time_stamp": "2025-12-15T11:00:00Z",
            "is_forecast": False,
            "temperature": 11,
            "wind_speed": 3.5,
            "humidity": 52,
            "precipitation_type": 0,
        },
        {
            "latitude": 25.8,
            "longitude": -97.5,
            "time_stamp": "2025-12-15T10:00:00Z",
            "is_forecast": True,
            "temperature": 20,
            "wind_speed": 8.0,
            "humidity": 60,
            "precipitation_type": 1,
        },
    ]

@pytest.fixture(scope="session")
def db_engine(app_config):
    db = app_config["db"]
    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/{db['database']}"
    )
    yield engine
    engine.dispose()

@pytest.fixture
def db_client(db_engine, app_config):
    client = WeatherDB(app_config["db"])
    client.engine = db_engine

    with db_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE weather_data RESTART IDENTITY;"))
        conn.commit()

    return client

def mock_get_request(status_code, json_data=None):
    """Create a mock HTTP response with proper HTTPError behavior."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data or {}

    if status_code >= 400:
        error = requests.exceptions.HTTPError(f"Mock HTTP {status_code}")
        error.response = mock_resp     # 🔥 THIS IS THE KEY FIX
        mock_resp.raise_for_status.side_effect = error
    else:
        mock_resp.raise_for_status.return_value = None

    return mock_resp

