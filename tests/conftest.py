import pytest
import os
import sys
from unittest.mock import MagicMock
from tomorrow.config_loader import CONFIG
from tomorrow.db import WeatherDB  # Import WeatherDB for metadata access
from sqlalchemy import create_engine, text

# Setting up the path is correct for modular imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture(scope="session")
def app_config():
    """Provides the application configuration loaded from the file/environment."""
    return CONFIG


@pytest.fixture(scope="session")
def db_config(app_config):
    """Provides database configuration for the test environment."""
    return app_config['db']


@pytest.fixture(scope="session")
def db_engine(db_config):
    """
    Provides a SQLAlchemy engine and ensures the test table is created
    and then truncated once before the session.
    """
    db_url = (f"postgresql://{db_config['user']}:{db_config['password']}@"
              f"{db_config['host']}:{db_config['port']}/{db_config['database']}")
    engine = create_engine(db_url)

    # FIX 1: Ensure table creation using SQLAlchemy Metadata (Guarantees DDL is run for tests)
    # Instantiate a temporary client to access the metadata object and define the table
    temp_client = WeatherDB(db_config)
    temp_client.metadata.create_all(engine)

    # Initial cleanup (TRUNCATE should be safer now)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE weather_data RESTART IDENTITY;"))
        conn.commit()

    return engine


@pytest.fixture
# FIX 2 (CRITICAL): Explicitly require the db_config dictionary result
def db_client(db_engine, db_config):
    """Provides an instance of the WeatherDB client."""

    # CORRECT: db_config is now the dictionary result
    client = WeatherDB(db_config)

    # Use the session-scoped engine for transactional cleanup
    client.engine = db_engine

    # Clear the table before each test for isolation
    with db_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE weather_data RESTART IDENTITY;"))
        conn.commit()

    return client


@pytest.fixture
def mock_api_client():
    """Provides a mock instance of the TomorrowAPIClient."""
    mock_client = MagicMock()
    mock_client.config = CONFIG['api']
    return mock_client


@pytest.fixture
def sample_api_response():
    """Provides a realistic structure for a mocked successful API response."""
    return {
        "data": {
            "timelines": [
                {
                    "timestep": "1h",
                    "intervals": [
                        {
                            "startTime": "2025-12-15T12:00:00Z",
                            "values": {"temperature": 15.5, "windSpeed": 5.0, "humidity": 70, "precipitationType": 0}
                        },
                        {
                            "startTime": "2025-12-15T13:00:00Z",
                            "values": {"temperature": 16.0, "windSpeed": 5.5, "humidity": 68, "precipitationType": 1}
                        },
                    ]
                }
            ]
        }
    }


@pytest.fixture
def sample_db_data():
    """Provides a list of dictionaries ready for DB insertion."""
    return [
        {'latitude': 25.9, 'longitude': -97.4, 'time_stamp': '2025-12-15T10:00:00Z', 'is_forecast': False,
         'temperature': 10, 'wind_speed': 3.1, 'humidity': 50, 'precipitation_type': 0},
        {'latitude': 25.9, 'longitude': -97.4, 'time_stamp': '2025-12-15T11:00:00Z', 'is_forecast': False,
         'temperature': 11, 'wind_speed': 3.5, 'humidity': 52, 'precipitation_type': 0},
        {'latitude': 25.8, 'longitude': -97.5, 'time_stamp': '2025-12-15T10:00:00Z', 'is_forecast': True,
         'temperature': 20, 'wind_speed': 8.0, 'humidity': 60, 'precipitation_type': 1},
    ]