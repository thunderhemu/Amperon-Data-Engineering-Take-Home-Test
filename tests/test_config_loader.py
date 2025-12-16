import os
import pytest
from unittest.mock import patch, mock_open

from tomorrow.config_loader import load_config


VALID_YAML = """
api:
  base_url: "https://api.tomorrow.io"
  forecast_endpoint: "/v4/weather/forecast"
  fields:
    - temperature
    - windSpeed
    - humidity
    - precipitationType
  timesteps:
    - "1h"
  units: "metric"
  timeout_seconds: 5
  max_retries: 3
locations:
  - lat: 25.9
    lon: -97.4
"""


@patch.dict(
    os.environ,
    {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "postgres",
        "PGPASSWORD": "postgres",
        "PGDATABASE": "tomorrow",
        "TOMORROW_IO_API_KEY": "dummy-api-key",
    },
    clear=True,
)
@patch("builtins.open", new_callable=mock_open, read_data=VALID_YAML)
def test_load_config_success(mock_file):
    config = load_config()

    assert "api" in config
    assert "db" in config
    assert "locations" in config

    assert config["db"]["host"] == "localhost"
    assert config["api"]["key"] == "dummy-api-key"
    assert config["api"]["timesteps_minutes"] == 60

@patch("builtins.open", side_effect=FileNotFoundError)
def test_missing_config_file(mock_file):
    with pytest.raises(RuntimeError, match="Configuration file missing"):
        load_config()

INVALID_YAML_MISSING_SECTIONS = """
api:
  timesteps:
    - "1h"
"""

@patch.dict(
    os.environ,
    {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "postgres",
        "PGPASSWORD": "postgres",
        "PGDATABASE": "tomorrow",
        "TOMORROW_IO_API_KEY": "dummy-api-key",
    },
    clear=True,
)
@patch("builtins.open", new_callable=mock_open, read_data=INVALID_YAML_MISSING_SECTIONS)
def test_missing_required_yaml_sections(mock_file):
    with pytest.raises(RuntimeError, match="Invalid configuration file"):
        load_config()

@patch.dict(
    os.environ,
    {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        # PGUSER missing
        "PGPASSWORD": "postgres",
        "PGDATABASE": "tomorrow",
        "TOMORROW_IO_API_KEY": "dummy-api-key",
    },
    clear=True,
)
@patch("builtins.open", new_callable=mock_open, read_data=VALID_YAML)
def test_missing_db_env_vars(mock_file):
    with pytest.raises(RuntimeError, match="Database configuration incomplete"):
        load_config()

@patch.dict(
    os.environ,
    {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "postgres",
        "PGPASSWORD": "postgres",
        "PGDATABASE": "tomorrow",
        # TOMORROW_IO_API_KEY missing
    },
    clear=True,
)
@patch("builtins.open", new_callable=mock_open, read_data=VALID_YAML)
def test_missing_api_key(mock_file):
    with pytest.raises(RuntimeError, match="Missing API key"):
        load_config()

INVALID_TIMESTEP_YAML = """
api:
  base_url: "https://api.tomorrow.io"
  forecast_endpoint: "/v4/weather/forecast"
  fields:
    - temperature
  timesteps:
    - "1x"
  units: "metric"
  timeout_seconds: 5
  max_retries: 3
locations:
  - lat: 25.9
    lon: -97.4
"""

@patch.dict(
    os.environ,
    {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "postgres",
        "PGPASSWORD": "postgres",
        "PGDATABASE": "tomorrow",
        "TOMORROW_IO_API_KEY": "dummy-api-key",
    },
    clear=True,
)
@patch("builtins.open", new_callable=mock_open, read_data=INVALID_TIMESTEP_YAML)
def test_invalid_timestep_unit(mock_file):
    with pytest.raises(RuntimeError, match="Unsupported timestep unit"):
        load_config()

