import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from tomorrow.api import TomorrowAPIClient


MOCK_NOW = datetime(2025, 12, 15, 15, 0, tzinfo=timezone.utc)


def mock_get_request(status_code, json_data=None):
    """Create a mock HTTP response with proper HTTPError behavior."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data or {}

    if status_code >= 400:
        error = requests.exceptions.HTTPError(f"Mock HTTP {status_code}")
        error.response = mock_resp      # ✅ CRITICAL LINE
        mock_resp.raise_for_status.side_effect = error
    else:
        mock_resp.raise_for_status.return_value = None

    return mock_resp



@patch("requests.Session.get")
class TestTomorrowAPIClient:

    @patch("tomorrow.api.datetime")
    def test_fetch_success_and_parsing(
        self, mock_datetime, mock_get, app_config
    ):
        """Successful API call and correct parsing of hourly data."""

        mock_datetime.now.return_value = MOCK_NOW

        mock_get.return_value = mock_get_request(
            200,
            {
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
            },
        )

        client = TomorrowAPIClient(app_config["api"])
        data = client.fetch_weather_data(25.9, -97.4)

        assert len(data) == 2
        assert data[0]["temperature"] == 15.5
        assert data[0]["is_forecast"] is False
        assert data[1]["is_forecast"] is True
        assert mock_get.call_count == 1


    @patch("tomorrow.api.datetime")
    def test_retry_on_5xx_then_success(
        self, mock_datetime, mock_get, app_config
    ):
        """Retry on transient server error (500) and succeed."""

        mock_datetime.now.return_value = MOCK_NOW

        mock_get.side_effect = [
            mock_get_request(500),
            mock_get_request(
                200,
                {
                    "timelines": {
                        "hourly": [
                            {
                                "time": "2025-12-15T15:00:00Z",
                                "values": {"temperature": 15.0},
                            }
                        ]
                    }
                },
            ),
        ]

        client = TomorrowAPIClient(app_config["api"])

        with patch("time.sleep"):
            data = client.fetch_weather_data(25.9, -97.4)

        assert len(data) == 1
        assert mock_get.call_count == 2


    def test_429_rate_limit_hard_failure(self, mock_get, app_config):
        """429 errors must fail immediately (no retries)."""

        mock_get.return_value = mock_get_request(429)

        client = TomorrowAPIClient(app_config["api"])

        with pytest.raises(requests.exceptions.HTTPError):
            client.fetch_weather_data(25.9, -97.4)

        assert mock_get.call_count == 1


    def test_retry_exhaustion_raises(self, mock_get, app_config):
        """All retries exhausted → exception is raised."""

        mock_get.side_effect = [
            mock_get_request(500),
            mock_get_request(500),
            mock_get_request(500),
        ]

        client = TomorrowAPIClient(app_config["api"])

        with patch("time.sleep"), pytest.raises(requests.exceptions.HTTPError):
            client.fetch_weather_data(25.9, -97.4)

        assert mock_get.call_count == app_config["api"]["max_retries"]
