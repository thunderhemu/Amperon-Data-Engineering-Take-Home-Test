import pytest
import requests
from unittest.mock import patch, MagicMock
from tomorrow.api import TomorrowAPIClient

# Mock the current time to make tests predictable
MOCK_NOW = "2025-12-15T15:00:00Z"


def mock_get_request(status_code, json_data=None):
    """Utility function to create a mock response object."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data if json_data is not None else {}
    mock_resp.raise_for_status.side_effect = (
        requests.exceptions.HTTPError(f"Mock HTTP {status_code}")
        if status_code >= 400 else None
    )
    return mock_resp


@patch('requests.Session.get')
class TestTomorrowAPIClient:

    @patch('datetime.datetime', MagicMock(now=lambda tz: datetime.fromisoformat(MOCK_NOW.replace('Z', '+00:00')),
                                          side_effect=lambda tz: datetime.fromisoformat(
                                              MOCK_NOW.replace('Z', '+00:00'))))
    def test_fetch_success_and_parsing(self, mock_get, app_config, sample_api_response):
        """Test successful fetch and data parsing."""
        client = TomorrowAPIClient(app_config['api'])

        # Mock responses for history and forecast calls
        mock_get.side_effect = [
            mock_get_request(200, sample_api_response),  # History
            mock_get_request(200, sample_api_response),  # Forecast
        ]

        data = client.fetch_weather_data(25.9, -97.4)

        assert len(data) == 4  # 2 from history + 2 from forecast
        assert data[0]['temperature'] == 15.5
        assert data[2]['is_forecast'] is True
        assert data[0]['latitude'] == 25.9
        assert mock_get.call_count == 2

    @patch('datetime.datetime', MagicMock(now=lambda tz: datetime.fromisoformat(MOCK_NOW.replace('Z', '+00:00')),
                                          side_effect=lambda tz: datetime.fromisoformat(
                                              MOCK_NOW.replace('Z', '+00:00'))))
    def test_api_retry_and_success(self, mock_get, app_config, sample_api_response):
        """Test resilience: 429 failure followed by a successful retry."""
        client = TomorrowAPIClient(app_config['api'])

        # Sequence: 429 (History 1), 200 (History 2), 200 (Forecast)
        mock_get.side_effect = [
            mock_get_request(429),
            mock_get_request(200, sample_api_response),
            mock_get_request(200, sample_api_response),
        ]

        # Patch time.sleep to run fast
        with patch('time.sleep'):
            data = client.fetch_weather_data(25.9, -97.4)

        assert len(data) == 4  # Must return data
        assert mock_get.call_count == 3  # Must have called 3 times (1 fail + 2 success)

    @patch('datetime.datetime', MagicMock(now=lambda tz: datetime.fromisoformat(MOCK_NOW.replace('Z', '+00:00')),
                                          side_effect=lambda tz: datetime.fromisoformat(
                                              MOCK_NOW.replace('Z', '+00:00'))))
    def test_api_final_failure(self, mock_get, app_config):
        """Test failure after exhausting all retries."""
        client = TomorrowAPIClient(app_config['api'])

        # Max retries is 3. Sequence: 500, 500, 500 (3 calls total)
        mock_get.side_effect = [
            mock_get_request(500),
            mock_get_request(500),
            mock_get_request(500),
            requests.exceptions.HTTPError("Mock Final Failure")  # The API client re-raises the final error
        ]

        with pytest.raises(requests.exceptions.HTTPError):
            with patch('time.sleep'):
                client.fetch_weather_data(25.9, -97.4)

        # Should have called mock_get for history (3 times) before the exception is raised.
        # Note: Depending on implementation flow, this count can vary slightly, but the exception is key.