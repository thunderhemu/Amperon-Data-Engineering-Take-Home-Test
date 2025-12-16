import pytest
from unittest.mock import patch, MagicMock
from tomorrow.etl import run_weather_etl


class TestETLOrchestrator:

    @patch('tomorrow.etl.WeatherDB')
    @patch('tomorrow.etl.TomorrowAPIClient')
    def test_etl_full_success(self, MockAPI, MockDB, app_config, sample_db_data):
        """Test full pipeline success where all locations are processed successfully."""

        # Setup Mocks
        mock_api_instance = MockAPI.return_value
        mock_db_instance = MockDB.return_value

        # Configure the API client to return data for every location
        mock_api_instance.fetch_weather_data.return_value = sample_db_data

        # Execution
        run_weather_etl(app_config)

        # Assertions
        expected_api_calls = len(app_config['locations'])

        # 1. Verify API was called for ALL 10 locations
        assert mock_api_instance.fetch_weather_data.call_count == expected_api_calls

        # 2. Verify DB bulk insert was called for ALL 10 locations
        assert mock_db_instance.bulk_insert_weather_data.call_count == expected_api_calls

        # 3. Verify DB bulk insert received data (called with sample data)
        mock_db_instance.bulk_insert_weather_data.assert_called_with(sample_db_data)

    @patch('tomorrow.etl.WeatherDB')
    @patch('tomorrow.etl.TomorrowAPIClient')
    def test_etl_location_failure_resilience(self, MockAPI, MockDB, app_config, sample_db_data):
        """Test resilience: Ensures the pipeline continues after one location fails."""

        mock_api_instance = MockAPI.return_value
        mock_db_instance = MockDB.return_value

        locations = app_config['locations']
        fail_location_index = 2  # Make the 3rd location fail

        # Set up a side effect list: 9 successes, 1 failure
        side_effects = [sample_db_data] * len(locations)
        side_effects[fail_location_index] = Exception("Mock API failure for critical test.")

        mock_api_instance.fetch_weather_data.side_effect = side_effects

        # Execution
        run_weather_etl(app_config)

        # Assertions
        total_locations = len(locations)
        successful_locations = total_locations - 1

        # 1. Verify API was *attempted* for ALL locations
        assert mock_api_instance.fetch_weather_data.call_count == total_locations

        # 2. Verify DB insert was called ONLY for successful locations
        assert mock_db_instance.bulk_insert_weather_data.call_count == successful_locations

        # 3. Verify the ETL did NOT crash (function returned successfully)
        # (Implicitly passed if no exception was raised)