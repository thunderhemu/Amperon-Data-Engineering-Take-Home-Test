import requests
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class TomorrowAPIClient:
    """A resilient API client with explicit error handling and exponential backoff."""

    def __init__(self, api_config: Dict[str, Any]):
        self.base_url = api_config['base_url']
        self.key = api_config['key']
        self.config = api_config
        self.session = requests.Session()
        self.max_retries = api_config['max_retries']
        self.timeout = api_config['timeout_seconds']
        logger.info("API Client initialized.")

    def _call_api(self, endpoint: str, params: Dict[str, Any], location_str: str) -> Dict[str, Any]:
        """Handles HTTP request, retries, and error surfacing."""
        full_url = f"{self.base_url}{endpoint}"
        params['apikey'] = self.key

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(full_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                status = response.status_code
                if status in [429, 500, 502, 503, 504] and attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"API: Failed ({status}) for {location_str}. "
                        f"Retrying in {wait_time}s (Attempt {attempt + 1})."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"API: Final failure for {location_str} (Status: {status}).")
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"API: Network or timeout error for {location_str}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        raise RuntimeError(f"API: Failed to fetch data for {location_str} after all retries.")

    def _extract_intervals(self, raw_data: Dict[str, Any], is_forecast: bool) -> List[Dict[str, Any]]:
        """Safely extracts and transforms the required fields from the JSON response."""
        data = []
        try:
            intervals = raw_data['data']['timelines'][0]['intervals']
            for interval in intervals:
                values = interval['values']
                data.append({
                    'time_stamp': datetime.fromisoformat(interval['startTime'].replace('Z', '+00:00')),
                    'is_forecast': is_forecast,
                    'temperature': values.get('temperature'),
                    'wind_speed': values.get('windSpeed'),
                    'humidity': values.get('humidity'),
                    'precipitation_type': values.get('precipitationType'),
                })
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"API: Failed to parse response structure. Error: {e}", exc_info=True)
            return []

        return data

    def fetch_weather_data(self, lat: float, lon: float) -> List[Dict[str, Any]]:
        """Coordinates fetching both history and forecast."""
        location_str = f"{lat},{lon}"
        now_utc = datetime.now(timezone.utc)

        REQUIRED_FIELDS = 'temperature,windSpeed,humidity,precipitationType'
        TIMELINE_ENDPOINT = '/v4/timelines'

        history_end = now_utc
        history_start = now_utc - timedelta(hours=24)
        forecast_start = now_utc + timedelta(minutes=self.config['timesteps_minutes'])
        forecast_end = now_utc + timedelta(days=5)

        # 1. History (24h back)
        history_params = {
            'location': location_str, 'timesteps': self.config['timesteps'], 'units': self.config['units'],
            'fields': REQUIRED_FIELDS,
            'startTime': history_start.isoformat().replace('+00:00', 'Z'),
            'endTime': history_end.isoformat().replace('+00:00', 'Z')
        }
        logger.info(f"Fetching history for {location_str}...")
        history_raw = self._call_api(TIMELINE_ENDPOINT, history_params, location_str + ' (History)')
        history_data = self._extract_intervals(history_raw, is_forecast=False)

        # 2. Forecast (5 days forward)
        forecast_params = {
            'location': location_str, 'timesteps': self.config['timesteps'], 'units': self.config['units'],
            'fields': REQUIRED_FIELDS,
            'startTime': forecast_start.isoformat().replace('+00:00', 'Z'),
            'endTime': forecast_end.isoformat().replace('+00:00', 'Z')
        }
        logger.info(f"Fetching forecast for {location_str}...")
        forecast_raw = self._call_api(TIMELINE_ENDPOINT, forecast_params, location_str + ' (Forecast)')
        forecast_data = self._extract_intervals(forecast_raw, is_forecast=True)

        # 3. Consolidate and Return
        all_weather_data = history_data + forecast_data
        logger.info(f"Successfully fetched {len(all_weather_data)} total data points for {location_str}.")
        return all_weather_data