import requests
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class TomorrowAPIClient:
    """
    Tomorrow.io v4 Weather Forecast client.

    """

    def __init__(self, api_config: Dict[str, Any]):
        self.base_url = api_config["base_url"]
        self.endpoint = api_config["forecast_endpoint"]
        self.key = api_config["key"]

        self.fields = ",".join(api_config["fields"])
        self.timesteps = api_config["timesteps"]
        self.units = api_config["units"]

        self.max_retries = api_config["max_retries"]
        self.timeout = api_config["timeout_seconds"]
        self.retry_backoff = api_config.get("retry_backoff_seconds", 2)

        self.session = requests.Session()

        logger.info("Tomorrow.io Forecast API client initialized")

    def _call_api(self, params: Dict[str, Any], location: str) -> Dict[str, Any]:
        url = f"{self.base_url}{self.endpoint}"
        params = dict(params)
        params["apikey"] = self.key

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response else None

                # HARD STOP on rate limit
                if status == 429:
                    logger.critical(
                        "Tomorrow.io rate limit exceeded for %s. "
                        "Free tier allows only 25 requests/hour. "
                        "Skipping until next scheduled run.",
                        location,
                    )
                    raise

                # Retry only transient server errors
                if status in {500, 502, 503, 504} and attempt < self.max_retries - 1:
                    wait = self.retry_backoff ** attempt
                    logger.warning(
                        "Transient API error %s for %s. Retrying in %ss",
                        status, location, wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "API failure for %s (status=%s)", location, status
                    )
                    raise

            except requests.exceptions.RequestException as exc:
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        raise RuntimeError(f"API failed after retries for {location}")

    def fetch_weather_data(self, lat: float, lon: float) -> List[Dict[str, Any]]:
        """
        Fetch hourly weather data from 24h ago to 5 days in the future
        using /v4/weather/forecast.
        """
        location = f"{lat},{lon}"
        now = datetime.now(timezone.utc)

        params = {
            "location": location,
            "timesteps": self.timesteps,
            "units": self.units,
            "fields": self.fields,
            "startTime": (now - timedelta(hours=24)).isoformat().replace("+00:00", "Z"),
            "endTime": (now + timedelta(days=5)).isoformat().replace("+00:00", "Z"),
        }

        logger.info("Fetching forecast for %s", location)
        raw = self._call_api(params, location)

        intervals = raw.get("timelines", {}).get("hourly", [])
        if not intervals:
            logger.warning("No hourly data returned for %s", location)
            return []

        records: List[Dict[str, Any]] = []
        for interval in intervals:
            values = interval.get("values", {})
            records.append(
                {
                    "time_stamp": datetime.fromisoformat(
                        interval["time"].replace("Z", "+00:00")
                    ),
                    "is_forecast": interval["time"] > now.isoformat(),
                    "temperature": values.get("temperature"),
                    "wind_speed": values.get("windSpeed"),
                    "humidity": values.get("humidity"),
                    "precipitation_type": values.get("precipitationType"),
                }
            )

        logger.info("Parsed %d hourly records for %s", len(records), location)
        return records

    def close(self):
        self.session.close()
