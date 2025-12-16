import logging
import time
from typing import Dict, Any

from .api import TomorrowAPIClient
from .db import WeatherDB

logger = logging.getLogger(__name__)


def run_weather_etl(config: Dict[str, Any]) -> int:
    """
    Orchestrates the weather ETL pipeline.
    Returns total number of records attempted to load.
    """

    try:
        api_client = TomorrowAPIClient(config["api"])
        db_client = WeatherDB(config["db"])
    except Exception:
        logger.exception("ETL initialization failed")
        raise

    locations = config.get("locations")
    if not isinstance(locations, list):
        raise RuntimeError("config.locations must be a list")

    total_records_processed = 0
    sleep_seconds = config.get("rate_limit_sleep_seconds", 2)

    logger.info("ETL started for %d locations", len(locations))

    for location in locations:
        if "lat" not in location or "lon" not in location:
            logger.warning("Skipping invalid location entry: %s", location)
            continue

        lat, lon = location["lat"], location["lon"]
        location_str = f"{lat},{lon}"

        try:
            logger.info("ETL processing location %s", location_str)

            records = api_client.fetch_weather_data(lat, lon)
            if not records:
                logger.warning("No data returned for %s", location_str)
                continue

            for record in records:
                record["latitude"] = lat
                record["longitude"] = lon

            db_client.bulk_insert_weather_data(records)
            total_records_processed += len(records)

            logger.info(
                "ETL loaded %d records for %s",
                len(records),
                location_str,
            )

        except Exception:
            logger.exception("ETL failed for location %s", location_str)

        time.sleep(sleep_seconds)

    api_client.close()
    db_client.close()

    logger.info(
        "ETL completed successfully. Total records processed: %d",
        total_records_processed,
    )

    return total_records_processed
