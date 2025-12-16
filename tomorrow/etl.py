import logging
import sys
import traceback
from typing import Dict, Any
import time
from .api import TomorrowAPIClient
from .db import WeatherDB
from .config_loader import CONFIG

# Setting to DEBUG ensures maximum output for troubleshooting.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def run_weather_etl(config: Dict[str, Any]):
    """
    Orchestrates the data pipeline. Handles location iteration and failure recovery.
    """
    try:
        api_client = TomorrowAPIClient(config['api'])
        db_client = WeatherDB(config['db'])
    except Exception:
        logger.error("ETL: Failed to initialize clients. Terminating run.", exc_info=True)
        return

    locations = config['locations']
    total_records_processed = 0

    logger.info(f"ETL: Starting run for {len(locations)} locations.")

    for location in locations:
        lat, lon = location['lat'], location['lon']
        location_str = f"{lat},{lon}"

        try:
            logger.info(f"ETL: --- Processing {location_str} ---")

            # 1. Extract
            records = api_client.fetch_weather_data(lat, lon)

            if not records:
                logger.warning(f"ETL: No data returned for {location_str}. Skipping load.")
                continue

            # FIX: Inject lat/lon into every record for DB schema consistency
            final_records = []
            for record in records:
                record['latitude'] = lat
                record['longitude'] = lon
                final_records.append(record)

            logger.info(f"ETL: Extracted {len(final_records)} records. Loading to DB...")

            # 2. Load
            db_client.bulk_insert_weather_data(final_records)

            total_records_processed += len(final_records)
            logger.info(f"ETL: Load completed for {location_str}. Loaded {len(final_records)} records.")

        except Exception as e:  # Catch the exception object
            # CATCH AND PRINT THE EXCEPTION FOR DIAGNOSTICS
            print(f"ETL: FAILED to process location {location_str}. Error: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

            # Original logging for completeness
            logger.error(f"ETL: FAILED to process location {location_str}. Continuing to next location.", exc_info=True)

        # Since we make 2 requests per location (history+forecast), a 2 second delay ensures we stay below 1 RPS.
        time.sleep(2)

    logger.info(f"ETL: Run complete. Total records processed across all locations: {total_records_processed}.")