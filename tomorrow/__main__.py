# File: tomorrow/__main__.py

import logging
import os
import sys

from tomorrow.etl import run_weather_etl
from tomorrow.config_loader import load_config


def configure_logging() -> None:
    """Configure application-wide logging."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    """Application entry point."""
    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Weather ETL process starting")

    try:
        config = load_config()
        run_weather_etl(config)
    except Exception:
        logger.exception("Weather ETL process failed")
        sys.exit(1)

    logger.info("Weather ETL process finished successfully")


if __name__ == "__main__":
    main()
