import logging
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from .config_loader import CONFIG
from .etl import run_weather_etl

logger = logging.getLogger(__name__)


def configure_logging():
    """Sets up root logging handler for consistent output."""
    # Use INFO level for the scheduler service
    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        stream=sys.stdout
    )


def main():
    """Initializes logging, runs the bootstrap ETL, and starts the scheduler."""
    configure_logging()

    logger.info("SCHEDULER: Service starting up. Environment validation passed.")

    # 1. Bootstrap: Run ETL once immediately
    logger.info("SCHEDULER: Running initial ETL for bootstrap...")
    try:
        run_weather_etl(CONFIG)
        logger.info("SCHEDULER: Initial ETL completed successfully.")
    except Exception:
        logger.error("SCHEDULER: Initial ETL run failed. Scheduler will proceed with hourly schedule.", exc_info=True)

    # 2. Start the hourly job
    scheduler = BlockingScheduler()
    scheduler.add_job(
        func=lambda: run_weather_etl(CONFIG),
        trigger='interval',
        hours=1,
        id='hourly_weather_scrape'
    )

    logger.info("SCHEDULER: Hourly scheduler loop started (Interval: 1 hour).")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("SCHEDULER: Shutting down gracefully.")
    except Exception as e:
        logger.critical(f"SCHEDULER: Critical error during operation: {e}", exc_info=True)


if __name__ == '__main__':
    main()