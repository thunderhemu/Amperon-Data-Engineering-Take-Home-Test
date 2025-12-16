import logging
import sys
from apscheduler.schedulers.blocking import BlockingScheduler

from .config_loader import load_config
from .etl import run_weather_etl

# 🔹 CRITICAL FIX: send logs to stdout for Docker
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def main() -> None:
    """Run bootstrap ETL and start hourly scheduler."""

    logger.info("Scheduler service starting")

    config = load_config()

    # --- Bootstrap run ---
    logger.info("Running initial ETL bootstrap")
    try:
        records = run_weather_etl(config)
        logger.info(
            "Initial ETL completed successfully (records processed=%s)",
            records,
        )
    except Exception:
        logger.exception(
            "Initial ETL failed. Scheduler will still start hourly jobs."
        )

    # --- Scheduled job definition ---
    def scheduled_job() -> None:
        logger.info("Scheduled ETL job started")
        records = run_weather_etl(config)
        logger.info(
            "Scheduled ETL job finished (records processed=%s)",
            records,
        )

    # --- Scheduler setup ---
    scheduler = BlockingScheduler()
    scheduler.add_job(
        func=scheduled_job,
        trigger="interval",
        hours=1,
        id="hourly_weather_scrape",
        coalesce=True,
        misfire_grace_time=300,
    )

    logger.info("Hourly scheduler started (interval=1h)")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down gracefully")
    except Exception:
        logger.critical("Scheduler crashed unexpectedly", exc_info=True)


if __name__ == "__main__":
    main()
