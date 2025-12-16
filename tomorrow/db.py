import logging
from typing import List, Dict, Any

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


class WeatherDB:
    """PostgreSQL persistence with idempotent inserts."""

    def __init__(self, db_config: Dict[str, str]):
        db_url = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

        self.engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=5,
            pool_timeout=30,
            future=True,
        )

        self.metadata = MetaData()

        self.weather_table = Table(
            "weather_data",
            self.metadata,
            autoload_with=self.engine,
        )

        logger.info("DB: Engine initialized and schema reflected")

    def bulk_insert_weather_data(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        required = {"latitude", "longitude", "time_stamp", "is_forecast"}
        rows = [r for r in rows if required.issubset(r)]

        if not rows:
            logger.warning("DB: No valid rows to insert")
            return 0

        try:
            with self.engine.begin() as conn:
                stmt = insert(self.weather_table).values(rows)

                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[
                        "latitude",
                        "longitude",
                        "time_stamp",
                        "is_forecast",
                    ]
                )

                conn.execute(stmt)

            logger.info(
                "DB: Insert attempted for %d rows (duplicates skipped)",
                len(rows),
            )
            return len(rows)

        except Exception:
            logger.exception("DB: Bulk insert failed")
            raise

    def close(self) -> None:
        self.engine.dispose()
