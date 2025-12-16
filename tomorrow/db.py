import logging
from sqlalchemy import create_engine, Table, Column, Float, DateTime, Boolean, MetaData
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class WeatherDB:
    """Manages high-performance, idempotent insertion into the PostgreSQL database."""

    def __init__(self, db_config: Dict[str, str]):
        db_url = (f"postgresql://{db_config['user']}:{db_config['password']}@"
                  f"{db_config['host']}:{db_config['port']}/{db_config['database']}")

        self.engine = create_engine(db_url, pool_size=10, max_overflow=5, pool_timeout=30)
        self.metadata = MetaData()
        self.weather_table = self._define_table()
        logger.info("DB: Engine initialized.")

    def _define_table(self):
        """Maps Python representation to the 'weather_data' SQL table."""
        return Table(
            'weather_data', self.metadata,
            Column('latitude', Float(10, 6), nullable=False),
            Column('longitude', Float(10, 6), nullable=False),
            Column('time_stamp', DateTime(timezone=True), nullable=False),
            Column('is_forecast', Boolean, nullable=False),
            Column('temperature', Float),
            Column('wind_speed', Float),
            Column('humidity', Float),
            Column('precipitation_type', Float),
            extend_existing=True
        )

    def bulk_insert_weather_data(self, data: List[Dict[str, Any]]) -> int:
        """Executes bulk insert with PostgreSQL's ON CONFLICT DO NOTHING."""
        if not data:
            return 0

        try:
            # FIX: Use engine.begin() for automatic transaction management (commit/rollback)
            with self.engine.begin() as conn:
                insert_stmt = insert(self.weather_table).values(data)

                # This requires a UNIQUE constraint on (latitude, longitude, time_stamp) in PostgreSQL
                on_conflict_stmt = insert_stmt.on_conflict_do_nothing(
                    index_elements=['latitude', 'longitude', 'time_stamp']
                )

                conn.execute(on_conflict_stmt)

                return len(data)

        except Exception:
            # This will be caught by the ETL logger, but we re-raise it here
            logger.error("DB: Bulk insert failed.", exc_info=True)
            raise