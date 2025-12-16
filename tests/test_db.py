import pytest
from tomorrow.db import WeatherDB
from sqlalchemy import text
from psycopg2.errors import UniqueViolation


class TestWeatherDB:

    def test_bulk_insert_success(self, db_client: WeatherDB, db_engine, sample_db_data):
        """Test successful bulk insertion of unique records."""

        attempted_count = db_client.bulk_insert_weather_data(sample_db_data)

        assert attempted_count == 3

        # Verify count in the actual database
        with db_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM weather_data;")).scalar()
            assert result == 3

    def test_idempotency_check(self, db_client: WeatherDB, db_engine, sample_db_data):
        """Test insertion idempotency using ON CONFLICT DO NOTHING (Senior Practice)."""

        # 1. First insert (should succeed fully)
        db_client.bulk_insert_weather_data(sample_db_data)

        # 2. Second insert (should conflict and insert nothing new)
        db_client.bulk_insert_weather_data(sample_db_data)

        # Verify final count in the actual database is the initial amount
        with db_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM weather_data;")).scalar()
            assert result == 3  # Should still be 3, proving idempotency