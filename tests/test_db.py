import pytest
from sqlalchemy import text

from tomorrow.db import WeatherDB


class TestWeatherDB:
    """
    Unit tests for WeatherDB persistence layer.
    These tests use a real PostgreSQL database (via Docker)
    and validate idempotent, transactional behavior.
    """

    def test_bulk_insert_success(
        self,
        db_client: WeatherDB,
        db_engine,
        sample_db_data,
    ):
        """
        Test successful bulk insertion of unique records.
        """

        attempted_count = db_client.bulk_insert_weather_data(sample_db_data)

        # The function returns attempted rows, not inserted rows
        assert attempted_count == len(sample_db_data)

        # Verify actual rows in DB
        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM weather_data;")
            ).scalar()

        assert count == len(sample_db_data)

    def test_idempotent_insert(
        self,
        db_client: WeatherDB,
        db_engine,
        sample_db_data,
    ):
        """
        Test ON CONFLICT DO NOTHING behavior.
        Re-inserting the same data must not create duplicates.
        """

        db_client.bulk_insert_weather_data(sample_db_data)
        db_client.bulk_insert_weather_data(sample_db_data)

        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM weather_data;")
            ).scalar()

        # Must still be the original number of rows
        assert count == len(sample_db_data)

    def test_empty_input_returns_zero(
        self,
        db_client: WeatherDB,
        db_engine,
    ):
        """
        Empty input should be handled gracefully.
        """

        attempted_count = db_client.bulk_insert_weather_data([])

        assert attempted_count == 0

        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM weather_data;")
            ).scalar()

        assert count == 0

    def test_rows_missing_required_fields_are_skipped(
        self,
        db_client: WeatherDB,
        db_engine,
    ):
        """
        Rows missing required fields must be ignored safely.
        """

        bad_rows = [
            {"latitude": 25.9, "longitude": -97.4},  # missing required fields
            {"latitude": 25.8},  # missing everything else
        ]

        attempted_count = db_client.bulk_insert_weather_data(bad_rows)

        # No valid rows should be attempted
        assert attempted_count == 0

        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM weather_data;")
            ).scalar()

        assert count == 0

    def test_partial_valid_rows_only_insert_valid(
        self,
        db_client: WeatherDB,
        db_engine,
        sample_db_data,
    ):
        """
        Mixed valid + invalid rows:
        only valid rows should be inserted.
        """

        mixed_rows = sample_db_data + [
            {"latitude": 25.7, "longitude": -97.3},  # invalid row
        ]

        attempted_count = db_client.bulk_insert_weather_data(mixed_rows)

        # Only valid rows are counted as attempted
        assert attempted_count == len(sample_db_data)

        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM weather_data;")
            ).scalar()

        assert count == len(sample_db_data)

    def test_engine_close(
        self,
        db_client: WeatherDB,
    ):
        """
        Engine should close cleanly without error.
        """

        db_client.close()

        # Disposing twice should not raise
        db_client.close()
