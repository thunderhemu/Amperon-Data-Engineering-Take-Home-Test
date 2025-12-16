import pytest
from unittest.mock import patch, MagicMock

import tomorrow.scheduler as scheduler_module


def test_scheduler_bootstrap_and_start():
    """Scheduler should run bootstrap ETL and start the scheduler."""

    mock_config = {"dummy": "config"}

    with patch.object(scheduler_module, "load_config", return_value=mock_config), \
         patch.object(scheduler_module, "run_weather_etl", return_value=42) as mock_etl, \
         patch.object(scheduler_module, "BlockingScheduler") as mock_scheduler_cls:

        mock_scheduler = MagicMock()
        mock_scheduler_cls.return_value = mock_scheduler

        # Act
        scheduler_module.main()

        # Bootstrap ETL should run once
        mock_etl.assert_called_once_with(mock_config)

        # Scheduler should be configured
        mock_scheduler.add_job.assert_called_once()
        mock_scheduler.start.assert_called_once()

def test_scheduler_graceful_shutdown():
    """Scheduler should shut down cleanly on KeyboardInterrupt."""

    with patch.object(scheduler_module, "load_config", return_value={}), \
         patch.object(scheduler_module, "run_weather_etl"), \
         patch.object(scheduler_module, "BlockingScheduler") as mock_scheduler_cls:

        mock_scheduler = MagicMock()
        mock_scheduler.start.side_effect = KeyboardInterrupt
        mock_scheduler_cls.return_value = mock_scheduler

        # Should NOT raise
        scheduler_module.main()

        mock_scheduler.start.assert_called_once()

def test_scheduler_unexpected_crash_logged():
    """Unexpected scheduler crash should be logged."""

    with patch.object(scheduler_module, "load_config", return_value={}), \
         patch.object(scheduler_module, "run_weather_etl"), \
         patch.object(scheduler_module, "BlockingScheduler") as mock_scheduler_cls, \
         patch.object(scheduler_module.logger, "critical") as mock_critical:

        mock_scheduler = MagicMock()
        mock_scheduler.start.side_effect = RuntimeError("Boom")
        mock_scheduler_cls.return_value = mock_scheduler

        scheduler_module.main()

        mock_critical.assert_called_once()
