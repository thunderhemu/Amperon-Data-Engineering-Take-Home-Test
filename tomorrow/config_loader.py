import os
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Load configuration from YAML and environment variables."""

    # --- Load YAML ---
    try:
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "config.yaml"
        )
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.critical("config/config.yaml not found")
        raise RuntimeError("Configuration file missing")

    # --- Validate YAML structure ---
    required_sections = ["api", "locations"]
    missing = [s for s in required_sections if s not in config]
    if missing:
        logger.critical(f"Missing required config sections: {missing}")
        raise RuntimeError("Invalid configuration file")

    # --- Load DB config from environment ---
    db_config = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "database": os.getenv("PGDATABASE"),
    }

    missing_db = [k for k, v in db_config.items() if not v]
    if missing_db:
        logger.critical(f"Missing DB environment variables: {missing_db}")
        raise RuntimeError("Database configuration incomplete")

    config["db"] = db_config

    # --- Load API key ---
    api_key = os.getenv("TOMORROW_IO_API_KEY")
    if not api_key:
        logger.critical("TOMORROW_IO_API_KEY not set")
        raise RuntimeError("Missing API key")

    config["api"]["key"] = api_key

    # --- Validate and parse timestep ---
    timesteps = config["api"].get("timesteps")
    if not timesteps or not isinstance(timesteps, list):
        raise RuntimeError("api.timesteps must be a non-empty list")

    timestep_str = timesteps[0]
    value = int(timestep_str[:-1])
    unit = timestep_str[-1].lower()

    if unit == "h":
        minutes = value * 60
    elif unit == "m":
        minutes = value
    else:
        raise RuntimeError(f"Unsupported timestep unit: {timestep_str}")

    config["api"]["timesteps_minutes"] = minutes

    logger.debug("API timestep resolved to %s minutes", minutes)

    return config
