# --- File: tomorrow/config_loader.py (Corrected) ---

import os
import sys
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Loads configuration from YAML and environment variables."""
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("Configuration file config/config.yaml not found. Check repository structure.")
        sys.exit(1)

    # Database config from environment (Standard Docker practice)
    config['db'] = {
        'host': os.getenv('PGHOST'), 'port': os.getenv('PGPORT'),
        'user': os.getenv('PGUSER'), 'password': os.getenv('PGPASSWORD'),
        'database': os.getenv('PGDATABASE'),
    }

    # API key is a secret, sourced only from the environment
    config['api']['key'] = os.getenv('TOMORROW_IO_API_KEY')

    if not config['api']['key']:
        logger.critical("TOMORROW_IO_API_KEY not set. Cannot authenticate with API.")
        sys.exit(1)

    # 1. Get the timestep string (e.g., '1h' or '30m')
    timestep_str = config['api']['timesteps'][0]

    # 2. Extract value and unit
    value = int(timestep_str[:-1])  # Takes '1h' -> 1
    unit = timestep_str[-1].lower()  # Takes '1h' -> 'h'

    # 3. Calculate minutes
    if unit == 'h':
        minutes = value * 60
    elif unit == 'm':
        minutes = value
    else:
        logger.critical(f"Unsupported timestep unit in config: {timestep_str}. Only 'h' or 'm' supported.")
        sys.exit(1)

    # 4. Inject the calculated value into the API config
    config['api']['timesteps_minutes'] = minutes

    logger.debug(f"API timestep set to {minutes} minutes.")

    return config


# Load config immediately upon module import
CONFIG = load_config()