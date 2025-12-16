# File: __main__.py

import logging
import os
import sys

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.append(os.path.dirname(os.path.abspath(__file__))) 

from tomorrow.etl import run_weather_etl
from tomorrow.config_loader import CONFIG # Assuming CONFIG is loaded here

logging.info("WELCOME! Starting ETL process...")

if __name__ == "__main__":
    run_weather_etl(CONFIG)
    logging.info("ETL process finished.")