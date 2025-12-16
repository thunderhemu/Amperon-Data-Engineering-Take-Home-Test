## Amperon Data Engineering Take-Home Assignment

This repository contains a containerized ETL (Extract, Transform, Load) system designed to scrape hourly weather forecast and historical data for a set of 10 specified geolocations from the Tomorrow.io API and persist this data into a PostgreSQL database.

The system is automated using an internal Python scheduler and is built using Docker Compose for local, reproducible deployment.

## Getting Started
## Architecture Overview

The system consists of three Dockerized services:

1. **PostgreSQL**
   - Stores all weather data
   - Uses a persistent volume so data survives container restarts

2. **ETL Scheduler (Python)**
   - Fetches data from the Tomorrow.io API
   - Performs an initial bootstrap load on startup
   - Continues execution on an hourly schedule

3. **Jupyter Notebook**
   - Used for querying and visualizing the data

Each service is defined and orchestrated using `docker-compose.yml`.


### Prerequisites

1.  **Docker & Docker Compose:** Must be installed on your system.
2.  **Tomorrow.io API Key:** A Free Tier API key is required.

### Setup Instructions

## Testing & Code Coverage

This project includes unit tests for all core components:
- API client (retry logic, rate-limit handling)
- Configuration loading and validation
- Database persistence and idempotent inserts
- ETL orchestration logic

Entry-point modules and long-running scheduler logic are intentionally excluded
from unit tests, as they contain no business logic and are better validated via
integration testing.

### Run Unit Tests Locally

From the project root, activate the virtual environment and run:

```bash
pytest -v


1.  **Set API Key:** Create a file named `.env` in the root directory of this project and add your Tomorrow.io API key:

    ```
    TOMORROW_IO_API_KEY=YOUR_API_KEY_HERE
    ```

2.  **Build and Run the System:** This command builds the custom Python Docker images and starts the entire three-service stack (`postgres`, `tomorrow` scheduler, and `jupyter`) in detached (background) mode.

    ```bash
    docker compose up --build -d
    ```

### System Operation

Upon running the command, the following occurs:

1.  **Database Starts:** The `postgres` service starts and passes its health check.
2.  **ETL Scheduler Runs:** The `tomorrow` service starts. It immediately performs a **bootstrap run** to populate the database with current data.
3.  **Scheduler Loop:** After the bootstrap run completes (which takes $\approx 50$ seconds due to API rate limiting), the ETL scheduler settles into an **hourly loop** to keep the data fresh.
4.  **Jupyter Notebook:** The analysis environment starts.

### Viewing Logs and Status

To monitor the data loading process or check for errors:

```bash
# View the logs of the ETL scheduler service
docker compose logs -f tomorrow

# Check the status of all running containers
docker compose ps
```

### Cleanup

To stop and remove all containers, networks, and the persistent volume (`pgdata`):

```bash
docker compose down --volumes
```

##  Analysis and Visualization

All analysis is performed in the dedicated Jupyter Notebook container.

### Access

Open your browser to: **`http://localhost:8888`** and open the `analysis.ipynb` file.

### Required Analytical Queries (Solved via SQL and Python)

The system is designed to easily answer the following questions:

#### 1\. Latest Observations (Temperature and Wind Speed)

**SQL Approach:** Uses a Common Table Expression (CTE) to find the maximum `time_stamp` for each `(latitude, longitude)` pair and then joins back to retrieve the corresponding weather data.

#### 2\. Hourly Time Series Plot

**SQL Approach:** Selects all `time_stamp`, `temperature`, and `is_forecast` data for a single, selected location.

**Visualization:** The Python notebook uses **Pandas** to execute the query and **Matplotlib** to generate a line chart showing the continuous time series, visually distinguishing between historical (observed) data and future (forecast) data.

-----

##  Technical Rationale

| Component/Tool | Rationale for Choice |
| :--- | :--- |
| **Python** | **Primary Language:** Required by the assignment. Excellent for data processing, API integration (using `requests`), and database interaction (using `SQLAlchemy`). |
| **PostgreSQL** | **Database:** Robust, open-source, and supports advanced SQL features (like `ON CONFLICT DO NOTHING` for idempotency) and time-series data structures. |
| **Docker Compose** | **Deployment:** Provides a reproducible, isolated environment (database, ETL, analysis) that is easily deployable on any machine, fulfilling the core requirement. |
| **`APScheduler`** | **Scheduling:** Lightweight, Python-native library used to implement the mandatory **hourly** execution loop for the ETL process. |
| **`requests` / Retry Logic** | **Resilience:** Custom retry and backoff logic was built into the API client to handle intermittent network failures. |
| **`time.sleep(5)` Delay** | **Rate Limit Handling:** **Critical fix** required due to the Tomorrow.io Free Tier limit of 3 Requests Per Second (RPS). A 5-second delay between processing each location ensures the 20 total requests are spread out over $\approx 50$ seconds, guaranteeing successful data collection on the free plan. |
| **`depends_on: service_healthy`** | **Orchestration:** Ensures the ETL service (`tomorrow`) waits until the database (`postgres`) is fully initialized, preventing connection errors and improving startup reliability. |