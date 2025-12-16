-- init-db.sql
-- Define schema using best practices for time-series and auditability.
CREATE TABLE IF NOT EXISTS weather_data (
    id BIGSERIAL PRIMARY KEY,

    -- Auditability: Time when the ETL job successfully saved the row.
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Location fields for unique identification (high precision).
    latitude NUMERIC(10, 6) NOT NULL,
    longitude NUMERIC(10, 6) NOT NULL,

    -- Measurement Time: The time of the observation/forecast from the API.
    time_stamp TIMESTAMP WITH TIME ZONE NOT NULL,

    is_forecast BOOLEAN NOT NULL,

    -- Core Weather Metrics (using NUMERIC/FLOAT for flexibility)
    temperature NUMERIC,
    wind_speed NUMERIC,
    humidity NUMERIC,
    precipitation_type INTEGER,

    -- Idempotency constraint: Prevents duplicate rows for the same measurement/location.
    UNIQUE (latitude, longitude, time_stamp)
);

-- Indexing Strategy: Speeds up queries filtering by location and fetching the latest data.
CREATE INDEX idx_location_time ON weather_data (latitude, longitude, time_stamp DESC);