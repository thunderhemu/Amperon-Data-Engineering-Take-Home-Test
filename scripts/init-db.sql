CREATE TABLE IF NOT EXISTS weather_data (
    id BIGSERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL,
    time_stamp TIMESTAMPTZ NOT NULL,
    is_forecast BOOLEAN NOT NULL,
    temperature NUMERIC,
    wind_speed NUMERIC,
    humidity NUMERIC,
    precipitation_type INTEGER,
    CONSTRAINT uq_weather_unique
        UNIQUE (latitude, longitude, time_stamp, is_forecast)
);

CREATE INDEX IF NOT EXISTS idx_weather_location_time
ON weather_data (latitude, longitude, time_stamp DESC);
