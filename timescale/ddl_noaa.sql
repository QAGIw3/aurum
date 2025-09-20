CREATE EXTENSION IF NOT EXISTS timescaledb;

-- NOAA daily weather observations (GHCND) loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.noaa_weather_timeseries (
    station_id TEXT NOT NULL,
    observation_date DATE NOT NULL,
    element TEXT NOT NULL,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    elevation_m DOUBLE PRECISION,
    dataset TEXT NOT NULL,
    value DOUBLE PRECISION,
    raw_value TEXT,
    unit TEXT,
    observation_time TIMESTAMPTZ,
    measurement_flag TEXT,
    quality_flag TEXT,
    source_flag TEXT,
    attributes TEXT,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (station_id, observation_date, element)
);

SELECT create_hypertable('public.noaa_weather_timeseries', 'observation_date', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_noaa_element_date
    ON public.noaa_weather_timeseries (element, observation_date DESC);

CREATE INDEX IF NOT EXISTS idx_noaa_station_date
    ON public.noaa_weather_timeseries (station_id, observation_date DESC);

-- Enable native compression and retention policies
ALTER TABLE IF EXISTS public.noaa_weather_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'station_id,element',
        timescaledb.compress_orderby = 'observation_date DESC'
    );

-- Compress data older than 30 days; retain for 5 years
SELECT add_compression_policy('public.noaa_weather_timeseries', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.noaa_weather_timeseries', INTERVAL '1825 days', if_not_exists => TRUE);

