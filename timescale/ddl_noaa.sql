CREATE EXTENSION IF NOT EXISTS timescaledb;

-- NOAA daily weather observations (GHCND) loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.noaa_weather_timeseries (
    tenant_id TEXT NOT NULL,
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
    ingest_job_id TEXT,
    ingest_run_id TEXT,
    ingest_batch_id TEXT,
    PRIMARY KEY (tenant_id, station_id, observation_date, element)
);

SELECT create_hypertable('public.noaa_weather_timeseries', 'observation_date', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_noaa_element_date
    ON public.noaa_weather_timeseries (tenant_id, element, observation_date DESC);

CREATE INDEX IF NOT EXISTS idx_noaa_station_date
    ON public.noaa_weather_timeseries (tenant_id, station_id, observation_date DESC);

-- Enable native compression and retention policies
ALTER TABLE IF EXISTS public.noaa_weather_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'tenant_id,station_id,element',
        timescaledb.compress_orderby = 'observation_date DESC'
    );

-- Compress data older than 30 days; retain for 5 years
SELECT add_compression_policy('public.noaa_weather_timeseries', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.noaa_weather_timeseries', INTERVAL '1825 days', if_not_exists => TRUE);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'noaa_weather_timeseries_wk'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.noaa_weather_timeseries_wk
            WITH (timescaledb.continuous) AS
            SELECT
                tenant_id,
                station_id,
                element,
                time_bucket('7 days', observation_date) AS bucket_start,
                AVG(value) AS value_avg,
                MIN(value) AS value_min,
                MAX(value) AS value_max,
                COUNT(*) AS sample_count,
                MAX(ingest_ts) AS latest_ingest_ts
            FROM public.noaa_weather_timeseries
            GROUP BY tenant_id, station_id, element, bucket_start
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_noaa_weather_timeseries_wk
    ON public.noaa_weather_timeseries_wk (tenant_id, station_id, element, bucket_start DESC);

SELECT add_continuous_aggregate_policy(
    'public.noaa_weather_timeseries_wk',
    start_offset => INTERVAL '2 years',
    end_offset => INTERVAL '7 days',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_retention_policy('public.noaa_weather_timeseries_wk', INTERVAL '5 years', if_not_exists => TRUE);
