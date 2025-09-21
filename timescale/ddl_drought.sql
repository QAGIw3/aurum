CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Daily drought index zonal statistics hydrated from Kafka
CREATE TABLE IF NOT EXISTS public.drought_index_timeseries (
    series_id TEXT NOT NULL,
    region_type TEXT NOT NULL,
    region_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    index TEXT NOT NULL,
    timescale TEXT NOT NULL,
    valid_time DATE NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    as_of TIMESTAMPTZ,
    source_url TEXT,
    ingest_job_id TEXT,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (series_id, valid_time)
);

SELECT create_hypertable('public.drought_index_timeseries', 'valid_time', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_drought_index_region_date
    ON public.drought_index_timeseries (region_type, region_id, valid_time DESC);

CREATE INDEX IF NOT EXISTS idx_drought_index_dataset
    ON public.drought_index_timeseries (dataset, index, timescale, valid_time DESC);

ALTER TABLE IF EXISTS public.drought_index_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'valid_time DESC'
    );

SELECT add_compression_policy('public.drought_index_timeseries', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.drought_index_timeseries', INTERVAL '3 years', if_not_exists => TRUE);

-- Continuous aggregate for weekly means by region
CREATE MATERIALIZED VIEW IF NOT EXISTS public.drought_index_timeseries_wk
WITH (timescaledb.continuous) AS
SELECT
    series_id,
    region_type,
    region_id,
    dataset,
    index,
    timescale,
    time_bucket('7 days', valid_time) AS bucket_start,
    AVG(value) AS avg_value,
    MAX(value) AS max_value,
    MIN(value) AS min_value,
    MAX(as_of) AS latest_as_of,
    MAX(ingest_ts) AS latest_ingest_ts
FROM public.drought_index_timeseries
GROUP BY series_id, region_type, region_id, dataset, index, timescale, bucket_start;

SELECT add_continuous_aggregate_policy('public.drought_index_timeseries_wk',
    start_offset => INTERVAL '30 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

SELECT add_retention_policy('public.drought_index_timeseries_wk', INTERVAL '5 years', if_not_exists => TRUE);
