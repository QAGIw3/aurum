CREATE EXTENSION IF NOT EXISTS timescaledb;

-- FRED series observations loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.fred_series_timeseries (
    tenant_id TEXT NOT NULL,
    series_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    frequency TEXT,
    seasonal_adjustment TEXT,
    value DOUBLE PRECISION,
    raw_value TEXT,
    units TEXT,
    title TEXT,
    notes TEXT,
    metadata TEXT,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_job_id TEXT,
    ingest_run_id TEXT,
    ingest_batch_id TEXT,
    PRIMARY KEY (series_id, obs_date)
);

-- Make the time column a hypertable (Timescale supports DATE)
SELECT create_hypertable('public.fred_series_timeseries', 'obs_date', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_fred_series_id_date ON public.fred_series_timeseries (tenant_id, series_id, obs_date DESC);

-- Enable compression and retention policies (older than 90 days compressed; keep 10 years)
ALTER TABLE IF EXISTS public.fred_series_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'obs_date DESC'
    );

SELECT add_compression_policy('public.fred_series_timeseries', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.fred_series_timeseries', INTERVAL '3650 days', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.fred_series_daily_summary
WITH (timescaledb.continuous) AS
SELECT
    tenant_id,
    series_id,
    obs_date,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS observation_count
FROM public.fred_series_timeseries
GROUP BY tenant_id, series_id, obs_date;

SELECT add_continuous_aggregate_policy(
    'public.fred_series_daily_summary',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);
