CREATE EXTENSION IF NOT EXISTS timescaledb;

-- FRED series observations loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.fred_series_timeseries (
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
    PRIMARY KEY (series_id, obs_date)
);

-- Make the time column a hypertable (Timescale supports DATE)
SELECT create_hypertable('public.fred_series_timeseries', 'obs_date', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_fred_series_id_date ON public.fred_series_timeseries (series_id, obs_date DESC);

-- Enable compression and retention policies (older than 90 days compressed; keep 10 years)
ALTER TABLE IF EXISTS public.fred_series_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'obs_date DESC'
    );

SELECT add_compression_policy('public.fred_series_timeseries', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.fred_series_timeseries', INTERVAL '3650 days', if_not_exists => TRUE);

