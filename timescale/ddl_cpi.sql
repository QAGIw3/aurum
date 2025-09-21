CREATE EXTENSION IF NOT EXISTS timescaledb;

-- CPI series observations loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.cpi_series_timeseries (
    series_id TEXT NOT NULL,
    period TEXT NOT NULL,
    area TEXT,
    frequency TEXT,
    seasonal_adjustment TEXT,
    value DOUBLE PRECISION,
    units TEXT DEFAULT 'Index',
    source TEXT DEFAULT 'FRED',
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata TEXT,
    PRIMARY KEY (series_id, period)
);

-- Hypertable on ingest timestamp to enable compression/retention policies
SELECT create_hypertable(
    'public.cpi_series_timeseries',
    'ingest_ts',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_cpi_series_period
    ON public.cpi_series_timeseries (series_id, period);
CREATE INDEX IF NOT EXISTS idx_cpi_period
    ON public.cpi_series_timeseries (period);

ALTER TABLE IF EXISTS public.cpi_series_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'ingest_ts DESC'
    );

SELECT add_compression_policy('public.cpi_series_timeseries', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.cpi_series_timeseries', INTERVAL '3650 days', if_not_exists => TRUE);

