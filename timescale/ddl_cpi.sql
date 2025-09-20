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

-- Not a hypertable (period is a string label). Indexes for lookups
CREATE INDEX IF NOT EXISTS idx_cpi_series_period ON public.cpi_series_timeseries (series_id, period);
CREATE INDEX IF NOT EXISTS idx_cpi_period ON public.cpi_series_timeseries (period);

