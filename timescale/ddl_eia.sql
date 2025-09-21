CREATE EXTENSION IF NOT EXISTS timescaledb;

-- EIA series observations loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.eia_series_timeseries (
    series_id TEXT NOT NULL,
    period TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ,
    frequency TEXT,
    value DOUBLE PRECISION,
    raw_value TEXT,
    unit TEXT,
    canonical_unit TEXT,
    canonical_currency TEXT,
    canonical_value DOUBLE PRECISION,
    conversion_factor DOUBLE PRECISION,
    area TEXT,
    sector TEXT,
    seasonal_adjustment TEXT,
    description TEXT,
    source TEXT,
    dataset TEXT,
    metadata JSONB,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (series_id, period)
);

SELECT
    create_hypertable(
        'public.eia_series_timeseries',
        'period_start',
        if_not_exists => TRUE,
        migrate_data => TRUE
    );

CREATE INDEX IF NOT EXISTS idx_eia_series_period_start
    ON public.eia_series_timeseries (series_id, period_start DESC);

CREATE INDEX IF NOT EXISTS idx_eia_series_dataset_period
    ON public.eia_series_timeseries (dataset, period_start DESC);

CREATE INDEX IF NOT EXISTS idx_eia_series_area_period
    ON public.eia_series_timeseries (area, period_start DESC);

ALTER TABLE IF EXISTS public.eia_series_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'period_start DESC'
    );

SELECT add_compression_policy('public.eia_series_timeseries', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.eia_series_timeseries', INTERVAL '3650 days', if_not_exists => TRUE);
