CREATE EXTENSION IF NOT EXISTS timescaledb;

-- CPI series observations loaded from Kafka via SeaTunnel
CREATE TABLE IF NOT EXISTS public.cpi_series_timeseries (
    tenant_id TEXT NOT NULL,
    series_id TEXT NOT NULL,
    period TEXT NOT NULL,
    area TEXT,
    frequency TEXT,
    seasonal_adjustment TEXT,
    value DOUBLE PRECISION,
    units TEXT DEFAULT 'Index',
    source TEXT DEFAULT 'FRED',
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_job_id TEXT,
    ingest_run_id TEXT,
    ingest_batch_id TEXT,
    metadata TEXT,
    PRIMARY KEY (tenant_id, series_id, period)
);

-- Hypertable on ingest timestamp to enable compression/retention policies
SELECT create_hypertable(
    'public.cpi_series_timeseries',
    'ingest_ts',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_cpi_series_period
    ON public.cpi_series_timeseries (tenant_id, series_id, period);
CREATE INDEX IF NOT EXISTS idx_cpi_period
    ON public.cpi_series_timeseries (tenant_id, period);

ALTER TABLE IF EXISTS public.cpi_series_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'tenant_id,series_id',
        timescaledb.compress_orderby = 'ingest_ts DESC'
    );

SELECT add_compression_policy('public.cpi_series_timeseries', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.cpi_series_timeseries', INTERVAL '3650 days', if_not_exists => TRUE);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'cpi_series_monthly_summary'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.cpi_series_monthly_summary
            WITH (timescaledb.continuous) AS
            SELECT
                tenant_id,
                series_id,
                time_bucket('1 month', to_date(period || '-01', 'YYYY-MM-DD')) AS bucket_start,
                AVG(value) AS value_avg,
                MIN(value) AS value_min,
                MAX(value) AS value_max,
                MAX(ingest_ts) AS latest_ingest_ts,
                COUNT(*) AS sample_count
            FROM public.cpi_series_timeseries
            GROUP BY tenant_id, series_id, bucket_start
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_cpi_series_monthly_summary
    ON public.cpi_series_monthly_summary (tenant_id, series_id, bucket_start DESC);

SELECT add_continuous_aggregate_policy(
    'public.cpi_series_monthly_summary',
    start_offset => INTERVAL '5 years',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_retention_policy('public.cpi_series_monthly_summary', INTERVAL '10 years', if_not_exists => TRUE);
