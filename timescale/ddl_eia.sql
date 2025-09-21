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

SELECT set_chunk_time_interval('public.eia_series_timeseries', INTERVAL '30 days');

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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'eia_series_daily'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.eia_series_daily
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', period_start) AS bucket_start,
                series_id,
                dataset,
                area,
                sector,
                avg(value) AS value_avg,
                min(value) AS value_min,
                max(value) AS value_max,
                last(value, period_start) AS value_last,
                count(*) AS sample_count
            FROM public.eia_series_timeseries
            GROUP BY 1, 2, 3, 4, 5
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_eia_series_daily_key
    ON public.eia_series_daily (series_id, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.eia_series_daily',
    start_offset => INTERVAL '400 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'eia_series_monthly'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.eia_series_monthly
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 month', period_start) AS bucket_start,
                series_id,
                dataset,
                area,
                sector,
                avg(value) AS value_avg,
                last(value, period_start) AS value_last,
                count(*) AS sample_count
            FROM public.eia_series_timeseries
            GROUP BY 1, 2, 3, 4, 5
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_eia_series_monthly_key
    ON public.eia_series_monthly (series_id, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.eia_series_monthly',
    start_offset => INTERVAL '1825 days',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
