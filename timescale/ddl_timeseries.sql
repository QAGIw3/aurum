CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.iso_lmp_timeseries (
    record_hash TEXT NOT NULL,
    tenant_id UUID,
    iso_code TEXT NOT NULL,
    market TEXT NOT NULL,
    delivery_date DATE NOT NULL,
    interval_start TIMESTAMPTZ NOT NULL,
    interval_end TIMESTAMPTZ,
    interval_minutes INTEGER,
    location_id TEXT NOT NULL,
    location_name TEXT,
    location_type TEXT,
    zone TEXT,
    hub TEXT,
    timezone TEXT,
    price_total DOUBLE PRECISION NOT NULL,
    price_energy DOUBLE PRECISION,
    price_congestion DOUBLE PRECISION,
    price_loss DOUBLE PRECISION,
    currency TEXT DEFAULT 'USD',
    uom TEXT DEFAULT 'MWh',
    settlement_point TEXT,
    source_run_id TEXT,
    ingest_job_id TEXT,
    ingest_run_id TEXT,
    ingest_batch_id TEXT,
    metadata JSONB,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , PRIMARY KEY (record_hash, interval_start)
);

SELECT
    create_hypertable(
        'public.iso_lmp_timeseries',
        'interval_start',
        if_not_exists => TRUE,
        migrate_data => TRUE
    );

SELECT set_chunk_time_interval('public.iso_lmp_timeseries', INTERVAL '3 days');

CREATE INDEX IF NOT EXISTS idx_iso_lmp_lookup
    ON public.iso_lmp_timeseries (tenant_id, iso_code, location_id, market, interval_start DESC);

-- Enable native compression and lifecycle policies for ISO LMP data
ALTER TABLE IF EXISTS public.iso_lmp_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'tenant_id,iso_code,location_id,market',
        timescaledb.compress_orderby = 'interval_start DESC'
    );

SELECT add_compression_policy('public.iso_lmp_timeseries', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.iso_lmp_timeseries', INTERVAL '180 days', if_not_exists => TRUE);

-- Five minute aggregate (rolling 7 days)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'lmp_agg_5m'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_lmp_agg_5m
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('5 minutes', interval_start) AS bucket_start,
                tenant_id,
                iso_code,
                location_id,
                market,
                zone,
                hub,
                timezone,
                currency,
                uom,
                avg(price_total) AS price_avg,
                min(price_total) AS price_min,
                max(price_total) AS price_max,
                stddev_pop(price_total) AS price_stddev,
                count(*) AS sample_count
            FROM public.iso_lmp_timeseries
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_lmp_agg_5m_key
    ON public.iso_lmp_agg_5m(tenant_id, iso_code, location_id, market, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_lmp_agg_5m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- Hourly aggregate (rolling 30 days)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'lmp_agg_1h'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_lmp_agg_1h
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 hour', interval_start) AS bucket_start,
                tenant_id,
                iso_code,
                location_id,
                market,
                zone,
                hub,
                timezone,
                currency,
                uom,
                avg(price_total) AS price_avg,
                min(price_total) AS price_min,
                max(price_total) AS price_max,
                stddev_pop(price_total) AS price_stddev,
                count(*) AS sample_count
            FROM public.iso_lmp_timeseries
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_lmp_agg_1h_key
    ON public.iso_lmp_agg_1h(tenant_id, iso_code, location_id, market, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_lmp_agg_1h',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Daily aggregate (rolling 365 days)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'lmp_agg_1d'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_lmp_agg_1d
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', interval_start) AS bucket_start,
                tenant_id,
                iso_code,
                location_id,
                market,
                currency,
                uom,
                avg(price_total) AS price_avg,
                min(price_total) AS price_min,
                max(price_total) AS price_max,
                stddev_pop(price_total) AS price_stddev,
                count(*) AS sample_count
            FROM public.iso_lmp_timeseries
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_lmp_agg_1d_key
    ON public.iso_lmp_agg_1d(tenant_id, iso_code, location_id, market, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_lmp_agg_1d',
    start_offset => INTERVAL '365 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS public.load_timeseries (
    tenant_id TEXT NOT NULL,
    iso_code TEXT NOT NULL,
    area TEXT NOT NULL DEFAULT 'SYSTEM',
    interval_start TIMESTAMPTZ NOT NULL,
    interval_end TIMESTAMPTZ,
    interval_minutes INTEGER,
    mw DOUBLE PRECISION NOT NULL,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_job_id TEXT,
    ingest_run_id TEXT,
    ingest_batch_id TEXT,
    metadata TEXT,
    PRIMARY KEY (tenant_id, iso_code, area, interval_start)
);

SELECT create_hypertable('public.load_timeseries', 'interval_start', if_not_exists => TRUE, migrate_data => TRUE);
SELECT set_chunk_time_interval('public.load_timeseries', INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS idx_iso_load_lookup
    ON public.load_timeseries (tenant_id, iso_code, area, interval_start DESC);

ALTER TABLE IF EXISTS public.load_timeseries
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'iso_code,area',
        timescaledb.compress_orderby = 'interval_start DESC'
    );

SELECT add_compression_policy('public.load_timeseries', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.load_timeseries', INTERVAL '1825 days', if_not_exists => TRUE);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'iso_load_agg_15m'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_load_agg_15m
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('15 minutes', interval_start) AS bucket_start,
                iso_code,
                area,
                avg(mw) AS mw_avg,
                min(mw) AS mw_min,
                max(mw) AS mw_max,
                stddev_pop(mw) AS mw_stddev,
                count(*) AS sample_count
            FROM public.load_timeseries
            GROUP BY 1, 2, 3
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_load_agg_15m_key
    ON public.iso_load_agg_15m (iso_code, area, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_load_agg_15m',
    start_offset => INTERVAL '14 days',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'iso_load_agg_1h'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_load_agg_1h
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 hour', interval_start) AS bucket_start,
                iso_code,
                area,
                avg(mw) AS mw_avg,
                min(mw) AS mw_min,
                max(mw) AS mw_max,
                stddev_pop(mw) AS mw_stddev,
                count(*) AS sample_count
            FROM public.load_timeseries
            GROUP BY 1, 2, 3
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_load_agg_1h_key
    ON public.iso_load_agg_1h (iso_code, area, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_load_agg_1h',
    start_offset => INTERVAL '90 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'iso_load_agg_1d'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.iso_load_agg_1d
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', interval_start) AS bucket_start,
                iso_code,
                area,
                avg(mw) AS mw_avg,
                min(mw) AS mw_min,
                max(mw) AS mw_max,
                stddev_pop(mw) AS mw_stddev,
                count(*) AS sample_count
            FROM public.load_timeseries
            GROUP BY 1, 2, 3
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_iso_load_agg_1d_key
    ON public.iso_load_agg_1d (iso_code, area, bucket_start DESC);
SELECT add_continuous_aggregate_policy(
    'public.iso_load_agg_1d',
    start_offset => INTERVAL '730 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS public.ops_metrics (
    id BIGSERIAL,
    metric TEXT NOT NULL,
    labels JSONB DEFAULT '{}'::JSONB,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , PRIMARY KEY (id, ts)
);

SELECT create_hypertable('public.ops_metrics', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
SELECT set_chunk_time_interval('public.ops_metrics', INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS idx_ops_metrics_metric_ts ON public.ops_metrics(metric, ts DESC);

ALTER TABLE IF EXISTS public.ops_metrics
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'metric',
        timescaledb.compress_orderby = 'ts DESC'
    );

SELECT add_compression_policy('public.ops_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('public.ops_metrics', INTERVAL '365 days', if_not_exists => TRUE);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'ops_metrics_agg_5m'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.ops_metrics_agg_5m
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('5 minutes', ts) AS bucket_start,
                metric,
                labels,
                avg(value) AS value_avg,
                min(value) AS value_min,
                max(value) AS value_max,
                stddev_pop(value) AS value_stddev,
                count(*) AS sample_count
            FROM public.ops_metrics
            GROUP BY 1, 2, 3
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ops_metrics_agg_5m_key
    ON public.ops_metrics_agg_5m (metric, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_ops_metrics_agg_5m_labels
    ON public.ops_metrics_agg_5m USING GIN (labels);

SELECT add_continuous_aggregate_policy(
    'public.ops_metrics_agg_5m',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'ops_metrics_agg_1h'
    ) THEN
        EXECUTE $DDL$
            CREATE MATERIALIZED VIEW public.ops_metrics_agg_1h
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 hour', ts) AS bucket_start,
                metric,
                labels,
                avg(value) AS value_avg,
                min(value) AS value_min,
                max(value) AS value_max,
                stddev_pop(value) AS value_stddev,
                count(*) AS sample_count
            FROM public.ops_metrics
            GROUP BY 1, 2, 3
            WITH NO DATA;
        $DDL$;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ops_metrics_agg_1h_key
    ON public.ops_metrics_agg_1h (metric, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_ops_metrics_agg_1h_labels
    ON public.ops_metrics_agg_1h USING GIN (labels);

SELECT add_continuous_aggregate_policy(
    'public.ops_metrics_agg_1h',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
