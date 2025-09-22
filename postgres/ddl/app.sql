CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS tenant (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name CITEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS instrument (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
    asset_class TEXT NOT NULL,
    iso TEXT,
    region TEXT,
    location TEXT,
    market TEXT,
    product TEXT,
    block TEXT,
    spark_location TEXT,
    units_raw TEXT,
    curve_key TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, curve_key)
);

CREATE TABLE IF NOT EXISTS curve_def (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
    instrument_id UUID NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    methodology TEXT NOT NULL,
    horizon_months INTEGER NOT NULL,
    granularity TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenario (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    description TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenario_driver (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name, type)
);

CREATE TABLE IF NOT EXISTS scenario_assumption_value (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
    driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE RESTRICT,
    payload JSONB NOT NULL,
    version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scenario_id, driver_id, version)
);

CREATE TABLE IF NOT EXISTS assumption (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL,
    curve_def_id UUID REFERENCES curve_def(id) ON DELETE CASCADE,
    code_version TEXT NOT NULL,
    seed BIGINT,
    state TEXT NOT NULL,
    version_hash TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);

CREATE TABLE IF NOT EXISTS ppa_contract (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
    instrument_id UUID REFERENCES instrument(id) ON DELETE SET NULL,
    terms JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_ingest_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    asof DATE NOT NULL,
    path TEXT NOT NULL,
    sheet TEXT,
    status TEXT NOT NULL,
    details TEXT,
    version_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instrument_curve_key ON instrument(curve_key);
CREATE INDEX IF NOT EXISTS idx_curve_def_instrument ON curve_def(instrument_id);
CREATE INDEX IF NOT EXISTS idx_scenario_tenant ON scenario(tenant_id);
CREATE INDEX IF NOT EXISTS idx_model_run_state ON model_run(state);
CREATE INDEX IF NOT EXISTS idx_file_ingest_log_asof ON file_ingest_log(asof);

ALTER TABLE instrument ENABLE ROW LEVEL SECURITY;
ALTER TABLE curve_def ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE ppa_contract ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_instrument ON instrument
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_curve_def ON curve_def
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_scenario ON scenario
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_model_run ON model_run
    USING (scenario_id IN (
        SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_ppa ON ppa_contract
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

-- RLS policies for scenario-related tables
ALTER TABLE scenario_driver ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario_assumption_value ENABLE ROW LEVEL SECURITY;
ALTER TABLE assumption ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE file_ingest_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingest_source ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingest_watermark ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingest_slice ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_scenario_driver ON scenario_driver
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_scenario_assumption_value ON scenario_assumption_value
    USING (scenario_id IN (
        SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_assumption ON assumption
    USING (scenario_id IN (
        SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_model_run ON model_run
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_file_ingest_log ON file_ingest_log
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_ingest_source ON ingest_source
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_ingest_watermark ON ingest_watermark
    USING (source_name IN (
        SELECT name FROM ingest_source WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_ingest_slice ON ingest_slice
    USING (source_name IN (
        SELECT name FROM ingest_source WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

-- Additional scenario-related tables from migrations
CREATE TABLE IF NOT EXISTS scenario_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'queued',
    priority TEXT NOT NULL DEFAULT 'normal',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_seconds REAL,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    progress_percent REAL,
    parameters JSONB DEFAULT '{}'::JSONB,
    environment JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelled_at TIMESTAMPTZ,
    UNIQUE (scenario_id, id)
);

CREATE TABLE IF NOT EXISTS scenario_output (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE RESTRICT,
    timestamp TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    tags JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenario_run_output_pointer (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
    output_type TEXT NOT NULL,  -- 'file', 'database', 'api', etc.
    output_location TEXT NOT NULL,  -- S3 path, table name, API endpoint, etc.
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scenario_run_id, output_type)
);

CREATE TABLE IF NOT EXISTS scenario_run_event (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,  -- 'queued', 'started', 'completed', 'failed', 'cancelled', 'retry'
    event_data JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenario_feature_flag (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    feature_name TEXT NOT NULL,  -- 'scenario_outputs', 'bulk_operations', etc.
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    configuration JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, feature_name)
);

-- Enable RLS on additional tables
ALTER TABLE scenario_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario_output ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario_run_output_pointer ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario_run_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE scenario_feature_flag ENABLE ROW LEVEL SECURITY;

-- RLS policies for additional scenario tables
CREATE POLICY tenant_isolation_scenario_run ON scenario_run
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_scenario_output ON scenario_output
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

CREATE POLICY tenant_isolation_scenario_run_output_pointer ON scenario_run_output_pointer
    USING (scenario_run_id IN (
        SELECT id FROM scenario_run WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_scenario_run_event ON scenario_run_event
    USING (scenario_run_id IN (
        SELECT id FROM scenario_run WHERE tenant_id = current_setting('app.current_tenant')::UUID
    ));

CREATE POLICY tenant_isolation_scenario_feature_flag ON scenario_feature_flag
    USING (tenant_id = current_setting('app.current_tenant')::UUID);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_scenario_run_scenario_id ON scenario_run(scenario_id);
CREATE INDEX IF NOT EXISTS idx_scenario_run_tenant_id ON scenario_run(tenant_id);
CREATE INDEX IF NOT EXISTS idx_scenario_run_status ON scenario_run(status);
CREATE INDEX IF NOT EXISTS idx_scenario_run_queued_at ON scenario_run(queued_at);
CREATE INDEX IF NOT EXISTS idx_scenario_run_started_at ON scenario_run(started_at);
CREATE INDEX IF NOT EXISTS idx_scenario_output_scenario_run ON scenario_output(scenario_run_id);
CREATE INDEX IF NOT EXISTS idx_scenario_output_metric_timestamp ON scenario_output(metric_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_scenario_run_event_created_at ON scenario_run_event(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scenario_feature_flag_tenant ON scenario_feature_flag(tenant_id);

CREATE TABLE IF NOT EXISTS ingest_watermark (
    source_name CITEXT NOT NULL,
    watermark_key TEXT NOT NULL DEFAULT 'default',
    watermark TIMESTAMPTZ,
    watermark_policy TEXT NOT NULL DEFAULT 'exact',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, watermark_key),
    FOREIGN KEY (source_name) REFERENCES ingest_source(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ingest_source_active ON ingest_source(active);
CREATE INDEX IF NOT EXISTS idx_ingest_watermark_updated ON ingest_watermark(updated_at DESC);

CREATE OR REPLACE FUNCTION public.register_ingest_source(
    p_name CITEXT,
    p_description TEXT DEFAULT NULL,
    p_schedule TEXT DEFAULT NULL,
    p_target TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO ingest_source(name, description, schedule, target)
    VALUES (p_name, p_description, p_schedule, p_target)
    ON CONFLICT (name) DO UPDATE
        SET description = COALESCE(EXCLUDED.description, ingest_source.description),
            schedule = COALESCE(EXCLUDED.schedule, ingest_source.schedule),
            target = COALESCE(EXCLUDED.target, ingest_source.target),
            updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Window claims for incremental processing
CREATE TABLE IF NOT EXISTS ingest_watermark_claim (
    source_name CITEXT NOT NULL,
    watermark_key TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'CLAIMED',
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, watermark_key, window_start)
);

CREATE INDEX IF NOT EXISTS idx_ingest_watermark_claim_status ON ingest_watermark_claim(status);

CREATE OR REPLACE FUNCTION public.claim_ingest_window(
    p_source_name CITEXT,
    p_watermark_key TEXT,
    p_window_seconds INT,
    p_max_ahead_seconds INT,
    p_job_id TEXT
) RETURNS TABLE(window_start TIMESTAMPTZ, window_end TIMESTAMPTZ) AS $$
DECLARE
    last_wm TIMESTAMPTZ;
    latest_end TIMESTAMPTZ;
    proposed_start TIMESTAMPTZ;
    proposed_end TIMESTAMPTZ;
    safety_lag INTERVAL := INTERVAL '60 seconds';
    lock_key BIGINT := hashtextextended(p_source_name || ':' || p_watermark_key, 0);
BEGIN
    PERFORM pg_advisory_xact_lock(lock_key);

    SELECT public.get_ingest_watermark(p_source_name, p_watermark_key) INTO last_wm;
    IF last_wm IS NULL THEN
        last_wm := (NOW() AT TIME ZONE 'UTC') - make_interval(secs => p_max_ahead_seconds);
    END IF;

    SELECT COALESCE(MAX(window_end), last_wm)
      INTO latest_end
      FROM ingest_watermark_claim
     WHERE source_name = p_source_name
       AND watermark_key = p_watermark_key
       AND status IN ('CLAIMED','COMMITTED');

    proposed_start := GREATEST(last_wm, latest_end);
    proposed_end := proposed_start + make_interval(secs => p_window_seconds);
    IF proposed_end > (NOW() AT TIME ZONE 'UTC') - safety_lag THEN
        proposed_end := (NOW() AT TIME ZONE 'UTC') - safety_lag;
    END IF;
    IF proposed_end <= proposed_start THEN
        RETURN; -- no work
    END IF;

    INSERT INTO ingest_watermark_claim(source_name, watermark_key, window_start, window_end, job_id, status)
    VALUES (p_source_name, p_watermark_key, proposed_start, proposed_end, p_job_id, 'CLAIMED')
    ON CONFLICT DO NOTHING;

    RETURN QUERY
      SELECT window_start, window_end
        FROM ingest_watermark_claim
       WHERE source_name = p_source_name
         AND watermark_key = p_watermark_key
         AND window_start = proposed_start
         AND job_id = p_job_id
         AND status = 'CLAIMED';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.commit_ingest_window(
    p_source_name CITEXT,
    p_watermark_key TEXT,
    p_window_start TIMESTAMPTZ,
    p_window_end TIMESTAMPTZ,
    p_job_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    updated INT;
BEGIN
    UPDATE ingest_watermark_claim
       SET status = 'COMMITTED', updated_at = NOW()
     WHERE source_name = p_source_name
       AND watermark_key = p_watermark_key
       AND window_start = p_window_start
       AND job_id = p_job_id
       AND status = 'CLAIMED';
    GET DIAGNOSTICS updated = ROW_COUNT;

    IF updated > 0 THEN
        PERFORM public.update_ingest_watermark(p_source_name, p_watermark_key, p_window_end, 'exact');
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.gap_scan_ingest_windows(
    p_source_name CITEXT,
    p_watermark_key TEXT,
    p_backfill_hours INT,
    p_window_seconds INT,
    p_limit INT DEFAULT 100
) RETURNS TABLE(window_start TIMESTAMPTZ, window_end TIMESTAMPTZ) AS $$
DECLARE
    start_ts TIMESTAMPTZ := (NOW() AT TIME ZONE 'UTC') - make_interval(hours => p_backfill_hours);
    end_ts   TIMESTAMPTZ := NOW() AT TIME ZONE 'UTC';
BEGIN
    RETURN QUERY
    WITH series AS (
        SELECT gs AS window_start,
               gs + make_interval(secs => p_window_seconds) AS window_end
          FROM generate_series(start_ts, end_ts - make_interval(secs => p_window_seconds), make_interval(secs => p_window_seconds)) gs
    )
    SELECT s.window_start, s.window_end
      FROM series s
      LEFT JOIN ingest_watermark_claim c
        ON c.source_name = p_source_name
       AND c.watermark_key = p_watermark_key
       AND c.window_start = s.window_start
       AND c.status = 'COMMITTED'
     WHERE c.window_start IS NULL
     ORDER BY s.window_start
     LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.update_ingest_watermark(
    p_source_name CITEXT,
    p_watermark_key TEXT,
    p_new_watermark TIMESTAMPTZ,
    p_policy TEXT DEFAULT 'exact'
) RETURNS VOID AS $$
DECLARE
    current_watermark TIMESTAMPTZ;
    final_watermark TIMESTAMPTZ;
BEGIN
    -- Get current watermark
    SELECT watermark INTO current_watermark
    FROM ingest_watermark
    WHERE source_name = p_source_name AND watermark_key = p_watermark_key;

    -- Apply policy to determine final watermark
    CASE p_policy
        WHEN 'day' THEN
            -- Round to start of day
            final_watermark := DATE_TRUNC('day', p_new_watermark);
        WHEN 'hour' THEN
            -- Round to start of hour
            final_watermark := DATE_TRUNC('hour', p_new_watermark);
        WHEN 'month' THEN
            -- Round to start of month
            final_watermark := DATE_TRUNC('month', p_new_watermark);
        WHEN 'week' THEN
            -- Round to start of week (Monday)
            final_watermark := DATE_TRUNC('week', p_new_watermark);
        WHEN 'exact' THEN
            -- Use exact timestamp
            final_watermark := p_new_watermark;
        ELSE
            -- Default to exact for unknown policies
            final_watermark := p_new_watermark;
    END CASE;

    -- Only update if the final watermark is newer than current
    IF current_watermark IS NULL OR final_watermark > current_watermark THEN
        INSERT INTO ingest_watermark(source_name, watermark_key, watermark, watermark_policy, updated_at)
        VALUES (p_source_name, p_watermark_key, final_watermark, p_policy, NOW())
        ON CONFLICT (source_name, watermark_key) DO UPDATE
            SET watermark = EXCLUDED.watermark,
                watermark_policy = EXCLUDED.watermark_policy,
                updated_at = NOW()
            WHERE EXCLUDED.watermark > ingest_watermark.watermark;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_ingest_watermark(
    p_source_name CITEXT,
    p_watermark_key TEXT DEFAULT 'default'
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    result TIMESTAMPTZ;
BEGIN
    SELECT watermark INTO result
    FROM ingest_watermark
    WHERE source_name = p_source_name
      AND watermark_key = p_watermark_key;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Slice ledger for tracking individual work slices that can be resumed
CREATE TABLE IF NOT EXISTS ingest_slice (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name CITEXT NOT NULL,
    slice_key TEXT NOT NULL,  -- e.g., "2024-12-31_14:00" or "dataset_ELEC.PRICE"
    slice_type TEXT NOT NULL,  -- time_window, dataset, failed_records, incremental
    slice_data JSONB NOT NULL,  -- Additional data for the slice (time range, dataset names, etc.)
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, running, completed, failed, cancelled
    priority INTEGER NOT NULL DEFAULT 100,  -- Lower numbers = higher priority
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_retry_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    error_message TEXT,
    progress_percent DECIMAL(5,2) DEFAULT 0.00,
    records_processed INTEGER DEFAULT 0,
    records_expected INTEGER,
    processing_time_seconds DECIMAL(10,2),
    worker_id TEXT,  -- ID of the worker processing this slice
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, slice_key, slice_type),
    FOREIGN KEY (source_name) REFERENCES ingest_source(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ingest_slice_source_status ON ingest_slice(source_name, status);
CREATE INDEX IF NOT EXISTS idx_ingest_slice_next_retry ON ingest_slice(next_retry_at) WHERE status = 'failed' AND retry_count < max_retries;
CREATE INDEX IF NOT EXISTS idx_ingest_slice_priority_status ON ingest_slice(priority, status) WHERE status IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS idx_ingest_slice_source_type_key ON ingest_slice(source_name, slice_type, slice_key);
CREATE INDEX IF NOT EXISTS idx_ingest_slice_updated_at ON ingest_slice(updated_at DESC);

-- Function to register or update a slice
CREATE OR REPLACE FUNCTION public.upsert_ingest_slice(
    p_source_name CITEXT,
    p_slice_key TEXT,
    p_slice_type TEXT,
    p_slice_data JSONB,
    p_status TEXT DEFAULT 'pending',
    p_priority INTEGER DEFAULT 100,
    p_max_retries INTEGER DEFAULT 3,
    p_records_expected INTEGER DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::JSONB
) RETURNS UUID AS $$
DECLARE
    slice_id UUID;
    now_time TIMESTAMPTZ := NOW();
BEGIN
    -- Try to update existing slice
    UPDATE ingest_slice
    SET
        slice_data = p_slice_data,
        status = p_status,
        priority = p_priority,
        max_retries = p_max_retries,
        records_expected = COALESCE(p_records_expected, records_expected),
        metadata = p_metadata,
        updated_at = now_time
    WHERE source_name = p_source_name
      AND slice_key = p_slice_key
      AND slice_type = p_slice_type
    RETURNING id INTO slice_id;

    -- If no existing slice, insert new one
    IF slice_id IS NULL THEN
        INSERT INTO ingest_slice(
            source_name, slice_key, slice_type, slice_data, status,
            priority, max_retries, records_expected, metadata, updated_at
        )
        VALUES (
            p_source_name, p_slice_key, p_slice_type, p_slice_data, p_status,
            p_priority, p_max_retries, p_records_expected, p_metadata, now_time
        )
        RETURNING id INTO slice_id;
    END IF;

    RETURN slice_id;
END;
$$ LANGUAGE plpgsql;

-- Function to start processing a slice
CREATE OR REPLACE FUNCTION public.start_ingest_slice(
    p_source_name CITEXT,
    p_slice_key TEXT,
    p_slice_type TEXT,
    p_worker_id TEXT DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
    slice_id UUID;
    now_time TIMESTAMPTZ := NOW();
BEGIN
    -- Update slice status to running
    UPDATE ingest_slice
    SET
        status = 'running',
        started_at = now_time,
        worker_id = COALESCE(p_worker_id, worker_id),
        updated_at = now_time
    WHERE source_name = p_source_name
      AND slice_key = p_slice_key
      AND slice_type = p_slice_type
      AND status IN ('pending', 'failed')
    RETURNING id INTO slice_id;

    RETURN slice_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to complete a slice
CREATE OR REPLACE FUNCTION public.complete_ingest_slice(
    p_source_name CITEXT,
    p_slice_key TEXT,
    p_slice_type TEXT,
    p_records_processed INTEGER DEFAULT 0,
    p_processing_time_seconds DECIMAL DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::JSONB
) RETURNS BOOLEAN AS $$
DECLARE
    slice_id UUID;
    now_time TIMESTAMPTZ := NOW();
BEGIN
    UPDATE ingest_slice
    SET
        status = 'completed',
        completed_at = now_time,
        records_processed = COALESCE(p_records_processed, records_processed),
        processing_time_seconds = COALESCE(p_processing_time_seconds, processing_time_seconds),
        metadata = metadata || p_metadata,
        updated_at = now_time
    WHERE source_name = p_source_name
      AND slice_key = p_slice_key
      AND slice_type = p_slice_type
      AND status = 'running'
    RETURNING id INTO slice_id;

    RETURN slice_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to fail a slice
CREATE OR REPLACE FUNCTION public.fail_ingest_slice(
    p_source_name CITEXT,
    p_slice_key TEXT,
    p_slice_type TEXT,
    p_error_message TEXT,
    p_metadata JSONB DEFAULT '{}'::JSONB
) RETURNS BOOLEAN AS $$
DECLARE
    slice_id UUID;
    now_time TIMESTAMPTZ := NOW();
    slice_record RECORD;
BEGIN
    -- Get current slice info
    SELECT retry_count, max_retries, next_retry_at
    INTO slice_record
    FROM ingest_slice
    WHERE source_name = p_source_name
      AND slice_key = p_slice_key
      AND slice_type = p_slice_type;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    -- Calculate next retry time (exponential backoff: 1min, 2min, 4min, etc.)
    DECLARE
        next_retry_delay_minutes INTEGER := GREATEST(1, LEAST(60, POW(2, slice_record.retry_count))); -- Max 60 minutes
        next_retry_time TIMESTAMPTZ := now_time + (next_retry_delay_minutes || ' minutes')::INTERVAL;
    BEGIN
        UPDATE ingest_slice
        SET
            status = CASE
                WHEN slice_record.retry_count >= slice_record.max_retries THEN 'failed'
                ELSE 'failed'
            END,
            failed_at = now_time,
            error_message = p_error_message,
            retry_count = slice_record.retry_count + 1,
            next_retry_at = CASE
                WHEN slice_record.retry_count < slice_record.max_retries THEN next_retry_time
                ELSE NULL
            END,
            metadata = metadata || p_metadata,
            updated_at = now_time
        WHERE source_name = p_source_name
          AND slice_key = p_slice_key
          AND slice_type = p_slice_type
        RETURNING id INTO slice_id;

        RETURN slice_id IS NOT NULL;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to update slice progress
CREATE OR REPLACE FUNCTION public.update_ingest_slice_progress(
    p_source_name CITEXT,
    p_slice_key TEXT,
    p_slice_type TEXT,
    p_progress_percent DECIMAL,
    p_records_processed INTEGER DEFAULT 0,
    p_metadata JSONB DEFAULT '{}'::JSONB
) RETURNS BOOLEAN AS $$
DECLARE
    slice_id UUID;
    now_time TIMESTAMPTZ := NOW();
BEGIN
    UPDATE ingest_slice
    SET
        progress_percent = p_progress_percent,
        records_processed = COALESCE(p_records_processed, records_processed),
        metadata = metadata || p_metadata,
        updated_at = now_time
    WHERE source_name = p_source_name
      AND slice_key = p_slice_key
      AND slice_type = p_slice_type
      AND status = 'running'
    RETURNING id INTO slice_id;

    RETURN slice_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to get slices ready for retry
CREATE OR REPLACE FUNCTION public.get_slices_for_retry(
    p_max_slices INTEGER DEFAULT 10,
    p_source_name CITEXT DEFAULT NULL
) RETURNS TABLE(
    id UUID,
    source_name CITEXT,
    slice_key TEXT,
    slice_type TEXT,
    slice_data JSONB,
    priority INTEGER,
    retry_count INTEGER,
    max_retries INTEGER,
    next_retry_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.id,
        s.source_name,
        s.slice_key,
        s.slice_type,
        s.slice_data,
        s.priority,
        s.retry_count,
        s.max_retries,
        s.next_retry_at
    FROM ingest_slice s
    WHERE s.status = 'failed'
      AND s.retry_count < s.max_retries
      AND (s.next_retry_at IS NULL OR s.next_retry_at <= NOW())
      AND (p_source_name IS NULL OR s.source_name = p_source_name)
    ORDER BY s.priority ASC, s.next_retry_at ASC
    LIMIT p_max_slices;
END;
$$ LANGUAGE plpgsql;

-- Function to get pending slices for processing
CREATE OR REPLACE FUNCTION public.get_pending_slices(
    p_max_slices INTEGER DEFAULT 10,
    p_source_name CITEXT DEFAULT NULL,
    p_slice_type TEXT DEFAULT NULL
) RETURNS TABLE(
    id UUID,
    source_name CITEXT,
    slice_key TEXT,
    slice_type TEXT,
    slice_data JSONB,
    priority INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.id,
        s.source_name,
        s.slice_key,
        s.slice_type,
        s.slice_data,
        s.priority
    FROM ingest_slice s
    WHERE s.status = 'pending'
      AND (p_source_name IS NULL OR s.source_name = p_source_name)
      AND (p_slice_type IS NULL OR s.slice_type = p_slice_type)
    ORDER BY s.priority ASC, s.created_at ASC
    LIMIT p_max_slices;
END;
$$ LANGUAGE plpgsql;

-- Function to get operation statistics
CREATE OR REPLACE FUNCTION public.get_slice_statistics(
    p_source_name CITEXT DEFAULT NULL,
    p_hours_back INTEGER DEFAULT 24
) RETURNS JSONB AS $$
DECLARE
    result JSONB;
    cutoff_time TIMESTAMPTZ := NOW() - (p_hours_back || ' hours')::INTERVAL;
BEGIN
    WITH stats AS (
        SELECT
            COUNT(*) as total_slices,
            COUNT(*) FILTER (WHERE status = 'completed') as completed_slices,
            COUNT(*) FILTER (WHERE status = 'failed') as failed_slices,
            COUNT(*) FILTER (WHERE status = 'running') as running_slices,
            COUNT(*) FILTER (WHERE status = 'pending') as pending_slices,
            AVG(processing_time_seconds) FILTER (WHERE processing_time_seconds IS NOT NULL) as avg_processing_time,
            SUM(records_processed) FILTER (WHERE records_processed IS NOT NULL) as total_records_processed,
            AVG(progress_percent) FILTER (WHERE status = 'running') as avg_progress_percent
        FROM ingest_slice
        WHERE (p_source_name IS NULL OR source_name = p_source_name)
          AND created_at >= cutoff_time
    )
    SELECT jsonb_build_object(
        'time_range_hours', p_hours_back,
        'cutoff_time', cutoff_time,
        'source_name', p_source_name,
        'total_slices', total_slices,
        'completed_slices', completed_slices,
        'failed_slices', failed_slices,
        'running_slices', running_slices,
        'pending_slices', pending_slices,
        'success_rate', CASE WHEN total_slices > 0 THEN completed_slices::DECIMAL / total_slices ELSE 0 END,
        'average_processing_time_seconds', avg_processing_time,
        'total_records_processed', total_records_processed,
        'average_progress_percent', avg_progress_percent
    ) INTO result
    FROM stats;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
