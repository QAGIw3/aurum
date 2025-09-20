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
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name, type)
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

CREATE TABLE IF NOT EXISTS ingest_source (
    name CITEXT PRIMARY KEY,
    description TEXT,
    schedule TEXT,
    target TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_watermark (
    source_name CITEXT NOT NULL REFERENCES ingest_source(name) ON DELETE CASCADE,
    watermark_key TEXT NOT NULL DEFAULT 'default',
    watermark TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, watermark_key)
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

CREATE OR REPLACE FUNCTION public.update_ingest_watermark(
    p_source_name CITEXT,
    p_watermark_key TEXT,
    p_new_watermark TIMESTAMPTZ
) RETURNS VOID AS $$
BEGIN
    INSERT INTO ingest_watermark(source_name, watermark_key, watermark, updated_at)
    VALUES (p_source_name, p_watermark_key, p_new_watermark, NOW())
    ON CONFLICT (source_name, watermark_key) DO UPDATE
        SET watermark = EXCLUDED.watermark,
            updated_at = NOW();
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
