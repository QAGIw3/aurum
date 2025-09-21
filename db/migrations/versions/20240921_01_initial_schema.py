"""Initial schema for scenario store and PPA contracts."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20240921_01"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";',
    'CREATE EXTENSION IF NOT EXISTS citext;',
    """
    CREATE TABLE IF NOT EXISTS tenant (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name CITEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
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
    """,
    """
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
    """,
    """
    CREATE TABLE IF NOT EXISTS scenario (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
        name TEXT NOT NULL,
        description TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS scenario_driver (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        description TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (name, type)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS scenario_assumption_value (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
        driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE RESTRICT,
        payload JSONB NOT NULL,
        version TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (scenario_id, driver_id, version)
    );
    """,
    """
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
    """,
    """
    CREATE TABLE IF NOT EXISTS ppa_contract (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID REFERENCES tenant(id) ON DELETE RESTRICT,
        instrument_id UUID REFERENCES instrument(id) ON DELETE SET NULL,
        terms JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_instrument_curve_key ON instrument(curve_key);",
    "CREATE INDEX IF NOT EXISTS idx_curve_def_instrument ON curve_def(instrument_id);",
    "CREATE INDEX IF NOT EXISTS idx_scenario_tenant ON scenario(tenant_id);",
    "CREATE INDEX IF NOT EXISTS idx_model_run_state ON model_run(state);",
    "CREATE INDEX IF NOT EXISTS idx_ppa_contract_tenant ON ppa_contract(tenant_id);",
    "ALTER TABLE instrument ENABLE ROW LEVEL SECURITY;",
    "ALTER TABLE curve_def ENABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario ENABLE ROW LEVEL SECURITY;",
    "ALTER TABLE model_run ENABLE ROW LEVEL SECURITY;",
    "ALTER TABLE ppa_contract ENABLE ROW LEVEL SECURITY;",
    ""
    "CREATE POLICY tenant_isolation_instrument ON instrument\n"
    "    USING (tenant_id = current_setting('app.current_tenant')::UUID);",
    ""
    "CREATE POLICY tenant_isolation_curve_def ON curve_def\n"
    "    USING (tenant_id = current_setting('app.current_tenant')::UUID);",
    ""
    "CREATE POLICY tenant_isolation_scenario ON scenario\n"
    "    USING (tenant_id = current_setting('app.current_tenant')::UUID);",
    ""
    "CREATE POLICY tenant_isolation_model_run ON model_run\n"
    "    USING (scenario_id IN (\n"
    "        SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID\n"
    "    ));",
    ""
    "CREATE POLICY tenant_isolation_ppa ON ppa_contract\n"
    "    USING (tenant_id = current_setting('app.current_tenant')::UUID);",
    ""
    "CREATE TABLE IF NOT EXISTS file_ingest_log ("
    "    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),"
    "    source_name CITEXT NOT NULL,"
    "    file_path TEXT NOT NULL,"
    "    file_size BIGINT,"
    "    record_count INTEGER,"
    "    status TEXT NOT NULL DEFAULT 'pending',"
    "    error_message TEXT,"
    "    started_at TIMESTAMPTZ DEFAULT NOW(),"
    "    completed_at TIMESTAMPTZ,"
    "    UNIQUE (source_name, file_path)"
    ");",
    ""
    "CREATE TABLE IF NOT EXISTS ingest_source ("
    "    name CITEXT PRIMARY KEY,"
    "    description TEXT,"
    "    schedule TEXT,"
    "    target TEXT,"
    "    active BOOLEAN NOT NULL DEFAULT TRUE,"
    "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    "    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
    ");",
    ""
    "CREATE TABLE IF NOT EXISTS ingest_watermark ("
    "    source_name CITEXT NOT NULL REFERENCES ingest_source(name) ON DELETE CASCADE,"
    "    watermark_key TEXT NOT NULL DEFAULT 'default',"
    "    watermark TIMESTAMPTZ,"
    "    watermark_policy TEXT NOT NULL DEFAULT 'exact',"
    "    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    "    PRIMARY KEY (source_name, watermark_key)"
    ");",
    ""
    "CREATE INDEX IF NOT EXISTS idx_ingest_source_active ON ingest_source(active);",
    "CREATE INDEX IF NOT EXISTS idx_ingest_watermark_updated ON ingest_watermark(updated_at DESC);",
    ""
    "CREATE OR REPLACE FUNCTION public.register_ingest_source("
    "    p_name CITEXT,"
    "    p_description TEXT DEFAULT NULL,"
    "    p_schedule TEXT DEFAULT NULL,"
    "    p_target TEXT DEFAULT NULL"
    ") RETURNS VOID AS $$"
    "BEGIN"
    "    INSERT INTO ingest_source(name, description, schedule, target)"
    "    VALUES (p_name, p_description, p_schedule, p_target)"
    "    ON CONFLICT (name) DO UPDATE"
    "        SET description = COALESCE(EXCLUDED.description, ingest_source.description),"
    "            schedule = COALESCE(EXCLUDED.schedule, ingest_source.schedule),"
    "            target = COALESCE(EXCLUDED.target, ingest_source.target),"
    "            updated_at = NOW();"
    "END;"
    "$$ LANGUAGE plpgsql;",
    ""
    "CREATE OR REPLACE FUNCTION public.update_ingest_watermark("
    "    p_source_name CITEXT,"
    "    p_watermark_key TEXT,"
    "    p_new_watermark TIMESTAMPTZ,"
    "    p_policy TEXT DEFAULT 'exact'"
    ") RETURNS VOID AS $$"
    "DECLARE"
    "    current_watermark TIMESTAMPTZ;"
    "    final_watermark TIMESTAMPTZ;"
    "BEGIN"
    "    -- Get current watermark"
    "    SELECT watermark INTO current_watermark"
    "    FROM ingest_watermark"
    "    WHERE source_name = p_source_name AND watermark_key = p_watermark_key;"
    ""
    "    -- Apply policy to determine final watermark"
    "    CASE p_policy"
    "        WHEN 'day' THEN"
    "            -- Round to start of day"
    "            final_watermark := DATE_TRUNC('day', p_new_watermark);"
    "        WHEN 'hour' THEN"
    "            -- Round to start of hour"
    "            final_watermark := DATE_TRUNC('hour', p_new_watermark);"
    "        WHEN 'month' THEN"
    "            -- Round to start of month"
    "            final_watermark := DATE_TRUNC('month', p_new_watermark);"
    "        WHEN 'week' THEN"
    "            -- Round to start of week (Monday)"
    "            final_watermark := DATE_TRUNC('week', p_new_watermark);"
    "        WHEN 'exact' THEN"
    "            -- Use exact timestamp"
    "            final_watermark := p_new_watermark;"
    "        ELSE"
    "            -- Default to exact for unknown policies"
    "            final_watermark := p_new_watermark;"
    "    END CASE;"
    ""
    "    -- Only update if the final watermark is newer than current"
    "    IF current_watermark IS NULL OR final_watermark > current_watermark THEN"
    "        INSERT INTO ingest_watermark(source_name, watermark_key, watermark, watermark_policy, updated_at)"
    "        VALUES (p_source_name, p_watermark_key, final_watermark, p_policy, NOW())"
    "        ON CONFLICT (source_name, watermark_key) DO UPDATE"
    "            SET watermark = EXCLUDED.watermark,"
    "                watermark_policy = EXCLUDED.watermark_policy,"
                updated_at = NOW()
            WHERE EXCLUDED.watermark > ingest_watermark.watermark;"
    "    END IF;"
    "END;"
    "$$ LANGUAGE plpgsql;",
    ""
    "CREATE OR REPLACE FUNCTION public.get_ingest_watermark("
    "    p_source_name CITEXT,"
    "    p_watermark_key TEXT DEFAULT 'default'"
    ") RETURNS TIMESTAMPTZ AS $$"
    "DECLARE"
    "    result TIMESTAMPTZ;"
    "BEGIN"
    "    SELECT watermark INTO result"
    "    FROM ingest_watermark"
    "    WHERE source_name = p_source_name AND watermark_key = p_watermark_key;"
    "    RETURN result;"
    "END;"
    "$$ LANGUAGE plpgsql;",
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP POLICY IF EXISTS tenant_isolation_ppa ON ppa_contract;",
    "DROP POLICY IF EXISTS tenant_isolation_model_run ON model_run;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario ON scenario;",
    "DROP POLICY IF EXISTS tenant_isolation_curve_def ON curve_def;",
    "DROP POLICY IF EXISTS tenant_isolation_instrument ON instrument;",
    "ALTER TABLE ppa_contract DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE model_run DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE curve_def DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE instrument DISABLE ROW LEVEL SECURITY;",
    "DROP INDEX IF EXISTS idx_ppa_contract_tenant;",
    "DROP INDEX IF EXISTS idx_model_run_state;",
    "DROP INDEX IF EXISTS idx_scenario_tenant;",
    "DROP INDEX IF EXISTS idx_curve_def_instrument;",
    "DROP INDEX IF EXISTS idx_instrument_curve_key;",
    "DROP INDEX IF EXISTS idx_ingest_source_active;",
    "DROP INDEX IF EXISTS idx_ingest_watermark_updated;",
    "DROP TABLE IF EXISTS scenario_assumption_value;",
    "DROP TABLE IF EXISTS model_run;",
    "DROP TABLE IF EXISTS ppa_contract;",
    "DROP TABLE IF EXISTS scenario_driver;",
    "DROP TABLE IF EXISTS scenario;",
    "DROP TABLE IF EXISTS curve_def;",
    "DROP TABLE IF EXISTS instrument;",
    "DROP TABLE IF EXISTS ingest_watermark;",
    "DROP TABLE IF EXISTS ingest_source;",
    "DROP TABLE IF EXISTS file_ingest_log;",
    "DROP TABLE IF EXISTS tenant;",
    'DROP EXTENSION IF EXISTS "uuid-ossp";',
    "DROP EXTENSION IF EXISTS citext;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
