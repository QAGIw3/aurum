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
    "DROP TABLE IF EXISTS scenario_assumption_value;",
    "DROP TABLE IF EXISTS model_run;",
    "DROP TABLE IF EXISTS ppa_contract;",
    "DROP TABLE IF EXISTS scenario_driver;",
    "DROP TABLE IF EXISTS scenario;",
    "DROP TABLE IF EXISTS curve_def;",
    "DROP TABLE IF EXISTS instrument;",
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
