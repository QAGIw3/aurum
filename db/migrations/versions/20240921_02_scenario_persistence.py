"""Enhanced scenario persistence with assumptions, outputs, and lifecycle management."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20240921_02"
down_revision = "20240921_01"
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    # Add missing columns to scenario table
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'created';
    """,
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS assumptions JSONB DEFAULT '[]'::JSONB;
    """,
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS parameters JSONB DEFAULT '{}'::JSONB;
    """,
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]'::JSONB;
    """,
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1;
    """,
    """
    ALTER TABLE scenario ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
    """,

    # Create scenario_run table
    """
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
    """,

    # Create scenario_output table
    """
    CREATE TABLE IF NOT EXISTS scenario_output (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
        tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE RESTRICT,
        timestamp TIMESTAMPTZ NOT NULL,
        metric_name TEXT NOT NULL,
        value REAL NOT NULL,
        unit TEXT NOT NULL,
        tags JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        INDEX idx_scenario_output_run_id (scenario_run_id),
        INDEX idx_scenario_output_timestamp (timestamp),
        INDEX idx_scenario_output_metric (metric_name),
        INDEX idx_scenario_output_tenant (tenant_id)
    );
    """,

    # Create scenario_run_output_pointer table for linking runs to output locations
    """
    CREATE TABLE IF NOT EXISTS scenario_run_output_pointer (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
        output_type TEXT NOT NULL,  -- 'file', 'database', 'api', etc.
        output_location TEXT NOT NULL,  -- S3 path, table name, API endpoint, etc.
        metadata JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (scenario_run_id, output_type)
    );
    """,

    # Create scenario_run_event table for audit trail
    """
    CREATE TABLE IF NOT EXISTS scenario_run_event (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        scenario_run_id UUID NOT NULL REFERENCES scenario_run(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,  -- 'queued', 'started', 'completed', 'failed', 'cancelled', 'retry'
        event_data JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        INDEX idx_scenario_run_event_run_id (scenario_run_id),
        INDEX idx_scenario_run_event_type (event_type)
    );
    """,

    # Create scenario_feature_flag table for tenant-specific feature toggles
    """
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
    """,

    # Create indexes for performance
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_scenario_id ON scenario_run(scenario_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_tenant_id ON scenario_run(tenant_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_status ON scenario_run(status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_queued_at ON scenario_run(queued_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_started_at ON scenario_run(started_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_output_scenario_run ON scenario_output(scenario_run_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_output_metric_timestamp ON scenario_output(metric_name, timestamp);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_run_event_created_at ON scenario_run_event(created_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_feature_flag_tenant ON scenario_feature_flag(tenant_id);
    """,

    # Enable RLS on new tables
    """
    ALTER TABLE scenario_run ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE scenario_output ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE scenario_run_output_pointer ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE scenario_run_event ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE scenario_feature_flag ENABLE ROW LEVEL SECURITY;
    """,

    # Create RLS policies
    """
    CREATE POLICY tenant_isolation_scenario_run ON scenario_run
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY tenant_isolation_scenario_output ON scenario_output
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY tenant_isolation_scenario_run_output_pointer ON scenario_run_output_pointer
        USING (scenario_run_id IN (
            SELECT id FROM scenario_run WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,
    """
    CREATE POLICY tenant_isolation_scenario_run_event ON scenario_run_event
        USING (scenario_run_id IN (
            SELECT id FROM scenario_run WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,
    """
    CREATE POLICY tenant_isolation_scenario_feature_flag ON scenario_feature_flag
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,

    # Create trigger to update updated_at timestamps
    """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    """,
    """
    CREATE TRIGGER update_scenario_updated_at
        BEFORE UPDATE ON scenario
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,
    """
    CREATE TRIGGER update_scenario_feature_flag_updated_at
        BEFORE UPDATE ON scenario_feature_flag
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,

    # Create function to calculate scenario run duration
    """
    CREATE OR REPLACE FUNCTION calculate_scenario_run_duration(run_id UUID)
    RETURNS REAL AS $$
    DECLARE
        start_time TIMESTAMPTZ;
        end_time TIMESTAMPTZ;
        duration_seconds REAL;
    BEGIN
        SELECT started_at, completed_at
        INTO start_time, end_time
        FROM scenario_run
        WHERE id = run_id;

        IF start_time IS NOT NULL THEN
            IF end_time IS NOT NULL THEN
                duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
            ELSE
                duration_seconds := EXTRACT(EPOCH FROM (NOW() - start_time));
            END IF;
        ELSE
            duration_seconds := NULL;
        END IF;

        RETURN duration_seconds;
    END;
    $$ LANGUAGE plpgsql;
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP POLICY IF EXISTS tenant_isolation_scenario_feature_flag ON scenario_feature_flag;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_run_event ON scenario_run_event;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_run_output_pointer ON scenario_run_output_pointer;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_output ON scenario_output;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_run ON scenario_run;",
    "ALTER TABLE scenario_feature_flag DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_run_event DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_run_output_pointer DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_output DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_run DISABLE ROW LEVEL SECURITY;",
    "DROP TRIGGER IF EXISTS update_scenario_feature_flag_updated_at ON scenario_feature_flag;",
    "DROP TRIGGER IF EXISTS update_scenario_updated_at ON scenario;",
    "DROP FUNCTION IF EXISTS calculate_scenario_run_duration(UUID);",
    "DROP FUNCTION IF EXISTS update_updated_at_column();",
    "DROP INDEX IF EXISTS idx_scenario_feature_flag_tenant;",
    "DROP INDEX IF EXISTS idx_scenario_run_event_created_at;",
    "DROP INDEX IF EXISTS idx_scenario_output_metric_timestamp;",
    "DROP INDEX IF EXISTS idx_scenario_output_scenario_run;",
    "DROP INDEX IF EXISTS idx_scenario_run_started_at;",
    "DROP INDEX IF EXISTS idx_scenario_run_queued_at;",
    "DROP INDEX IF EXISTS idx_scenario_run_status;",
    "DROP INDEX IF EXISTS idx_scenario_run_tenant_id;",
    "DROP INDEX IF EXISTS idx_scenario_run_scenario_id;",
    "DROP TABLE IF EXISTS scenario_run_event;",
    "DROP TABLE IF EXISTS scenario_run_output_pointer;",
    "DROP TABLE IF EXISTS scenario_output;",
    "DROP TABLE IF EXISTS scenario_run;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS archived_at;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS version;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS tags;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS parameters;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS assumptions;",
    "ALTER TABLE scenario DROP COLUMN IF EXISTS status;",
    "DROP TABLE IF EXISTS scenario_feature_flag;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
