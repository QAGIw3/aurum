"""Ensure all tables have tenant_id columns and RLS policies."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20240921_05"
down_revision = "20240921_04"
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    # Add tenant_id columns to tables that don't have them
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS tenant_id UUID;
    """,
    """
    ALTER TABLE model_run ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT uuid_generate_v4();
    """,
    """
    ALTER TABLE file_ingest_log ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT uuid_generate_v4();
    """,
    """
    ALTER TABLE ingest_source ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT uuid_generate_v4();
    """,

    # Set default tenant_id values for existing records
    """
    UPDATE scenario_driver
    SET tenant_id = s.tenant_id
    FROM scenario s
    WHERE scenario_driver.name = s.name
    AND scenario_driver.tenant_id IS NULL;
    """,

    # Make tenant_id NOT NULL for scenario_driver after backfill
    """
    ALTER TABLE scenario_driver ALTER COLUMN tenant_id SET NOT NULL;
    """,

    # Add foreign key constraints
    """
    ALTER TABLE scenario_driver
    ADD CONSTRAINT IF NOT EXISTS fk_scenario_driver_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE RESTRICT;
    """,
    """
    ALTER TABLE model_run
    ADD CONSTRAINT IF NOT EXISTS fk_model_run_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE RESTRICT;
    """,
    """
    ALTER TABLE file_ingest_log
    ADD CONSTRAINT IF NOT EXISTS fk_file_ingest_log_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE RESTRICT;
    """,
    """
    ALTER TABLE ingest_source
    ADD CONSTRAINT IF NOT EXISTS fk_ingest_source_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE RESTRICT;
    """,

    # Enable RLS on tables that don't have it
    """
    ALTER TABLE scenario_driver ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE scenario_assumption_value ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE assumption ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE model_run ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE file_ingest_log ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE ingest_source ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE ingest_watermark ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE ingest_slice ENABLE ROW LEVEL SECURITY;
    """,

    # Create RLS policies
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_scenario_driver ON scenario_driver
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_scenario_assumption_value ON scenario_assumption_value
        USING (scenario_id IN (
            SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_assumption ON assumption
        USING (scenario_id IN (
            SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_model_run ON model_run
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_file_ingest_log ON file_ingest_log
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_ingest_source ON ingest_source
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_ingest_watermark ON ingest_watermark
        USING (source_name IN (
            SELECT name FROM ingest_source WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,
    """
    CREATE POLICY IF NOT EXISTS tenant_isolation_ingest_slice ON ingest_slice
        USING (source_name IN (
            SELECT name FROM ingest_source WHERE tenant_id = current_setting('app.current_tenant')::UUID
        ));
    """,

    # Add indexes for performance
    """
    CREATE INDEX IF NOT EXISTS idx_scenario_driver_tenant_id ON scenario_driver(tenant_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_run_tenant_id ON model_run(tenant_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_file_ingest_log_tenant_id ON file_ingest_log(tenant_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ingest_source_tenant_id ON ingest_source(tenant_id);
    """,

    # Add additional indexes from earlier migrations that might be missing
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
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    # Drop indexes
    "DROP INDEX IF EXISTS idx_scenario_feature_flag_tenant;",
    "DROP INDEX IF EXISTS idx_scenario_run_event_created_at;",
    "DROP INDEX IF EXISTS idx_scenario_output_metric_timestamp;",
    "DROP INDEX IF EXISTS idx_scenario_output_scenario_run;",
    "DROP INDEX IF EXISTS idx_scenario_run_started_at;",
    "DROP INDEX IF EXISTS idx_scenario_run_queued_at;",
    "DROP INDEX IF EXISTS idx_scenario_run_status;",
    "DROP INDEX IF EXISTS idx_scenario_run_tenant_id;",
    "DROP INDEX IF EXISTS idx_scenario_run_scenario_id;",
    "DROP INDEX IF EXISTS idx_ingest_source_tenant_id;",
    "DROP INDEX IF EXISTS idx_file_ingest_log_tenant_id;",
    "DROP INDEX IF EXISTS idx_model_run_tenant_id;",
    "DROP INDEX IF EXISTS idx_scenario_driver_tenant_id;",

    # Drop RLS policies
    "DROP POLICY IF EXISTS tenant_isolation_ingest_slice ON ingest_slice;",
    "DROP POLICY IF EXISTS tenant_isolation_ingest_watermark ON ingest_watermark;",
    "DROP POLICY IF EXISTS tenant_isolation_ingest_source ON ingest_source;",
    "DROP POLICY IF EXISTS tenant_isolation_file_ingest_log ON file_ingest_log;",
    "DROP POLICY IF EXISTS tenant_isolation_model_run ON model_run;",
    "DROP POLICY IF EXISTS tenant_isolation_assumption ON assumption;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_assumption_value ON scenario_assumption_value;",
    "DROP POLICY IF EXISTS tenant_isolation_scenario_driver ON scenario_driver;",

    # Disable RLS
    "ALTER TABLE ingest_slice DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE ingest_watermark DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE ingest_source DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE file_ingest_log DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE model_run DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE assumption DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_assumption_value DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE scenario_driver DISABLE ROW LEVEL SECURITY;",

    # Drop foreign key constraints
    "ALTER TABLE ingest_source DROP CONSTRAINT IF EXISTS fk_ingest_source_tenant;",
    "ALTER TABLE file_ingest_log DROP CONSTRAINT IF EXISTS fk_file_ingest_log_tenant;",
    "ALTER TABLE model_run DROP CONSTRAINT IF EXISTS fk_model_run_tenant;",
    "ALTER TABLE scenario_driver DROP CONSTRAINT IF EXISTS fk_scenario_driver_tenant;",

    # Drop tenant_id columns
    "ALTER TABLE ingest_source DROP COLUMN IF EXISTS tenant_id;",
    "ALTER TABLE file_ingest_log DROP COLUMN IF EXISTS tenant_id;",
    "ALTER TABLE model_run DROP COLUMN IF EXISTS tenant_id;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS tenant_id;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
