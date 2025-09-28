"""Auto reforecast trigger/job persistence with tenant isolation."""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "20250928_09_auto_reforecast_persistence"
down_revision = "20250922_08_scenario_schema_v2"
branch_labels = None
depends_on = None


SCHEMA_UPGRADE_STATEMENTS = [
    # Trigger table
    """
    CREATE TABLE IF NOT EXISTS auto_reforecast_trigger (
        trigger_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        conditions JSONB NOT NULL,
        forecast_config JSONB NOT NULL,
        cooldown_minutes INTEGER NOT NULL DEFAULT 30,
        priority NUMERIC(4,3) NOT NULL DEFAULT 1.0,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        last_triggered TIMESTAMPTZ,
        trigger_count BIGINT NOT NULL DEFAULT 0,
        debounce_window_seconds INTEGER NOT NULL DEFAULT 300,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_by TEXT,
        UNIQUE (tenant_id, name)
    );
    """,
    # Trigger indexes
    """
    CREATE INDEX IF NOT EXISTS idx_auto_reforecast_trigger_tenant_enabled
        ON auto_reforecast_trigger (tenant_id, enabled);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_auto_reforecast_trigger_tenant_priority
        ON auto_reforecast_trigger (tenant_id, priority DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_auto_reforecast_trigger_conditions_gin
        ON auto_reforecast_trigger USING GIN (conditions);
    """,

    # Job table
    """
    CREATE TABLE IF NOT EXISTS auto_reforecast_job (
        job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
        trigger_id UUID NOT NULL REFERENCES auto_reforecast_trigger(trigger_id) ON DELETE CASCADE,
        event_id UUID NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        priority NUMERIC(4,3) NOT NULL DEFAULT 1.0,
        scheduled_for TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        error_message TEXT,
        trigger_event JSONB NOT NULL,
        forecast_config JSONB NOT NULL,
        metrics JSONB NOT NULL DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_by TEXT,
        UNIQUE (tenant_id, event_id)
    );
    """,
    # Job indexes
    """
    CREATE INDEX IF NOT EXISTS idx_auto_reforecast_job_queue
        ON auto_reforecast_job (tenant_id, status, scheduled_for DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_auto_reforecast_job_trigger_sched
        ON auto_reforecast_job (tenant_id, trigger_id, scheduled_for DESC);
    """,

    # RLS enablement
    """
    ALTER TABLE auto_reforecast_trigger ENABLE ROW LEVEL SECURITY;
    """,
    """
    CREATE POLICY tenant_isolation_auto_reforecast_trigger
        ON auto_reforecast_trigger
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,
    """
    ALTER TABLE auto_reforecast_job ENABLE ROW LEVEL SECURITY;
    """,
    """
    CREATE POLICY tenant_isolation_auto_reforecast_job
        ON auto_reforecast_job
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,

    # updated_at triggers
    """
    CREATE TRIGGER update_auto_reforecast_trigger_updated_at
        BEFORE UPDATE ON auto_reforecast_trigger
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,
    """
    CREATE TRIGGER update_auto_reforecast_job_updated_at
        BEFORE UPDATE ON auto_reforecast_job
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP TRIGGER IF EXISTS update_auto_reforecast_job_updated_at ON auto_reforecast_job;",
    "DROP TRIGGER IF EXISTS update_auto_reforecast_trigger_updated_at ON auto_reforecast_trigger;",
    "DROP POLICY IF EXISTS tenant_isolation_auto_reforecast_job ON auto_reforecast_job;",
    "ALTER TABLE auto_reforecast_job DISABLE ROW LEVEL SECURITY;",
    "DROP POLICY IF EXISTS tenant_isolation_auto_reforecast_trigger ON auto_reforecast_trigger;",
    "ALTER TABLE auto_reforecast_trigger DISABLE ROW LEVEL SECURITY;",
    "DROP INDEX IF EXISTS idx_auto_reforecast_job_trigger_sched;",
    "DROP INDEX IF EXISTS idx_auto_reforecast_job_queue;",
    "DROP TABLE IF EXISTS auto_reforecast_job;",
    "DROP INDEX IF EXISTS idx_auto_reforecast_trigger_conditions_gin;",
    "DROP INDEX IF EXISTS idx_auto_reforecast_trigger_tenant_priority;",
    "DROP INDEX IF EXISTS idx_auto_reforecast_trigger_tenant_enabled;",
    "DROP TABLE IF EXISTS auto_reforecast_trigger;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
