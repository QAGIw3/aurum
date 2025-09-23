"""Rate limit overrides persistence with RLS."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20250101_09"
down_revision = "20250922_08"
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    # Create rate_limit_override table for tenant-specific rate limit overrides
    """
    CREATE TABLE IF NOT EXISTS rate_limit_override (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        tenant_id UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
        path_prefix TEXT NOT NULL,  -- e.g., "/v1/curves", "/v2/scenarios"
        requests_per_second INTEGER NOT NULL,
        burst_capacity INTEGER NOT NULL,
        daily_cap INTEGER DEFAULT 100000,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (tenant_id, path_prefix)
    );
    """,

    # Create indexes for performance
    """
    CREATE INDEX IF NOT EXISTS idx_rate_limit_override_tenant_id ON rate_limit_override(tenant_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rate_limit_override_path_prefix ON rate_limit_override(path_prefix);
    """,

    # Enable RLS on the new table
    """
    ALTER TABLE rate_limit_override ENABLE ROW LEVEL SECURITY;
    """,

    # Create RLS policy
    """
    CREATE POLICY tenant_isolation_rate_limit_override ON rate_limit_override
        USING (tenant_id = current_setting('app.current_tenant')::UUID);
    """,

    # Create trigger to update updated_at timestamps
    """
    CREATE TRIGGER update_rate_limit_override_updated_at
        BEFORE UPDATE ON rate_limit_override
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP POLICY IF EXISTS tenant_isolation_rate_limit_override ON rate_limit_override;",
    "ALTER TABLE rate_limit_override DISABLE ROW LEVEL SECURITY;",
    "DROP INDEX IF EXISTS idx_rate_limit_override_path_prefix;",
    "DROP INDEX IF EXISTS idx_rate_limit_override_tenant_id;",
    "DROP TABLE IF EXISTS rate_limit_override;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
