"""Hardened scenario constraints and idempotency support

Revision ID: 20250101_06_hardened_scenario_constraints
Revises: 20240921_05_tenant_rls_enforcement
Create Date: 2025-01-01 00:00:00
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "20250101_06_hardened_scenario_constraints"
down_revision = "20240921_05_tenant_rls_enforcement"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Scenario uniqueness per tenant + case-insensitive name
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_scenario_tenant_name_unique
        ON scenario (tenant_id, lower(name))
        WHERE tenant_id IS NOT NULL;
        """
    )
    op.execute(
        """
        ALTER TABLE scenario
        ADD CONSTRAINT scenario_tenant_name_unique
        UNIQUE USING INDEX idx_scenario_tenant_name_unique;
        """
    )

    # Model run idempotency and scenario/version uniqueness
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_model_run_scenario_hash_unique
        ON model_run (scenario_id, version_hash)
        WHERE scenario_id IS NOT NULL AND version_hash IS NOT NULL;
        """
    )
    op.execute(
        """
        ALTER TABLE model_run
        ADD CONSTRAINT model_run_scenario_hash_unique
        UNIQUE USING INDEX idx_model_run_scenario_hash_unique;
        """
    )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_model_run_idempotency_key_unique
        ON model_run (idempotency_key)
        WHERE idempotency_key IS NOT NULL;
        """
    )
    op.execute(
        """
        ALTER TABLE model_run
        ADD CONSTRAINT model_run_idempotency_key_unique
        UNIQUE USING INDEX idx_model_run_idempotency_key_unique;
        """
    )

    # Support columns and documentation
    op.execute(
        """
        ALTER TABLE model_run
        ADD COLUMN IF NOT EXISTS version_hash TEXT;
        """
    )
    op.execute(
        """
        ALTER TABLE model_run
        ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
        """
    )

    op.execute(
        """
        COMMENT ON CONSTRAINT scenario_tenant_name_unique ON scenario IS
        'Ensures scenario names are unique within each tenant';
        """
    )
    op.execute(
        """
        COMMENT ON CONSTRAINT model_run_scenario_hash_unique ON model_run IS
        'Ensures only one run exists per scenario version hash for idempotency';
        """
    )
    op.execute(
        """
        COMMENT ON CONSTRAINT model_run_idempotency_key_unique ON model_run IS
        'Ensures idempotency keys are unique when provided';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN model_run.version_hash IS
        'Hash of scenario version for idempotent run detection';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN model_run.idempotency_key IS
        'User-provided key for idempotent run creation';
        """
    )

    # Supporting indexes for performance
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scenario_tenant_id
        ON scenario (tenant_id)
        WHERE tenant_id IS NOT NULL;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_run_scenario_id
        ON model_run (scenario_id)
        WHERE scenario_id IS NOT NULL;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_run_state
        ON model_run (state)
        WHERE state IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE model_run
        DROP CONSTRAINT IF EXISTS model_run_idempotency_key_unique;
        """
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_model_run_idempotency_key_unique;"
    )

    op.execute(
        """
        ALTER TABLE model_run
        DROP CONSTRAINT IF EXISTS model_run_scenario_hash_unique;
        """
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_model_run_scenario_hash_unique;"
    )

    op.execute(
        """
        ALTER TABLE scenario
        DROP CONSTRAINT IF EXISTS scenario_tenant_name_unique;
        """
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_scenario_tenant_name_unique;"
    )

    op.execute(
        "DROP INDEX IF EXISTS idx_model_run_state;"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_model_run_scenario_id;"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_scenario_tenant_id;"
    )

    op.execute(
        """
        COMMENT ON COLUMN model_run.version_hash IS NULL;
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN model_run.idempotency_key IS NULL;
        """
    )

    op.execute(
        """
        ALTER TABLE model_run
        DROP COLUMN IF EXISTS idempotency_key;
        """
    )
    op.execute(
        """
        ALTER TABLE model_run
        DROP COLUMN IF EXISTS version_hash;
        """
    )
