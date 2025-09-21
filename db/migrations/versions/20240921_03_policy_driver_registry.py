"""Enhanced policy driver registry with versioning and parameter validation."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20240921_03"
down_revision = "20240921_02"
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    # Add versioning and metadata columns to scenario_driver table
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS version TEXT DEFAULT '1.0.0';
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active';
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS parameter_schema JSONB DEFAULT '{}'::JSONB;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS validation_rules JSONB DEFAULT '{}'::JSONB;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS implementation_path TEXT;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::JSONB;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS created_by TEXT;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS updated_by TEXT;
    """,
    """
    ALTER TABLE scenario_driver ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
    """,

    # Create policy_driver_version table for version history
    """
    CREATE TABLE IF NOT EXISTS policy_driver_version (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE CASCADE,
        version TEXT NOT NULL,
        parameter_schema JSONB NOT NULL DEFAULT '{}'::JSONB,
        validation_rules JSONB NOT NULL DEFAULT '{}'::JSONB,
        implementation_path TEXT NOT NULL,
        metadata JSONB DEFAULT '{}'::JSONB,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (driver_id, version)
    );
    """,

    # Create policy_driver_parameter table for parameter definitions
    """
    CREATE TABLE IF NOT EXISTS policy_driver_parameter (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE CASCADE,
        parameter_name TEXT NOT NULL,
        parameter_type TEXT NOT NULL,  -- 'string', 'number', 'boolean', 'array', 'object'
        required BOOLEAN NOT NULL DEFAULT FALSE,
        default_value JSONB,
        validation_rules JSONB DEFAULT '{}'::JSONB,
        description TEXT,
        order_index INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (driver_id, parameter_name)
    );
    """,

    # Create policy_driver_test_case table for validation
    """
    CREATE TABLE IF NOT EXISTS policy_driver_test_case (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        description TEXT,
        input_payload JSONB NOT NULL,
        expected_output JSONB,
        is_valid BOOLEAN NOT NULL DEFAULT TRUE,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (driver_id, name)
    );
    """,

    # Create indexes for performance
    """
    CREATE INDEX IF NOT EXISTS idx_policy_driver_version_driver_id ON policy_driver_version(driver_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_policy_driver_version_version ON policy_driver_version(version);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_policy_driver_parameter_driver_id ON policy_driver_parameter(driver_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_policy_driver_test_case_driver_id ON policy_driver_test_case(driver_id);
    """,

    # Enable RLS on new tables
    """
    ALTER TABLE policy_driver_version ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE policy_driver_parameter ENABLE ROW LEVEL SECURITY;
    """,
    """
    ALTER TABLE policy_driver_test_case ENABLE ROW LEVEL SECURITY;
    """,

    # Create RLS policies
    """
    CREATE POLICY tenant_isolation_policy_driver_version ON policy_driver_version
        USING (driver_id IN (
            SELECT id FROM scenario_driver WHERE type = 'policy'
        ));
    """,
    """
    CREATE POLICY tenant_isolation_policy_driver_parameter ON policy_driver_parameter
        USING (driver_id IN (
            SELECT id FROM scenario_driver WHERE type = 'policy'
        ));
    """,
    """
    CREATE POLICY tenant_isolation_policy_driver_test_case ON policy_driver_test_case
        USING (driver_id IN (
            SELECT id FROM scenario_driver WHERE type = 'policy'
        ));
    """,

    # Create trigger for updated_at
    """
    CREATE TRIGGER update_scenario_driver_updated_at
        BEFORE UPDATE ON scenario_driver
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,

    # Create function to validate driver parameter compatibility
    """
    CREATE OR REPLACE FUNCTION validate_driver_parameter_compatibility(
        driver_id UUID,
        parameter_payload JSONB
    ) RETURNS BOOLEAN AS $$
    DECLARE
        param_record RECORD;
        param_value JSONB;
        validation_result BOOLEAN := TRUE;
    BEGIN
        -- Check each required parameter
        FOR param_record IN
            SELECT parameter_name, required, parameter_type, validation_rules
            FROM policy_driver_parameter
            WHERE driver_id = driver_id AND required = TRUE
        LOOP
            -- Check if required parameter exists
            IF NOT (parameter_payload ? param_record.parameter_name) THEN
                RETURN FALSE;
            END IF;

            param_value := parameter_payload -> param_record.parameter_name;

            -- Type validation
            CASE param_record.parameter_type
                WHEN 'string' THEN
                    IF jsonb_typeof(param_value) != 'string' THEN
                        RETURN FALSE;
                    END IF;
                WHEN 'number' THEN
                    IF jsonb_typeof(param_value) NOT IN ('number') THEN
                        RETURN FALSE;
                    END IF;
                WHEN 'boolean' THEN
                    IF jsonb_typeof(param_value) != 'boolean' THEN
                        RETURN FALSE;
                    END IF;
                WHEN 'array' THEN
                    IF jsonb_typeof(param_value) != 'array' THEN
                        RETURN FALSE;
                    END IF;
                WHEN 'object' THEN
                    IF jsonb_typeof(param_value) != 'object' THEN
                        RETURN FALSE;
                    END IF;
            END CASE;

            -- Additional validation rules
            IF param_record.validation_rules != '{}'::JSONB THEN
                -- Custom validation logic would go here
                -- For now, we'll assume validation passes
                CONTINUE;
            END IF;
        END LOOP;

        RETURN TRUE;
    END;
    $$ LANGUAGE plpgsql;
    """,

    # Create function to get driver version history
    """
    CREATE OR REPLACE FUNCTION get_driver_version_history(driver_id UUID)
    RETURNS TABLE (
        version TEXT,
        parameter_schema JSONB,
        validation_rules JSONB,
        implementation_path TEXT,
        metadata JSONB,
        created_by TEXT,
        created_at TIMESTAMPTZ
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            pdv.version,
            pdv.parameter_schema,
            pdv.validation_rules,
            pdv.implementation_path,
            pdv.metadata,
            pdv.created_by,
            pdv.created_at
        FROM policy_driver_version pdv
        WHERE pdv.driver_id = driver_id
        ORDER BY
            CASE
                WHEN version ~ '^[0-9]+\.[0-9]+\.[0-9]+$' THEN
                    string_to_array(version, '.')::int[]
                ELSE
                    ARRAY[0, 0, 0]
            END DESC;
    END;
    $$ LANGUAGE plpgsql;
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP FUNCTION IF EXISTS get_driver_version_history(UUID);",
    "DROP FUNCTION IF EXISTS validate_driver_parameter_compatibility(UUID, JSONB);",
    "DROP TRIGGER IF EXISTS update_scenario_driver_updated_at ON scenario_driver;",
    "DROP POLICY IF EXISTS tenant_isolation_policy_driver_test_case ON policy_driver_test_case;",
    "DROP POLICY IF EXISTS tenant_isolation_policy_driver_parameter ON policy_driver_parameter;",
    "DROP POLICY IF EXISTS tenant_isolation_policy_driver_version ON policy_driver_version;",
    "ALTER TABLE policy_driver_test_case DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE policy_driver_parameter DISABLE ROW LEVEL SECURITY;",
    "ALTER TABLE policy_driver_version DISABLE ROW LEVEL SECURITY;",
    "DROP INDEX IF EXISTS idx_policy_driver_test_case_driver_id;",
    "DROP INDEX IF EXISTS idx_policy_driver_parameter_driver_id;",
    "DROP INDEX IF EXISTS idx_policy_driver_version_version;",
    "DROP INDEX IF EXISTS idx_policy_driver_version_driver_id;",
    "DROP TABLE IF EXISTS policy_driver_test_case;",
    "DROP TABLE IF EXISTS policy_driver_parameter;",
    "DROP TABLE IF EXISTS policy_driver_version;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS updated_at;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS updated_by;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS created_by;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS metadata;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS implementation_path;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS validation_rules;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS parameter_schema;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS status;",
    "ALTER TABLE scenario_driver DROP COLUMN IF EXISTS version;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
