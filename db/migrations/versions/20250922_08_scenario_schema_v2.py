"""Scenario schema v2 with constraints, curve families, and provenance tracking."""

from alembic import op
import sqlalchemy as sa


revision = "20250922_08_scenario_schema_v2"
down_revision = "20250101_06_hardened_scenario_constraints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add scenario schema v2 features."""

    # Create curve_family table
    op.execute("""
        CREATE TABLE IF NOT EXISTS curve_family (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name TEXT NOT NULL,
            description TEXT,
            family_type TEXT NOT NULL CHECK (family_type IN ('demand', 'supply', 'price', 'renewable', 'load', 'weather', 'economic')),
            curve_keys JSONB NOT NULL DEFAULT '[]'::JSONB,
            default_parameters JSONB DEFAULT '{}'::JSONB,
            validation_rules JSONB DEFAULT '{}'::JSONB,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_by TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (name)
        );
    """)

    # Create scenario_constraint table
    op.execute("""
        CREATE TABLE IF NOT EXISTS scenario_constraint (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
            constraint_type TEXT NOT NULL CHECK (constraint_type IN ('budget', 'capacity', 'policy', 'technical', 'regulatory')),
            constraint_key TEXT NOT NULL,
            constraint_value JSONB NOT NULL,
            operator TEXT NOT NULL CHECK (operator IN ('=', '!=', '<', '<=', '>', '>=', 'in', 'not_in', 'between')),
            severity TEXT NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'error', 'critical')),
            is_enforced BOOLEAN NOT NULL DEFAULT FALSE,
            violation_message TEXT,
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (scenario_id, constraint_key)
        );
    """)

    # Create scenario_provenance table
    op.execute("""
        CREATE TABLE IF NOT EXISTS scenario_provenance (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
            data_source TEXT NOT NULL,
            source_version TEXT,
            data_timestamp TIMESTAMPTZ NOT NULL,
            transformation_hash TEXT NOT NULL,
            input_parameters JSONB DEFAULT '{}'::JSONB,
            quality_metrics JSONB DEFAULT '{}'::JSONB,
            lineage_metadata JSONB DEFAULT '{}'::JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    # Create scenario_curve_family table for many-to-many relationship
    op.execute("""
        CREATE TABLE IF NOT EXISTS scenario_curve_family (
            scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
            curve_family_id UUID NOT NULL REFERENCES curve_family(id) ON DELETE CASCADE,
            weight DECIMAL(3,2) DEFAULT 1.0 CHECK (weight >= 0 AND weight <= 1),
            is_primary BOOLEAN DEFAULT FALSE,
            parameters JSONB DEFAULT '{}'::JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (scenario_id, curve_family_id)
        );
    """)

    # Add new columns to scenario table
    op.execute("""
        ALTER TABLE scenario
        ADD COLUMN IF NOT EXISTS curve_families JSONB DEFAULT '[]'::JSONB;
    """)

    op.execute("""
        ALTER TABLE scenario
        ADD COLUMN IF NOT EXISTS constraints JSONB DEFAULT '[]'::JSONB;
    """)

    op.execute("""
        ALTER TABLE scenario
        ADD COLUMN IF NOT EXISTS provenance_enabled BOOLEAN DEFAULT FALSE;
    """)

    op.execute("""
        ALTER TABLE scenario
        ADD COLUMN IF NOT EXISTS schema_version INTEGER DEFAULT 2;
    """)

    # Create indexes for performance
    op.execute("CREATE INDEX IF NOT EXISTS idx_curve_family_type ON curve_family(family_type);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_curve_family_active ON curve_family(is_active) WHERE is_active = TRUE;")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scenario_constraint_scenario ON scenario_constraint(scenario_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scenario_constraint_type ON scenario_constraint(constraint_type);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scenario_provenance_scenario ON scenario_provenance(scenario_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scenario_provenance_timestamp ON scenario_provenance(data_timestamp);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scenario_curve_family_scenario ON scenario_curve_family(scenario_id);")

    # Enable RLS on new tables
    op.execute("ALTER TABLE curve_family ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE scenario_constraint ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE scenario_provenance ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE scenario_curve_family ENABLE ROW LEVEL SECURITY;")

    # Create RLS policies
    op.execute("""
        CREATE POLICY tenant_isolation_curve_family ON curve_family
        USING (created_by = current_setting('app.current_user') OR is_active = TRUE);
    """)

    op.execute("""
        CREATE POLICY tenant_isolation_scenario_constraint ON scenario_constraint
        USING (scenario_id IN (SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID));
    """)

    op.execute("""
        CREATE POLICY tenant_isolation_scenario_provenance ON scenario_provenance
        USING (scenario_id IN (SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID));
    """)

    op.execute("""
        CREATE POLICY tenant_isolation_scenario_curve_family ON scenario_curve_family
        USING (scenario_id IN (SELECT id FROM scenario WHERE tenant_id = current_setting('app.current_tenant')::UUID));
    """)

    # Create trigger for updated_at
    op.execute("""
        CREATE TRIGGER update_curve_family_updated_at
        BEFORE UPDATE ON curve_family
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)

    # Create functions for constraint validation
    op.execute("""
        CREATE OR REPLACE FUNCTION validate_scenario_constraints(scenario_id_param UUID)
        RETURNS TABLE (
            constraint_id UUID,
            constraint_key TEXT,
            severity TEXT,
            violation_message TEXT,
            is_violated BOOLEAN
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT
                sc.id,
                sc.constraint_key,
                sc.severity,
                sc.violation_message,
                CASE
                    WHEN sc.operator = '=' THEN NOT(sc.constraint_value = scenario_data)
                    WHEN sc.operator = '!=' THEN NOT(sc.constraint_value != scenario_data)
                    WHEN sc.operator = '<' THEN NOT(sc.constraint_value < scenario_data)
                    WHEN sc.operator = '<=' THEN NOT(sc.constraint_value <= scenario_data)
                    WHEN sc.operator = '>' THEN NOT(sc.constraint_value > scenario_data)
                    WHEN sc.operator = '>=' THEN NOT(sc.constraint_value >= scenario_data)
                    WHEN sc.operator = 'in' THEN NOT(sc.constraint_value @> scenario_data)
                    WHEN sc.operator = 'not_in' THEN NOT(NOT(sc.constraint_value @> scenario_data))
                    WHEN sc.operator = 'between' THEN NOT(sc.constraint_value->0 <= scenario_data AND scenario_data <= sc.constraint_value->1)
                    ELSE FALSE
                END as is_violated
            FROM scenario_constraint sc
            CROSS JOIN LATERAL (
                SELECT parameters->sc.constraint_key as scenario_data
                FROM scenario
                WHERE id = scenario_id_param
            ) data
            WHERE sc.scenario_id = scenario_id_param
              AND sc.is_enforced = TRUE;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Create function to get scenario curve families
    op.execute("""
        CREATE OR REPLACE FUNCTION get_scenario_curve_families(scenario_id_param UUID)
        RETURNS TABLE (
            curve_family_id UUID,
            name TEXT,
            family_type TEXT,
            weight DECIMAL,
            is_primary BOOLEAN,
            parameters JSONB
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT
                cf.id,
                cf.name,
                cf.family_type,
                scf.weight,
                scf.is_primary,
                scf.parameters
            FROM scenario_curve_family scf
            JOIN curve_family cf ON scf.curve_family_id = cf.id
            WHERE scf.scenario_id = scenario_id_param
            ORDER BY scf.is_primary DESC, scf.weight DESC;
        END;
        $$ LANGUAGE plpgsql;
    """)


def downgrade() -> None:
    """Remove scenario schema v2 features."""

    # Drop functions
    op.execute("DROP FUNCTION IF EXISTS get_scenario_curve_families(UUID);")
    op.execute("DROP FUNCTION IF EXISTS validate_scenario_constraints(UUID);")

    # Drop triggers
    op.execute("DROP TRIGGER IF EXISTS update_curve_family_updated_at ON curve_family;")

    # Drop policies
    op.execute("DROP POLICY IF EXISTS tenant_isolation_scenario_curve_family ON scenario_curve_family;")
    op.execute("DROP POLICY IF EXISTS tenant_isolation_scenario_provenance ON scenario_provenance;")
    op.execute("DROP POLICY IF EXISTS tenant_isolation_scenario_constraint ON scenario_constraint;")
    op.execute("DROP POLICY IF EXISTS tenant_isolation_curve_family ON curve_family;")

    # Disable RLS
    op.execute("ALTER TABLE scenario_curve_family DISABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE scenario_provenance DISABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE scenario_constraint DISABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE curve_family DISABLE ROW LEVEL SECURITY;")

    # Drop indexes
    op.execute("DROP INDEX IF EXISTS idx_scenario_curve_family_scenario;")
    op.execute("DROP INDEX IF EXISTS idx_scenario_provenance_timestamp;")
    op.execute("DROP INDEX IF EXISTS idx_scenario_provenance_scenario;")
    op.execute("DROP INDEX IF EXISTS idx_scenario_constraint_type;")
    op.execute("DROP INDEX IF EXISTS idx_scenario_constraint_scenario;")
    op.execute("DROP INDEX IF EXISTS idx_curve_family_active;")
    op.execute("DROP INDEX IF EXISTS idx_curve_family_type;")

    # Drop new columns from scenario
    op.execute("ALTER TABLE scenario DROP COLUMN IF EXISTS schema_version;")
    op.execute("ALTER TABLE scenario DROP COLUMN IF EXISTS provenance_enabled;")
    op.execute("ALTER TABLE scenario DROP COLUMN IF EXISTS constraints;")
    op.execute("ALTER TABLE scenario DROP COLUMN IF EXISTS curve_families;")

    # Drop tables
    op.execute("DROP TABLE IF EXISTS scenario_curve_family;")
    op.execute("DROP TABLE IF EXISTS scenario_provenance;")
    op.execute("DROP TABLE IF EXISTS scenario_constraint;")
    op.execute("DROP TABLE IF EXISTS curve_family;")
