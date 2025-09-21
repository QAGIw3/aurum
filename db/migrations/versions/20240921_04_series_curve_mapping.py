"""Add series to curve mapping table for external data integration."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20240921_04"
down_revision = "20240921_03"
branch_labels = None
depends_on = None

SCHEMA_UPGRADE_STATEMENTS = [
    # Create series_curve_map table
    """
    CREATE TABLE IF NOT EXISTS series_curve_map (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        external_provider TEXT NOT NULL,  -- FRED, EIA, etc.
        external_series_id TEXT NOT NULL,  -- Provider-specific series identifier
        curve_key TEXT NOT NULL,          -- Internal curve key for mapping
        mapping_confidence DECIMAL(3,2) NOT NULL DEFAULT 1.0,  -- 0.0 to 1.0 confidence score
        mapping_method TEXT NOT NULL DEFAULT 'manual',  -- manual, automated, heuristic
        mapping_notes TEXT,                 -- Human-readable mapping rationale
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_by TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (external_provider, external_series_id, curve_key)
    );
    """,

    # Create indexes for performance
    """
    CREATE INDEX IF NOT EXISTS idx_series_curve_map_provider_series ON series_curve_map(external_provider, external_series_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_series_curve_map_curve_key ON series_curve_map(curve_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_series_curve_map_active ON series_curve_map(is_active) WHERE is_active = TRUE;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_series_curve_map_confidence ON series_curve_map(mapping_confidence DESC);
    """,

    # Enable RLS
    """
    ALTER TABLE series_curve_map ENABLE ROW LEVEL SECURITY;
    """,

    # Create RLS policies
    """
    CREATE POLICY tenant_isolation_series_curve_map ON series_curve_map
        USING (TRUE);  -- Public read access for mapping queries
    """,

    # Create trigger for updated_at
    """
    CREATE TRIGGER update_series_curve_map_updated_at
        BEFORE UPDATE ON series_curve_map
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """,

    # Create function to get active mappings for a provider
    """
    CREATE OR REPLACE FUNCTION get_active_series_curve_mappings(
        p_provider TEXT DEFAULT NULL,
        p_confidence_threshold DECIMAL DEFAULT 0.8
    )
    RETURNS TABLE (
        external_provider TEXT,
        external_series_id TEXT,
        curve_key TEXT,
        mapping_confidence DECIMAL,
        mapping_method TEXT,
        mapping_notes TEXT,
        is_active BOOLEAN
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            scm.external_provider,
            scm.external_series_id,
            scm.curve_key,
            scm.mapping_confidence,
            scm.mapping_method,
            scm.mapping_notes,
            scm.is_active
        FROM series_curve_map scm
        WHERE (p_provider IS NULL OR scm.external_provider = p_provider)
          AND scm.is_active = TRUE
          AND scm.mapping_confidence >= p_confidence_threshold
        ORDER BY scm.mapping_confidence DESC, scm.external_provider, scm.external_series_id;
    END;
    $$ LANGUAGE plpgsql;
    """,

    # Create function to find potential mappings for unmapped series
    """
    CREATE OR REPLACE FUNCTION find_potential_series_mappings(
        p_provider TEXT,
        p_series_id TEXT,
        p_limit INTEGER DEFAULT 10
    )
    RETURNS TABLE (
        curve_key TEXT,
        similarity_score DECIMAL,
        matching_criteria JSONB
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            c.curve_key,
            0.5::DECIMAL as similarity_score,  -- Placeholder for actual similarity logic
            jsonb_build_object(
                'provider_match', FALSE,
                'series_id_pattern', FALSE,
                'metadata_similarity', FALSE
            ) as matching_criteria
        FROM instrument c
        WHERE c.curve_key LIKE '%' || substring(p_series_id from '\d+') || '%'  -- Simple numeric pattern matching
        ORDER BY similarity_score DESC
        LIMIT p_limit;
    END;
    $$ LANGUAGE plpgsql;
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP FUNCTION IF EXISTS find_potential_series_mappings(TEXT, TEXT, INTEGER);",
    "DROP FUNCTION IF EXISTS get_active_series_curve_mappings(TEXT, DECIMAL);",
    "DROP TRIGGER IF EXISTS update_series_curve_map_updated_at ON series_curve_map;",
    "DROP POLICY IF EXISTS tenant_isolation_series_curve_map ON series_curve_map;",
    "ALTER TABLE series_curve_map DISABLE ROW LEVEL SECURITY;",
    "DROP INDEX IF EXISTS idx_series_curve_map_confidence;",
    "DROP INDEX IF EXISTS idx_series_curve_map_active;",
    "DROP INDEX IF EXISTS idx_series_curve_map_curve_key;",
    "DROP INDEX IF EXISTS idx_series_curve_map_provider_series;",
    "DROP TABLE IF EXISTS series_curve_map;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
