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

    # Create function to find potential mappings for unmapped series using ML-based similarity
    """
    CREATE OR REPLACE FUNCTION find_potential_series_mappings(
        p_provider TEXT,
        p_series_id TEXT,
        p_series_name TEXT DEFAULT NULL,
        p_description TEXT DEFAULT NULL,
        p_units TEXT DEFAULT NULL,
        p_limit INTEGER DEFAULT 10
    )
    RETURNS TABLE (
        curve_key TEXT,
        similarity_score DECIMAL,
        confidence_score DECIMAL,
        reasoning JSONB
    ) AS $$
    DECLARE
        series_metadata JSONB;
        curve_record RECORD;
        text_similarity DECIMAL;
        pattern_similarity DECIMAL;
        overall_score DECIMAL;
        reasoning_data JSONB;
    BEGIN
        -- Build series metadata
        series_metadata := jsonb_build_object(
            'provider', p_provider,
            'series_id', p_series_id,
            'name', COALESCE(p_series_name, ''),
            'description', COALESCE(p_description, ''),
            'units', COALESCE(p_units, ''),
            'frequency', 'hourly',  -- Default assumption
            'category', CASE
                WHEN p_series_name ~* 'generation|gen|supply' THEN 'generation'
                WHEN p_series_name ~* 'demand|load|consumption' THEN 'demand'
                WHEN p_series_name ~* 'price|lmp|cost' THEN 'price'
                ELSE 'other'
            END,
            'tags', CASE
                WHEN p_series_name ~* 'solar|wind|renewable' THEN jsonb_build_array('renewable')
                WHEN p_series_name ~* 'coal|gas|nuclear' THEN jsonb_build_array('conventional')
                ELSE jsonb_build_array('general')
            END
        );

        -- For each available curve, calculate similarity
        FOR curve_record IN
            SELECT
                curve_key,
                name,
                description,
                curve_family,
                units,
                geography,
                category
            FROM instrument
            WHERE curve_key IS NOT NULL AND curve_key != ''
            ORDER BY curve_key
        LOOP
            -- Calculate text similarity (simplified TF-IDF style)
            text_similarity := 0.0;
            reasoning_data := jsonb_build_object();

            IF series_metadata->>'name' != '' AND curve_record.name IS NOT NULL THEN
                IF similarity(series_metadata->>'name', curve_record.name) > 0.3 THEN
                    text_similarity := text_similarity + 0.4;
                    reasoning_data := reasoning_data || jsonb_build_object('name_match', true);
                END IF;
            END IF;

            IF series_metadata->>'description' != '' AND curve_record.description IS NOT NULL THEN
                IF similarity(series_metadata->>'description', curve_record.description) > 0.3 THEN
                    text_similarity := text_similarity + 0.3;
                    reasoning_data := reasoning_data || jsonb_build_object('description_match', true);
                END IF;
            END IF;

            -- Unit compatibility
            IF series_metadata->>'units' = curve_record.units THEN
                text_similarity := text_similarity + 0.2;
                reasoning_data := reasoning_data || jsonb_build_object('unit_match', true);
            ELSIF series_metadata->>'units' IN ('MWh', 'MW') AND curve_record.units IN ('MWh', 'MW') THEN
                text_similarity := text_similarity + 0.15;
                reasoning_data := reasoning_data || jsonb_build_object('unit_compatible', true);
            END IF;

            -- Category match
            IF series_metadata->>'category' = curve_record.category THEN
                text_similarity := text_similarity + 0.1;
                reasoning_data := reasoning_data || jsonb_build_object('category_match', true);
            END IF;

            -- Pattern-based similarity (simplified)
            pattern_similarity := CASE
                WHEN series_metadata->>'category' = 'generation' AND curve_record.curve_family = 'supply' THEN 0.8
                WHEN series_metadata->>'category' = 'demand' AND curve_record.curve_family = 'demand' THEN 0.8
                WHEN series_metadata->>'category' = 'price' AND curve_record.curve_family = 'price' THEN 0.8
                ELSE 0.5
            END;

            -- Combine similarities
            overall_score := 0.6 * text_similarity + 0.4 * pattern_similarity;

            -- Confidence score
            confidence_score := CASE
                WHEN overall_score > 0.7 THEN 0.9
                WHEN overall_score > 0.5 THEN 0.7
                WHEN overall_score > 0.3 THEN 0.5
                ELSE 0.3
            END;

            -- Add reasoning for high-confidence matches
            IF confidence_score > 0.6 THEN
                reasoning_data := reasoning_data || jsonb_build_object(
                    'method', 'heuristic_similarity',
                    'score_breakdown', jsonb_build_object(
                        'text_similarity', text_similarity,
                        'pattern_similarity', pattern_similarity
                    )
                );

                RETURN QUERY SELECT
                    curve_record.curve_key::TEXT,
                    overall_score::DECIMAL(3,2),
                    confidence_score::DECIMAL(3,2),
                    reasoning_data;
            END IF;
        END LOOP;

        -- If no high-confidence matches, return top matches by pattern similarity
        IF NOT FOUND THEN
            FOR curve_record IN
                SELECT
                    curve_key,
                    name,
                    description,
                    curve_family,
                    units
                FROM instrument
                WHERE curve_key IS NOT NULL
                ORDER BY
                    CASE
                        WHEN curve_family = series_metadata->>'category' THEN 1
                        ELSE 2
                    END,
                    curve_key
                LIMIT p_limit
            LOOP
                RETURN QUERY SELECT
                    curve_record.curve_key::TEXT,
                    0.5::DECIMAL(3,2),
                    0.3::DECIMAL(3,2),
                    jsonb_build_object(
                        'method', 'pattern_fallback',
                        'reason', 'Basic pattern matching'
                    );
            END LOOP;
        END IF;
    END;
    $$ LANGUAGE plpgsql;
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP FUNCTION IF EXISTS find_potential_series_mappings(TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER);",
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
