"""Ingest watermark window claims + helper functions.

Revision ID: 20250922_07
Revises: 20240921_05
Create Date: 2025-09-22
"""
from __future__ import annotations

from alembic import op

revision = "20250922_07"
down_revision = "20240921_05"
branch_labels = None
depends_on = None


SCHEMA_UPGRADE_STATEMENTS = [
    # Claim table
    """
    CREATE TABLE IF NOT EXISTS ingest_watermark_claim (
        source_name CITEXT NOT NULL,
        watermark_key TEXT NOT NULL,
        window_start TIMESTAMPTZ NOT NULL,
        window_end TIMESTAMPTZ NOT NULL,
        job_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'CLAIMED',
        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (source_name, watermark_key, window_start)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_ingest_watermark_claim_status ON ingest_watermark_claim(status)",
    # Claim function
    """
    CREATE OR REPLACE FUNCTION public.claim_ingest_window(
        p_source_name CITEXT,
        p_watermark_key TEXT,
        p_window_seconds INT,
        p_max_ahead_seconds INT,
        p_job_id TEXT
    ) RETURNS TABLE(window_start TIMESTAMPTZ, window_end TIMESTAMPTZ) AS $$
    DECLARE
        last_wm TIMESTAMPTZ;
        latest_end TIMESTAMPTZ;
        proposed_start TIMESTAMPTZ;
        proposed_end TIMESTAMPTZ;
        safety_lag INTERVAL := INTERVAL '60 seconds';
        lock_key BIGINT := hashtextextended(p_source_name || ':' || p_watermark_key, 0);
    BEGIN
        -- Serialize per (source,key)
        PERFORM pg_advisory_xact_lock(lock_key);

        SELECT public.get_ingest_watermark(p_source_name, p_watermark_key) INTO last_wm;
        IF last_wm IS NULL THEN
            last_wm := (NOW() AT TIME ZONE 'UTC') - make_interval(secs => p_max_ahead_seconds);
        END IF;

        SELECT COALESCE(MAX(window_end), last_wm)
          INTO latest_end
          FROM ingest_watermark_claim
         WHERE source_name = p_source_name
           AND watermark_key = p_watermark_key
           AND status IN ('CLAIMED','COMMITTED');

        proposed_start := GREATEST(last_wm, latest_end);
        proposed_end := proposed_start + make_interval(secs => p_window_seconds);
        IF proposed_end > (NOW() AT TIME ZONE 'UTC') - safety_lag THEN
            proposed_end := (NOW() AT TIME ZONE 'UTC') - safety_lag;
        END IF;
        IF proposed_end <= proposed_start THEN
            RETURN; -- empty
        END IF;

        -- Try to insert claim
        INSERT INTO ingest_watermark_claim(source_name, watermark_key, window_start, window_end, job_id, status)
        VALUES (p_source_name, p_watermark_key, proposed_start, proposed_end, p_job_id, 'CLAIMED')
        ON CONFLICT DO NOTHING;

        RETURN QUERY
          SELECT window_start, window_end
            FROM ingest_watermark_claim
           WHERE source_name = p_source_name
             AND watermark_key = p_watermark_key
             AND window_start = proposed_start
             AND job_id = p_job_id
             AND status = 'CLAIMED';
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Commit function
    """
    CREATE OR REPLACE FUNCTION public.commit_ingest_window(
        p_source_name CITEXT,
        p_watermark_key TEXT,
        p_window_start TIMESTAMPTZ,
        p_window_end TIMESTAMPTZ,
        p_job_id TEXT
    ) RETURNS BOOLEAN AS $$
    DECLARE
        updated INT;
    BEGIN
        UPDATE ingest_watermark_claim
           SET status = 'COMMITTED', updated_at = NOW()
         WHERE source_name = p_source_name
           AND watermark_key = p_watermark_key
           AND window_start = p_window_start
           AND job_id = p_job_id
           AND status = 'CLAIMED';
        GET DIAGNOSTICS updated = ROW_COUNT;

        IF updated > 0 THEN
            PERFORM public.update_ingest_watermark(p_source_name, p_watermark_key, p_window_end, 'exact');
            RETURN TRUE;
        END IF;
        RETURN FALSE;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Gap scan function
    """
    CREATE OR REPLACE FUNCTION public.gap_scan_ingest_windows(
        p_source_name CITEXT,
        p_watermark_key TEXT,
        p_backfill_hours INT,
        p_window_seconds INT,
        p_limit INT DEFAULT 100
    ) RETURNS TABLE(window_start TIMESTAMPTZ, window_end TIMESTAMPTZ) AS $$
    DECLARE
        start_ts TIMESTAMPTZ := (NOW() AT TIME ZONE 'UTC') - make_interval(hours => p_backfill_hours);
        end_ts   TIMESTAMPTZ := NOW() AT TIME ZONE 'UTC';
    BEGIN
        RETURN QUERY
        WITH series AS (
            SELECT gs AS window_start,
                   gs + make_interval(secs => p_window_seconds) AS window_end
              FROM generate_series(start_ts, end_ts - make_interval(secs => p_window_seconds), make_interval(secs => p_window_seconds)) gs
        )
        SELECT s.window_start, s.window_end
          FROM series s
          LEFT JOIN ingest_watermark_claim c
            ON c.source_name = p_source_name
           AND c.watermark_key = p_watermark_key
           AND c.window_start = s.window_start
           AND c.status = 'COMMITTED'
         WHERE c.window_start IS NULL
         ORDER BY s.window_start
         LIMIT p_limit;
    END;
    $$ LANGUAGE plpgsql;
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP FUNCTION IF EXISTS public.gap_scan_ingest_windows(CITEXT, TEXT, INT, INT, INT)",
    "DROP FUNCTION IF EXISTS public.commit_ingest_window(CITEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TEXT)",
    "DROP FUNCTION IF EXISTS public.claim_ingest_window(CITEXT, TEXT, INT, INT, TEXT)",
    "DROP TABLE IF EXISTS ingest_watermark_claim",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)

