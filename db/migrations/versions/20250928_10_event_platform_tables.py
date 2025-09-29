"""Event store, saga, and outbox tables for event-driven platform."""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "20250928_10_event_platform_tables"
down_revision = "20250928_09_auto_reforecast_persistence"
branch_labels = None
depends_on = None


SCHEMA_UPGRADE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS event_store_events (
        stream_id TEXT NOT NULL,
        sequence BIGINT NOT NULL,
        event_id UUID NOT NULL,
        event_type TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        metadata JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        schema_version INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (stream_id, sequence),
        UNIQUE (event_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_events_stream_seq
        ON event_store_events (stream_id, sequence DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_events_recorded_at
        ON event_store_events (recorded_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS event_store_metadata (
        event_id UUID PRIMARY KEY,
        correlation_id TEXT,
        causation_id TEXT,
        trace_id TEXT,
        attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_metadata_correlation
        ON event_store_metadata (correlation_id)
        WHERE correlation_id IS NOT NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_metadata_trace
        ON event_store_metadata (trace_id)
        WHERE trace_id IS NOT NULL;
    """,
    """
    CREATE TABLE IF NOT EXISTS event_store_snapshots (
        stream_id TEXT PRIMARY KEY,
        version BIGINT NOT NULL,
        state JSONB NOT NULL,
        metadata JSONB NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_snapshots_updated
        ON event_store_snapshots (recorded_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS event_store_sagas (
        saga_id TEXT PRIMARY KEY,
        saga_type TEXT NOT NULL,
        state JSONB NOT NULL,
        status TEXT NOT NULL,
        version BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_sagas_type_status
        ON event_store_sagas (saga_type, status);
    """,
    """
    CREATE TABLE IF NOT EXISTS event_store_outbox (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        topic TEXT NOT NULL,
        partition_key TEXT,
        payload JSONB NOT NULL,
        headers JSONB NOT NULL DEFAULT '{}'::JSONB,
        schema_subject TEXT,
        schema_version INTEGER,
        scheduled_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        published_at TIMESTAMPTZ,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        last_error TEXT
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_outbox_pending
        ON event_store_outbox (scheduled_at)
        WHERE published_at IS NULL;
    """,
    """
    CREATE TABLE IF NOT EXISTS event_store_idempotency (
        event_id TEXT PRIMARY KEY,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_store_idempotency_recorded
        ON event_store_idempotency (recorded_at DESC);
    """,
]

SCHEMA_DOWNGRADE_STATEMENTS = [
    "DROP INDEX IF EXISTS idx_event_store_idempotency_recorded;",
    "DROP TABLE IF EXISTS event_store_idempotency;",
    "DROP INDEX IF EXISTS idx_event_store_outbox_pending;",
    "DROP TABLE IF EXISTS event_store_outbox;",
    "DROP INDEX IF EXISTS idx_event_store_sagas_type_status;",
    "DROP TABLE IF EXISTS event_store_sagas;",
    "DROP INDEX IF EXISTS idx_event_store_snapshots_updated;",
    "DROP TABLE IF EXISTS event_store_snapshots;",
    "DROP INDEX IF EXISTS idx_event_store_metadata_trace;",
    "DROP INDEX IF EXISTS idx_event_store_metadata_correlation;",
    "DROP TABLE IF EXISTS event_store_metadata;",
    "DROP INDEX IF EXISTS idx_event_store_events_recorded_at;",
    "DROP INDEX IF EXISTS idx_event_store_events_stream_seq;",
    "DROP TABLE IF EXISTS event_store_events;",
]


def upgrade() -> None:
    for statement in SCHEMA_UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in SCHEMA_DOWNGRADE_STATEMENTS:
        op.execute(statement)
