"""Helpers for interacting with ingest watermark tables.

This module attempts to discover the correct Postgres DSN from environment
variables commonly present in the Aurum dev stack. You can always override the
connection by passing ``dsn=...`` or setting ``AURUM_APP_DB_DSN``.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional, Tuple, List

import psycopg


def _env_default_dsn() -> str:
    """Build a sensible default DSN from standard env vars.

    Falls back to the Aurum dev stack defaults if not provided.
    """
    user = os.getenv("POSTGRES_USER", "aurum")
    password = os.getenv("POSTGRES_PASSWORD", "aurum")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "aurum")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@contextmanager
def _get_connection(dsn: Optional[str] = None) -> Generator[psycopg.Connection, None, None]:
    # Choose DSN in order of precedence: explicit arg, env var, derived default
    dsn = dsn or os.getenv("AURUM_APP_DB_DSN") or _env_default_dsn()
    conn = psycopg.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


def register_ingest_source(
    name: str,
    *,
    description: Optional[str] = None,
    schedule: Optional[str] = None,
    target: Optional[str] = None,
    dsn: Optional[str] = None,
) -> None:
    """Insert or update an ingest source definition."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT public.register_ingest_source(%s, %s, %s, %s)",
            (name, description, schedule, target),
        )
        conn.commit()


def update_ingest_watermark(
    source_name: str,
    watermark_key: str,
    watermark: datetime,
    *,
    policy: str = "exact",
    dsn: Optional[str] = None,
) -> None:
    """Upsert the watermark for the given source with policy-based rounding."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT public.update_ingest_watermark(%s, %s, %s, %s)",
            (source_name, watermark_key, watermark, policy),
        )
        conn.commit()


def get_ingest_watermark(
    source_name: str,
    watermark_key: str = "default",
    *,
    dsn: Optional[str] = None,
) -> Optional[datetime]:
    """Return the stored watermark for the source if one exists."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT public.get_ingest_watermark(%s, %s)",
            (source_name, watermark_key),
        )
        row = cur.fetchone()
        return row[0] if row else None


def claim_window(
    source_name: str,
    watermark_key: str,
    *,
    window_seconds: int,
    max_ahead_seconds: int = 86400,
    job_id: str,
    dsn: Optional[str] = None,
) -> Optional[Tuple[datetime, datetime]]:
    """Claim the next ingest window for (source, key) and return (start, end) if acquired."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM public.claim_ingest_window(%s, %s, %s, %s, %s)",
            (source_name, watermark_key, window_seconds, max_ahead_seconds, job_id),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1]
        return None


def commit_window(
    source_name: str,
    watermark_key: str,
    window_start: datetime,
    window_end: datetime,
    *,
    job_id: str,
    dsn: Optional[str] = None,
) -> bool:
    """Commit a previously claimed window and advance the watermark."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT public.commit_ingest_window(%s, %s, %s, %s, %s)",
            (source_name, watermark_key, window_start, window_end, job_id),
        )
        row = cur.fetchone()
        committed = bool(row and row[0])
        conn.commit()
        return committed


def gap_scan(
    source_name: str,
    watermark_key: str,
    *,
    backfill_hours: int,
    window_seconds: int,
    limit: int = 100,
    dsn: Optional[str] = None,
) -> List[Tuple[datetime, datetime]]:
    """Return a list of missing committed windows to backfill."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM public.gap_scan_ingest_windows(%s, %s, %s, %s, %s)",
            (source_name, watermark_key, backfill_hours, window_seconds, limit),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


__all__ = [
    "register_ingest_source",
    "update_ingest_watermark",
    "get_ingest_watermark",
    "claim_window",
    "commit_window",
    "gap_scan",
]
