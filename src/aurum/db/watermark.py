"""Helpers for interacting with ingest watermark tables.

This module attempts to discover the correct Postgres DSN from environment
variables commonly present in the Aurum dev stack. You can always override the
connection by passing ``dsn=...`` or setting ``AURUM_APP_DB_DSN``.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional

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
