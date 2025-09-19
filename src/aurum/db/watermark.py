"""Helpers for interacting with ingest watermark tables."""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional

import psycopg

DEFAULT_DSN = "postgresql://app:app@postgres:5432/app"


@contextmanager
def _get_connection(dsn: Optional[str] = None) -> Generator[psycopg.Connection, None, None]:
    dsn = dsn or os.getenv("AURUM_APP_DB_DSN", DEFAULT_DSN)
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
    dsn: Optional[str] = None,
) -> None:
    """Upsert the watermark for the given source."""
    with _get_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT public.update_ingest_watermark(%s, %s, %s)",
            (source_name, watermark_key, watermark),
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
