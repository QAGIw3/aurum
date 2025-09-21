#!/usr/bin/env python
"""Report quarantine row counts from Iceberg via Trino."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from trino.dbapi import connect


@dataclass
class QuarantineSample:
    asof_date: date
    rows: int


def query_quarantine_counts(
    *,
    host: str,
    port: int,
    user: str,
    catalog: str,
    schema: str,
    days: int,
) -> list[QuarantineSample]:
    start_date = date.today() - timedelta(days=days)
    sql = f"""
        SELECT
            asof_date,
            count(*) AS rows
        FROM {catalog}.{schema}.curve_observation_quarantine
        WHERE asof_date >= DATE '{start_date.isoformat()}'
        GROUP BY asof_date
        ORDER BY asof_date DESC
    """
    conn = connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()
    return [QuarantineSample(asof_date=row[0], rows=int(row[1])) for row in rows]


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Monitor curve quarantine counts")
    parser.add_argument("--host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    parser.add_argument("--user", default=os.getenv("TRINO_USER", "aurum-monitor"))
    parser.add_argument("--catalog", default=os.getenv("TRINO_CATALOG", "iceberg"))
    parser.add_argument("--schema", default=os.getenv("TRINO_SCHEMA", "market"))
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default 7)")
    parser.add_argument(
        "--threshold",
        type=int,
        default=int(os.getenv("AURUM_QUARANTINE_THRESHOLD", "500")),
        help="Alert threshold for total quarantined rows in the window",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    samples = query_quarantine_counts(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema,
        days=args.days,
    )
    total = sum(sample.rows for sample in samples)
    print(f"Quarantine rows (last {args.days} days): {total}")
    for sample in samples:
        print(f" - {sample.asof_date}: {sample.rows}")

    if total > args.threshold:
        raise SystemExit(f"Quarantine row count {total} exceeds threshold {args.threshold}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
