#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List

from trino.dbapi import connect


def find_sql_files(paths: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for p in paths:
        if p.is_file() and p.suffix.lower() == ".sql":
            files.append(p)
        elif p.is_dir():
            files.extend(sorted(p.rglob("*.sql")))
    # Sort by path for determinism
    return sorted(files, key=lambda fp: str(fp))


def split_statements(sql_text: str) -> List[str]:
    # Very simple splitter: split on semicolons; ignore empty/whitespace
    # Assumes DDL files don't contain semicolons inside strings.
    parts = [s.strip() for s in sql_text.split(";")]
    return [p for p in parts if p]


def apply_sql_file(cur, sql_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_statements(sql_text)
    for stmt in statements:
        cur.execute(stmt)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply Trino DDL files in order")
    parser.add_argument(
        "--server",
        required=True,
        help="Trino server base URL, e.g. http://localhost:8080",
    )
    parser.add_argument("--user", default="aurum", help="Trino user")
    parser.add_argument("--catalog", default="iceberg", help="Default catalog")
    parser.add_argument("--schema", default="market", help="Default schema")
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[
            Path("trino/ddl"),
            Path("infra/trino/ddl"),
        ],
        help="Directories or files to apply (default: trino/ddl and infra/trino/ddl)",
    )

    args = parser.parse_args()

    # Parse server host/port
    server = args.server
    if server.startswith("http://"):
        server = server[len("http://") :]
    elif server.startswith("https://"):
        server = server[len("https://") :]
    if ":" in server:
        host, port_str = server.split(":", 1)
        port = int(port_str)
    else:
        host, port = server, 8080

    files = find_sql_files([Path(p) for p in args.paths])
    if not files:
        print("No .sql files found to apply", file=sys.stderr)
        return 1

    print(f"Applying {len(files)} SQL file(s) to Trino {host}:{port} as {args.user} ...")

    with connect(host=host, port=port, user=args.user, catalog=args.catalog, schema=args.schema) as conn:
        with conn.cursor() as cur:
            for sql_path in files:
                print(f"-> {sql_path}")
                apply_sql_file(cur, sql_path)

    print("✅ Trino DDL application completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


