#!/usr/bin/env python
"""Apply a SQL file to a Postgres/Timescale database.

Usage:
  python scripts/sql/apply_file.py --dsn postgresql://user:pass@host:5432/db path/to/file.sql
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Iterator

import psycopg


def iter_pg_statements(sql_text: str) -> Iterator[str]:
    buf: list[str] = []
    in_single = False
    in_multi_comment = False
    in_line_comment = False
    dollar_tag: str | None = None
    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < length else ""

        # End of line comment
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # End of multi-line comment
        if in_multi_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_multi_comment = False
                continue
            i += 1
            continue

        # Start of line comment
        if not in_single and dollar_tag is None and ch == "-" and nxt == "-":
            buf.append(ch)
            buf.append(nxt)
            i += 2
            in_line_comment = True
            continue

        # Start of multi-line comment
        if not in_single and dollar_tag is None and ch == "/" and nxt == "*":
            buf.append(ch)
            buf.append(nxt)
            i += 2
            in_multi_comment = True
            continue

        # Handle start/end single-quoted strings ('' escapes)
        if dollar_tag is None and ch == "'":
            buf.append(ch)
            i += 1
            in_single = not in_single
            # handle escaped quote ''
            if in_single and i < length and sql_text[i] == "'":
                # it's an escaped single quote
                buf.append("'")
                i += 1
                in_single = True
            continue

        # Handle dollar-quoted strings: $tag$ ... $tag$
        if not in_single and ch == "$":
            # Try to parse a dollar tag
            j = i + 1
            while j < length and (sql_text[j].isalnum() or sql_text[j] == "_"):
                j += 1
            if j < length and sql_text[j] == "$":
                tag = sql_text[i:j + 1]  # includes both $ ... $
                if dollar_tag is None:
                    dollar_tag = tag
                elif tag == dollar_tag:
                    dollar_tag = None
                buf.append(sql_text[i:j + 1])
                i = j + 1
                continue
        
        # Statement terminator
        if not in_single and dollar_tag is None and ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                yield stmt
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        yield tail


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apply a SQL file to a Postgres-compatible database")
    p.add_argument("sql_file", type=Path, help="Path to .sql file")
    p.add_argument("--dsn", required=True, help="Database DSN, e.g. postgresql://user:pass@host:5432/db")
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    sql = args.sql_file.read_text(encoding="utf-8")
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            for stmt in iter_pg_statements(sql):
                cur.execute(stmt)
        conn.commit()
    print(f"Applied {args.sql_file} -> {args.dsn}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


