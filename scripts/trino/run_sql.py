#!/usr/bin/env python
"""Execute SQL files against a Trino server via HTTP REST API.

Minimal dependency runner to apply DDL (e.g., create views) in dev.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Iterator

from aurum.compat import requests
from urllib.parse import urlsplit


def iter_statements(sql_text: str) -> Iterator[str]:
    buf: list[str] = []
    in_string = False
    escape = False
    for ch in sql_text:
        if ch == "'" and not escape:
            in_string = not in_string
        if ch == ";" and not in_string:
            stmt = "".join(buf).strip()
            if stmt:
                yield stmt
            buf = []
            continue
        escape = (ch == "\\") and not escape
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        yield tail


def _rewrite_next_uri(next_uri: str, base_server: str) -> str:
    parts = urlsplit(next_uri)
    path = parts.path or "/"
    query = f"?{parts.query}" if parts.query else ""
    return f"{base_server.rstrip('/')}{path}{query}"


def post_statement(
    server: str,
    user: str,
    catalog: str | None,
    schema: str | None,
    sql: str,
    *,
    host_header: str | None = None,
) -> None:
    url = f"{server.rstrip('/')}/v1/statement"
    session = requests.Session()
    headers = {
        "X-Trino-User": user,
    }
    if host_header:
        headers["Host"] = host_header
    if catalog:
        headers["X-Trino-Catalog"] = catalog
    if schema:
        headers["X-Trino-Schema"] = schema

    resp = session.post(url, data=sql.encode("utf-8"), headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    next_uri = payload.get("nextUri")
    while next_uri:
        # Always route through the same server and keep Host header for Traefik
        rewritten = _rewrite_next_uri(next_uri, server)
        r = session.get(rewritten, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        next_uri = data.get("nextUri")
        if data.get("stats", {}).get("state") == "FINISHED":
            break
        if data.get("error"):
            raise RuntimeError(str(data["error"]))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run SQL files on Trino via REST API")
    p.add_argument("--server", default="http://localhost:8080", help="Trino coordinator URL")
    p.add_argument("--user", default="aurum", help="Trino user header value")
    p.add_argument("--catalog", help="Default catalog for the session")
    p.add_argument("--schema", help="Default schema for the session")
    p.add_argument("--host-header", dest="host_header", help="Optional Host header to send (for ingress)")
    p.add_argument("sql_file", type=Path, help="Path to .sql file to execute")
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    sql = args.sql_file.read_text(encoding="utf-8")
    for stmt in iter_statements(sql):
        post_statement(
            args.server,
            args.user,
            args.catalog,
            args.schema,
            stmt,
            host_header=args.host_header,
        )
    print(f"Applied SQL from {args.sql_file}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
