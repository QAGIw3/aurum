#!/usr/bin/env python
"""Execute a suite of Trino queries and capture latency/cost metrics."""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Iterable, Iterator

from aurum.compat import requests


def _parse_inline_query(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise ValueError(f"Query spec must be NAME=SQL, got: {spec!r}")
    name, sql = spec.split("=", 1)
    sql = sql.strip()
    if not sql:
        raise ValueError(f"Query SQL may not be empty for {name!r}")
    return name.strip(), sql


def _load_plan(path: Path) -> tuple[list[tuple[str, str]], int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    queries = payload.get("queries", [])
    iterations = int(payload.get("iterations", 3))
    parsed: list[tuple[str, str]] = []
    for entry in queries:
        name = entry.get("name")
        sql = entry.get("sql")
        if not name or not sql:
            raise ValueError(f"Invalid query entry in {path}: {entry}")
        parsed.append((str(name), str(sql)))
    return parsed, iterations


def _build_headers(user: str, catalog: str | None, schema: str | None, host_header: str | None) -> dict[str, str]:
    headers = {"X-Trino-User": user}
    if catalog:
        headers["X-Trino-Catalog"] = catalog
    if schema:
        headers["X-Trino-Schema"] = schema
    if host_header:
        headers["Host"] = host_header
    return headers


def _follow_query(server: str, session: requests.Session, headers: dict[str, str], sql: str) -> dict:
    url = f"{server.rstrip('/')}/v1/statement"
    resp = session.post(url, data=sql.encode("utf-8"), headers=headers, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    stats = payload.get("stats", {})
    next_uri = payload.get("nextUri")

    while next_uri:
        resp = session.get(next_uri, headers=headers, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("stats"):
            stats = payload["stats"]
        next_uri = payload.get("nextUri")
    return stats


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    k = (len(ordered) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    d0 = ordered[f] * (c - k)
    d1 = ordered[c] * (k - f)
    return d0 + d1


def summarize(metric_values: list[float]) -> dict[str, float]:
    clean = [v for v in metric_values if math.isfinite(v)]
    if not clean:
        return {"min": float("nan"), "median": float("nan"), "p95": float("nan")}
    return {
        "min": min(clean),
        "median": statistics.median(clean),
        "p95": percentile(clean, 0.95),
    }


def run_harness(
    server: str,
    user: str,
    catalog: str | None,
    schema: str | None,
    host_header: str | None,
    queries: Iterable[tuple[str, str]],
    iterations: int,
) -> dict[str, dict[str, dict[str, float]]]:
    session = requests.Session()
    headers = _build_headers(user, catalog, schema, host_header)

    results: dict[str, dict[str, dict[str, float]]] = {}
    for name, sql in queries:
        latencies: list[float] = []
        cpu_times: list[float] = []
        bytes_processed: list[float] = []
        rows_processed: list[float] = []
        peak_memory: list[float] = []

        for _ in range(iterations):
            stats = _follow_query(server, session, headers, sql)
            latencies.append(float(stats.get("wallTimeMillis", 0.0)))
            cpu_times.append(float(stats.get("cpuTimeMillis", 0.0)))
            bytes_processed.append(float(stats.get("processedBytes", 0.0)))
            rows_processed.append(float(stats.get("processedRows", 0.0)))
            peak_memory.append(float(stats.get("peakMemoryBytes", 0.0)))

        results[name] = {
            "latency_ms": summarize(latencies),
            "cpu_ms": summarize(cpu_times),
            "bytes_processed": summarize(bytes_processed),
            "rows_processed": summarize(rows_processed),
            "peak_memory_bytes": summarize(peak_memory),
        }
    return results


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Trino query harness and report p95 cost metrics")
    parser.add_argument("--server", default="http://localhost:8080", help="Trino coordinator URL")
    parser.add_argument("--user", default="aurum", help="Trino user header value")
    parser.add_argument("--catalog", help="Default catalog for the session")
    parser.add_argument("--schema", help="Default schema for the session")
    parser.add_argument("--host-header", dest="host_header", help="Optional Host header override")
    parser.add_argument("--iterations", type=int, default=3, help="Number of executions per query")
    parser.add_argument("--plan", type=Path, help="JSON file describing queries to execute")
    parser.add_argument(
        "--query",
        action="append",
        help="Inline query definition NAME=SQL (can be provided multiple times)",
    )
    parser.add_argument("--output", type=Path, help="Optional path to write JSON summary")
    return parser.parse_args(list(argv) if argv is not None else None)


def load_queries(args: argparse.Namespace) -> tuple[list[tuple[str, str]], int]:
    queries: list[tuple[str, str]] = []
    iterations = args.iterations
    if args.plan:
        plan_queries, plan_iterations = _load_plan(args.plan)
        queries.extend(plan_queries)
        iterations = plan_iterations or iterations
    if args.query:
        for spec in args.query:
            queries.append(_parse_inline_query(spec))
    if not queries:
        raise ValueError("At least one query must be provided via --plan or --query")
    return queries, max(iterations, 1)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        queries, iterations = load_queries(args)
        summary = run_harness(
            args.server,
            args.user,
            args.catalog,
            args.schema,
            args.host_header,
            queries,
            iterations,
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Harness failed: {exc}", file=sys.stderr)
        return 1

    output = json.dumps(summary, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
        print(f"Wrote harness metrics to {args.output}")
    else:
        print(output)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
