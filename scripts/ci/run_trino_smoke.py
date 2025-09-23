#!/usr/bin/env python3
"""Run the Trino query harness and fail when latency regressions are detected."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple

from scripts.trino import query_harness


def _load_plan_with_thresholds(plan_path: Path) -> Tuple[list[tuple[str, str]], Dict[str, float], int, float | None]:
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    queries: list[tuple[str, str]] = []
    thresholds: Dict[str, float] = {}

    for entry in payload.get("queries", []):
        name = entry.get("name")
        sql = entry.get("sql")
        if not name or not sql:
            raise ValueError(f"Invalid query entry: {entry}")
        queries.append((str(name), str(sql)))
        limit = entry.get("p95_limit_ms")
        if limit is not None:
            thresholds[str(name)] = float(limit)

    iterations = int(payload.get("iterations", 3))
    default_limit = payload.get("default_p95_limit_ms")
    return queries, thresholds, iterations, float(default_limit) if default_limit is not None else None


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Trino smoke tests with latency thresholds")
    parser.add_argument("--server", default="http://localhost:8080", help="Trino coordinator URL")
    parser.add_argument("--user", default="aurum", help="Trino user header")
    parser.add_argument("--catalog", default="iceberg", help="Trino catalog")
    parser.add_argument("--schema", default="mart", help="Trino schema")
    parser.add_argument("--host-header", dest="host_header", help="Optional Host header override")
    parser.add_argument("--plan", type=Path, default=Path("config/trino_query_harness.json"), help="Harness plan JSON")
    parser.add_argument("--output", type=Path, help="Optional path to write run summary")
    return parser.parse_args(list(argv) if argv is not None else None)


def _evaluate_thresholds(
    summary: Dict[str, Dict[str, Dict[str, float]]],
    thresholds: Dict[str, float],
    default_limit: float | None,
) -> list[tuple[str, float, float]]:
    failures: list[tuple[str, float, float]] = []
    for query_name, metrics in summary.items():
        limit = thresholds.get(query_name, default_limit)
        if limit is None:
            continue
        p95 = metrics.get("latency_ms", {}).get("p95")
        if p95 is None or math.isnan(p95):
            continue
        if p95 > limit:
            failures.append((query_name, p95, limit))
    return failures


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)

    queries, thresholds, iterations, default_limit = _load_plan_with_thresholds(args.plan)

    summary = query_harness.run_harness(
        args.server,
        args.user,
        args.catalog,
        args.schema,
        args.host_header,
        queries,
        iterations,
    )

    if args.output:
        args.output.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    failures = _evaluate_thresholds(summary, thresholds, default_limit)
    if failures:
        print("Trino smoke test exceeded latency thresholds:", file=sys.stderr)
        for name, observed, limit in failures:
            print(f"  - {name}: p95={observed:.2f}ms (limit {limit:.2f}ms)", file=sys.stderr)
        return 1

    print("Trino smoke test passed. p95 latencies:")
    for name, metrics in summary.items():
        p95 = metrics.get("latency_ms", {}).get("p95")
        if p95 is None:
            continue
        print(f"  - {name}: {p95:.2f}ms")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
