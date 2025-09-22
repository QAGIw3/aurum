#!/usr/bin/env python3
"""Benchmark harness for scenario output retrieval."""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from typing import List

from aurum.api import service
from aurum.api.config import CacheConfig, TrinoConfig
from aurum.core import AurumSettings


def _build_configs(use_cache: bool) -> tuple[TrinoConfig, CacheConfig]:
    settings = AurumSettings.from_env()
    trino_cfg = TrinoConfig.from_settings(settings)
    cache_cfg = CacheConfig.from_settings(settings) if use_cache else CacheConfig(ttl_seconds=0)
    return trino_cfg, cache_cfg


def _summarise(durations: List[float]) -> str:
    if not durations:
        return "no samples"
    mean = statistics.fmean(durations)
    p50 = statistics.median(durations)
    p95 = statistics.quantiles(durations, n=100)[94] if len(durations) >= 100 else max(durations)
    minimum = min(durations)
    maximum = max(durations)
    return (
        f"min={minimum:.2f}ms p50={p50:.2f}ms mean={mean:.2f}ms "
        f"p95={p95:.2f}ms max={maximum:.2f}ms"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark scenario output queries")
    parser.add_argument("--tenant-id", required=True, help="Tenant identifier")
    parser.add_argument("--scenario-id", required=True, help="Scenario identifier")
    parser.add_argument("--metric", default=None, help="Optional metric name filter")
    parser.add_argument("--limit", type=int, default=200, help="Limit rows per query")
    parser.add_argument("--offset", type=int, default=0, help="Offset for pagination benchmark")
    parser.add_argument("--iterations", type=int, default=5, help="Number of runs to execute")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between iterations (seconds)")
    parser.add_argument("--use-cache", action="store_true", help="Exercise Redis cache if configured")
    parser.add_argument("--descending", action="store_true", help="Query outputs in descending order")
    args = parser.parse_args()

    trino_cfg, cache_cfg = _build_configs(args.use_cache)

    durations: List[float] = []
    total_rows = 0

    for _ in range(args.iterations):
        query_start = time.perf_counter()
        rows, elapsed_ms = service.query_scenario_outputs(
            trino_cfg,
            cache_cfg,
            tenant_id=args.tenant_id,
            scenario_id=args.scenario_id,
            curve_key=None,
            tenor_type=None,
            metric=args.metric,
            limit=args.limit,
            offset=args.offset,
            cursor_after=None,
            cursor_before=None,
            descending=args.descending,
        )
        query_duration = (time.perf_counter() - query_start) * 1000.0
        durations.append(elapsed_ms or query_duration)
        total_rows += len(rows)
        if args.sleep:
            time.sleep(args.sleep)

    summary = _summarise(durations)
    print("Iterations:", args.iterations)
    print("Total rows fetched:", total_rows)
    print("Latency stats:", summary)
    if durations:
        total_duration_seconds = sum(durations) / 1000.0
        throughput = total_rows / total_duration_seconds if total_duration_seconds else 0.0
        print(f"Approx throughput: {throughput:.2f} rows/sec (data-plane latency only)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
