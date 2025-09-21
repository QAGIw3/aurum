#!/usr/bin/env python3
"""Backfill helper for EIA series ingestion jobs.

Supports both API (per-day windows) and bulk archive jobs by replaying SeaTunnel
runs and updating ingest watermarks automatically.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_API = REPO_ROOT / "config" / "eia_ingest_datasets.json"
CONFIG_BULK = REPO_ROOT / "config" / "eia_bulk_datasets.json"
RUN_JOB = REPO_ROOT / "scripts" / "seatunnel" / "run_job.sh"


def _date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    current = start
    step = dt.timedelta(days=1)
    while current <= end:
        yield current
        current += step


def _load_config(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("datasets", [])
    if not isinstance(entries, list):
        raise RuntimeError(f"Expected 'datasets' array in {path}")
    return entries


def _build_env_for_api(cfg: dict[str, Any]) -> Dict[str, str]:
    env: Dict[str, str] = {}
    env["EIA_SERIES_PATH"] = cfg["path"]
    env["EIA_SERIES_ID"] = cfg.get("series_id", "MULTI_SERIES")
    env["EIA_SERIES_ID_EXPR"] = cfg.get("series_id_expr", "'MULTI_SERIES'")
    env["EIA_FREQUENCY"] = cfg.get("frequency", "OTHER")
    env["EIA_TOPIC"] = cfg.get("default_topic", "aurum.ref.eia.series.v1")
    env["EIA_UNITS"] = cfg.get("default_units", "unknown")
    env["EIA_SEASONAL_ADJUSTMENT"] = cfg.get("seasonal_adjustment", "UNKNOWN")
    env["EIA_AREA_EXPR"] = cfg.get("area_expr", "CAST(NULL AS STRING)")
    env["EIA_SECTOR_EXPR"] = cfg.get("sector_expr", "CAST(NULL AS STRING)")
    env["EIA_DESCRIPTION_EXPR"] = cfg.get("description_expr", "CAST(NULL AS STRING)")
    env["EIA_SOURCE_EXPR"] = cfg.get("source_expr", "COALESCE(source, 'EIA')")
    env["EIA_DATASET_EXPR"] = cfg.get("dataset_expr", "COALESCE(dataset, '')")
    env["EIA_METADATA_EXPR"] = cfg.get("metadata_expr", "NULL")
    env["EIA_FILTER_EXPR"] = cfg.get("filter_expr", "TRUE")
    overrides = cfg.get("param_overrides") or []
    env["EIA_PARAM_OVERRIDES_JSON"] = json.dumps(overrides)
    return env


def _build_env_for_bulk(cfg: dict[str, Any]) -> Dict[str, str]:
    env: Dict[str, str] = {}
    env["EIA_BULK_URL"] = cfg["url"]
    env["EIA_BULK_TOPIC"] = cfg.get("default_topic", "aurum.ref.eia.bulk.unknown.v1")
    env["EIA_BULK_FREQUENCY"] = cfg.get("frequency", "MONTHLY")
    for key, env_name in [
        ("series_id_expr", "EIA_BULK_SERIES_ID_EXPR"),
        ("period_expr", "EIA_BULK_PERIOD_EXPR"),
        ("value_expr", "EIA_BULK_VALUE_EXPR"),
        ("raw_value_expr", "EIA_BULK_RAW_VALUE_EXPR"),
        ("units_expr", "EIA_BULK_UNITS_EXPR"),
        ("area_expr", "EIA_BULK_AREA_EXPR"),
        ("sector_expr", "EIA_BULK_SECTOR_EXPR"),
        ("description_expr", "EIA_BULK_DESCRIPTION_EXPR"),
        ("source_expr", "EIA_BULK_SOURCE_EXPR"),
        ("dataset_expr", "EIA_BULK_DATASET_EXPR"),
        ("metadata_expr", "EIA_BULK_METADATA_EXPR"),
        ("filter_expr", "EIA_BULK_FILTER_EXPR"),
    ]:
        if cfg.get(key) is not None:
            env[env_name] = str(cfg[key])
    if cfg.get("csv_delimiter") is not None:
        env["EIA_BULK_CSV_DELIMITER"] = str(cfg["csv_delimiter"])
    if cfg.get("skip_header") is not None:
        env["EIA_BULK_SKIP_HEADER"] = str(cfg["skip_header"])
    schema_fields = cfg.get("schema_fields")
    if schema_fields:
        env["EIA_BULK_SCHEMA_FIELDS_JSON"] = json.dumps(schema_fields)
    extra_env = cfg.get("extra_env") or {}
    for key, value in extra_env.items():
        env[str(key)] = str(value)
    return env


def _update_watermark(source: str, watermark_key: str, timestamp: dt.datetime, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] update watermark {source}/{watermark_key} -> {timestamp.isoformat()}")
        return
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from aurum.db import update_ingest_watermark  # type: ignore

    update_ingest_watermark(source, watermark_key, timestamp)


def _ensure_env(keys: Iterable[str]) -> None:
    missing = [key for key in keys if not os.environ.get(key)]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Missing required environment variables: {joined}")


def _run_job(job: str, env: Dict[str, str], *, dry_run: bool) -> None:
    merged_env = os.environ.copy()
    merged_env.update(env)
    cmd = ["bash", str(RUN_JOB), job]
    if dry_run:
        printable = " ".join([f"{key}={value}" for key, value in env.items()])
        print(f"[dry-run] {printable} {' '.join(cmd)}")
        return
    subprocess.run(cmd, cwd=str(REPO_ROOT), env=merged_env, check=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, help='Dataset source_name defined in the config file')
    parser.add_argument('--job', choices=['api', 'bulk'], default='api', help='SeaTunnel job type to execute')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD) for API backfills')
    parser.add_argument('--end', help='End date inclusive (YYYY-MM-DD) for API backfills (default start date)')
    parser.add_argument('--watermark-key', default='logical_date', help='Watermark key (default: logical_date)')
    parser.add_argument('--watermark-time', default='23:59:59', help='HH:MM:SS component for watermarks (default: 23:59:59)')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without executing')
    parser.add_argument('--extra-env', action='append', default=[], help='Additional KEY=VALUE environment assignments')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    extra_env = {}
    for item in args.extra_env:
        if '=' not in item:
            raise RuntimeError(f"Invalid extra env assignment: {item}")
        key, value = item.split('=', 1)
        extra_env[key.strip()] = value

    if args.job == 'api':
        datasets = _load_config(CONFIG_API)
    else:
        datasets = _load_config(CONFIG_BULK)

    selected = next((entry for entry in datasets if entry.get('source_name') == args.source), None)
    if not selected:
        raise RuntimeError(f"Source {args.source!r} not found in config")

    required_env = ["KAFKA_BOOTSTRAP_SERVERS", "SCHEMA_REGISTRY_URL"]
    if args.job == 'api':
        required_env.append("EIA_API_KEY")
    _ensure_env(required_env)

    watermark_time = args.watermark_time
    try:
        wt_hour, wt_minute, wt_second = [int(part) for part in watermark_time.split(':', 2)]
    except ValueError as exc:
        raise RuntimeError(f"Invalid watermark time: {watermark_time}") from exc

    if args.job == 'api':
        if not args.start:
            raise RuntimeError("--start is required for API backfills")
        start_date = dt.date.fromisoformat(args.start)
        end_date = dt.date.fromisoformat(args.end) if args.end else start_date
        if end_date < start_date:
            raise RuntimeError("--end must be on or after --start")
        base_env = _build_env_for_api(selected)
        base_env.update(extra_env)
        base_env.setdefault("EIA_API_KEY", os.environ.get("EIA_API_KEY", ""))
        base_env.setdefault("KAFKA_BOOTSTRAP_SERVERS", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""))
        base_env.setdefault("SCHEMA_REGISTRY_URL", os.environ.get("SCHEMA_REGISTRY_URL", ""))

        for current in _date_range(start_date, end_date):
            window_start = current.isoformat()
            window_end = current.isoformat()
            env = dict(base_env)
            env["EIA_START"] = window_start
            env["EIA_END"] = window_end
            _run_job('eia_series_to_kafka', env, dry_run=args.dry_run)
            watermark = dt.datetime(current.year, current.month, current.day, wt_hour, wt_minute, wt_second, tzinfo=dt.timezone.utc)
            _update_watermark(args.source, args.watermark_key, watermark, args.dry_run)
    else:
        env = _build_env_for_bulk(selected)
        env.update(extra_env)
        env.setdefault("KAFKA_BOOTSTRAP_SERVERS", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""))
        env.setdefault("SCHEMA_REGISTRY_URL", os.environ.get("SCHEMA_REGISTRY_URL", ""))
        _run_job('eia_bulk_to_kafka', env, dry_run=args.dry_run)
        watermark = dt.datetime.now(dt.timezone.utc)
        _update_watermark(args.source, args.watermark_key, watermark, args.dry_run)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
