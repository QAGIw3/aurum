#!/usr/bin/env python3
"""Orchestrate simple backfills and update ingest watermarks."""
from __future__ import annotations

import argparse
import shlex
import subprocess
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from aurum.db import update_ingest_watermark


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


def _run_command(template: str, current: date, *, use_shell: bool) -> None:
    rendered = template.format(date=current.isoformat())
    if use_shell:
        subprocess.run(rendered, shell=True, check=True)
    else:
        subprocess.run(shlex.split(rendered), check=True)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trigger backfills and bump ingest watermarks")
    parser.add_argument("--source", required=True, help="Ingest source name (matches register_ingest_source)")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date inclusive (YYYY-MM-DD). Defaults to start date")
    parser.add_argument("--key", default="default", help="Watermark key (default: default)")
    parser.add_argument(
        "--command",
        help="Optional command template to execute per day; use {date} placeholder",
    )
    parser.add_argument("--shell", action="store_true", help="Execute the command template via shell")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    parser.add_argument(
        "--watermark-time",
        default="23:59:59",
        help="Time component (HH:MM:SS) applied to the watermark timestamp",
    )
    return parser.parse_args(argv)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - user error path
        raise argparse.ArgumentTypeError(f"Invalid date: {value}") from exc


def _build_timestamp(current: date, time_str: str) -> datetime:
    try:
        hour, minute, second = [int(part) for part in time_str.split(":", 2)]
    except ValueError as exc:  # pragma: no cover - user error path
        raise argparse.ArgumentTypeError(f"Invalid time value: {time_str}") from exc
    return datetime(current.year, current.month, current.day, hour, minute, second, tzinfo=timezone.utc)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end) if args.end else start_date
    if end_date < start_date:
        raise argparse.ArgumentTypeError("--end must be on or after --start")

    for current in _date_range(start_date, end_date):
        print(f"[backfill] processing {current.isoformat()} for {args.source}")
        if args.command and not args.dry_run:
            _run_command(args.command, current, use_shell=args.shell)
        timestamp = _build_timestamp(current, args.watermark_time)
        if args.dry_run:
            print(f"  would update watermark {args.source}/{args.key} -> {timestamp.isoformat()}")
            continue
        update_ingest_watermark(args.source, args.key, timestamp)
        print(f"  updated watermark {args.source}/{args.key} -> {timestamp.isoformat()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
