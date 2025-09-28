#!/usr/bin/env python3
"""Wrapper CLI for orchestrating Airflow backfill operations via BackfillDriver."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.ops.backfill_driver import BackfillConfig, BackfillDriver


@dataclass
class CLIResult:
    code: int
    payload: Dict[str, Any]


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aurum Airflow backfill helper")
    parser.add_argument("source", help="Backfill source name")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--rate-limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--priority", choices=["low", "normal", "high"], default="normal")
    parser.add_argument("--job-id", help="Custom job identifier")
    parser.add_argument("--output", help="File to write JSON results")
    return parser.parse_args(argv)


def _build_config(ns: argparse.Namespace) -> BackfillConfig:
    start = datetime.strptime(ns.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(ns.end_date, "%Y-%m-%d").date()
    if start > end:
        raise ValueError("start_date must be before end_date")
    return BackfillConfig(
        source=ns.source,
        start_date=start,
        end_date=end,
        concurrency=ns.concurrency,
        batch_size=ns.batch_size,
        max_retries=ns.max_retries,
        rate_limit=ns.rate_limit,
        dry_run=ns.dry_run,
        priority=ns.priority,
        job_id=ns.job_id or f"BACKFILL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )


async def _run_driver(config: BackfillConfig) -> Dict[str, Any]:
    driver = BackfillDriver(config)
    return await driver.execute()


def _write_output(payload: Dict[str, Any], path: Optional[str]) -> None:
    if not path:
        print(json.dumps(payload, indent=2, default=str))
        return
    Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Results written to {path}")


def main(argv: Optional[list[str]] = None) -> int:
    try:
        args = _parse_args(argv)
        config = _build_config(args)
    except Exception as exc:
        print(f"Argument error: {exc}")
        return 2

    try:
        payload = asyncio.run(_run_driver(config))
    except KeyboardInterrupt:
        print("Backfill interrupted by user")
        return 130
    except Exception as exc:
        print(f"Backfill failed: {exc}")
        return 1

    _write_output(payload, args.output)
    return 0 if payload.get("failed_jobs", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

