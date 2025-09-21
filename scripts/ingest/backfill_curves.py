#!/usr/bin/env python
"""Run rolling backfills for vendor curve ingestion using the parser CLI."""
from __future__ import annotations

import argparse
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence


def _daterange(start: date, end: date) -> Iterable[date]:
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


def _run_runner(files: Sequence[str], as_of: date, extra_args: Sequence[str]) -> None:
    args = [
        "python",
        "-m",
        "aurum.parsers.runner",
        "--as-of",
        as_of.isoformat(),
        *extra_args,
        *files,
    ]
    print(f"Executing: {' '.join(args)}")
    subprocess.run(args, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill vendor curve workbooks over a date range")
    parser.add_argument("--start", required=True, help="Start as-of date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End as-of date (YYYY-MM-DD)")
    parser.add_argument("--vendor", action="append", required=True, help="Vendor key (e.g. pw, eugp, rp). Can be specified multiple times")
    parser.add_argument("--drop-dir", type=Path, default=Path("files"), help="Directory containing vendor workbooks")
    parser.add_argument("--runner-arg", action="append", default=[], help="Extra argument to forward to aurum.parsers.runner (repeatable)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    vendors = [item.lower() for item in args.vendor]

    if start > end:
        raise SystemExit("--start must be on or before --end")

    runner_args = list(args.runner_arg)

    for as_of in _daterange(start, end):
        for vendor in vendors:
            pattern = f"EOD_{vendor.upper()}_{as_of.strftime('%Y%m%d')}*.xlsx"
            files = sorted(str(path) for path in args.drop_dir.glob(pattern))
            if not files:
                print(f"[warn] no files matched {pattern} in {args.drop_dir}")
                continue
            _run_runner(files, as_of, runner_args)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
