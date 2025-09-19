"""Convenience wrapper to run the Aurum parser runner with typical options."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from aurum.parsers import runner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest daily vendor workbooks and persist outputs")
    parser.add_argument("files", nargs="+", type=Path, help="Workbook files to ingest")
    parser.add_argument("--as-of", dest="as_of", default=date.today().isoformat())
    parser.add_argument("--output-dir", dest="output_dir", default="artifacts")
    parser.add_argument("--format", dest="fmt", default="parquet", choices=["parquet", "csv"])
    parser.add_argument("--write-iceberg", action="store_true")
    parser.add_argument("--lakefs-commit", action="store_true")
    parser.add_argument("--lakefs-branch")
    parser.add_argument("--lakefs-tag")
    parser.add_argument("--lakefs-message")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)

    runner_args: list[str] = [
        "--as-of",
        args.as_of,
        "--output-dir",
        args.output_dir,
        "--format",
        args.fmt,
    ]
    if args.write_iceberg:
        runner_args.append("--write-iceberg")
    if args.lakefs_commit:
        runner_args.append("--lakefs-commit")
    if args.verbose:
        runner_args.append("--verbose")
    if args.lakefs_branch:
        runner_args.extend(["--lakefs-branch", args.lakefs_branch])
    if args.lakefs_tag:
        runner_args.extend(["--lakefs-tag", args.lakefs_tag])
    if args.lakefs_message:
        runner_args.extend(["--lakefs-message", args.lakefs_message])

    runner_args.extend(str(path) for path in args.files)
    return runner.main(runner_args)


if __name__ == "__main__":
    sys.exit(main())
