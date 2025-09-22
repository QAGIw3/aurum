#!/usr/bin/env python3
"""Utility to backfill derived dbt models with consistent environment controls."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import List


def build_dbt_command(models: List[str], *, target: str | None, full_refresh: bool, vars_arg: str | None) -> List[str]:
    command = os.environ.get("DBT_COMMAND", "dbt").split()
    command.extend(["run", "--select", ",".join(models)])
    if full_refresh:
        command.append("--full-refresh")
    if target:
        command.extend(["--target", target])
    if vars_arg:
        command.extend(["--vars", vars_arg])
    return command


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "models",
        nargs="+",
        help="List of dbt models to backfill (e.g. publish_curve_observation mart_curve_latest)",
    )
    parser.add_argument("--target", help="Optional dbt target name", default=None)
    parser.add_argument(
        "--full-refresh",
        dest="full_refresh",
        action="store_true",
        help="Force full refresh when running the selected models",
    )
    parser.add_argument(
        "--start-date",
        help="Optional start date passed to dbt via AURUM_BACKFILL_START",
    )
    parser.add_argument(
        "--end-date",
        help="Optional end date passed to dbt via AURUM_BACKFILL_END",
    )
    parser.add_argument(
        "--vars",
        help="YAML-formatted string passed to dbt via --vars",
        default=None,
    )
    args = parser.parse_args()

    env = os.environ.copy()
    if args.start_date:
        env["AURUM_BACKFILL_START"] = args.start_date
    if args.end_date:
        env["AURUM_BACKFILL_END"] = args.end_date

    command = build_dbt_command(args.models, target=args.target, full_refresh=args.full_refresh, vars_arg=args.vars)
    print("Running:", " ".join(command))
    try:
        subprocess.run(command, check=True, env=env)
    except FileNotFoundError as exc:
        raise SystemExit(
            "dbt executable not found. Set DBT_COMMAND env var if dbt is installed elsewhere."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"dbt run failed with exit code {exc.returncode}") from exc


if __name__ == "__main__":
    main()
