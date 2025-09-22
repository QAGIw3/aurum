#!/usr/bin/env python3
"""Refresh ISO catalog contract metadata and execute validation suites.

Runs:
- dbt run: materialize `mart_external_series_catalog`
- dbt test: validate the mart + dependents (unless skipped)
- Great Expectations: execute the `mart_external_series_catalog` checkpoint (unless skipped)

See docs/external/iso_canonical_contract.md for details.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Iterable, List


def run_command(command: Iterable[str], *, env: dict[str, str]) -> None:
    cmd = list(command)
    print("Running:", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, env=env)
    except FileNotFoundError as exc:
        raise SystemExit(f"Executable not found for command: {' '.join(cmd)}") from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Command failed with exit code {exc.returncode}: {' '.join(cmd)}") from exc


def build_dbt_run(target: str | None, profiles_dir: str | None, threads: int | None) -> List[str]:
    command = os.environ.get("DBT_COMMAND", "dbt").split()
    command.extend(["run", "--select", "mart_external_series_catalog"])
    if target:
        command.extend(["--target", target])
    if profiles_dir:
        command.extend(["--profiles-dir", profiles_dir])
    if threads:
        command.extend(["--threads", str(threads)])
    return command


def build_dbt_test(target: str | None, profiles_dir: str | None, threads: int | None) -> List[str]:
    command = os.environ.get("DBT_COMMAND", "dbt").split()
    command.extend(["test", "--select", "mart_external_series_catalog+"])
    if target:
        command.extend(["--target", target])
    if profiles_dir:
        command.extend(["--profiles-dir", profiles_dir])
    if threads:
        command.extend(["--threads", str(threads)])
    return command


def build_ge_checkpoint(checkpoint: str) -> List[str]:
    command = os.environ.get("GE_COMMAND", "great_expectations").split()
    command.extend(["--v3-api", "checkpoint", "run", checkpoint])
    return command


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", help="Optional dbt target name", default=None)
    parser.add_argument("--profiles-dir", help="Optional dbt profiles directory", default=None)
    parser.add_argument(
        "--threads", type=int, help="Override dbt threads when running", default=None
    )
    parser.add_argument(
        "--skip-dbt-test",
        action="store_true",
        help="Skip running dbt tests after the model refresh",
    )
    parser.add_argument(
        "--skip-ge",
        action="store_true",
        help="Skip running the Great Expectations checkpoint",
    )
    parser.add_argument(
        "--checkpoint",
        default="mart_external_series_catalog",
        help="Great Expectations checkpoint name (default: mart_external_series_catalog)",
    )

    args = parser.parse_args()

    env = os.environ.copy()

    run_command(
        build_dbt_run(args.target, args.profiles_dir, args.threads),
        env=env,
    )

    if not args.skip_dbt_test:
        run_command(
            build_dbt_test(args.target, args.profiles_dir, args.threads),
            env=env,
        )

    if not args.skip_ge:
        run_command(build_ge_checkpoint(args.checkpoint), env=env)


if __name__ == "__main__":
    main()
