#!/usr/bin/env python3
"""Execute Great Expectations suites against the DuckDB warehouse produced by dbt."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable

import duckdb

from aurum.dq import ExpectationFailedError, enforce_expectation_suite

DEFAULT_WAREHOUSE = Path("artifacts/dbt/warehouse.duckdb")
DEFAULT_SUITE_DIR = Path("ge/expectations")

CRITICAL_SUITES: Dict[str, str] = {
    "analytics.mart_curve_latest": "mart_curve_latest.json",
    "analytics.mart_eia_series_latest": "mart_eia_series_latest.json",
    "analytics.mart_scenario_output": "mart_scenario_output.json",
}


def _resolve_suites(suite_dir: Path, selection: Iterable[str] | None) -> Dict[str, Path]:
    if selection:
        invalid = sorted(set(selection) - CRITICAL_SUITES.keys())
        if invalid:
            raise SystemExit(f"Unknown relation(s) requested: {', '.join(invalid)}")
        relations = {rel: CRITICAL_SUITES[rel] for rel in selection}
    else:
        relations = CRITICAL_SUITES
    return {relation: suite_dir / filename for relation, filename in relations.items()}


def _load_dataframe(connection: duckdb.DuckDBPyConnection, relation: str):
    try:
        return connection.sql(f"select * from {relation}").df()
    except Exception as exc:  # pragma: no cover - duckdb errors
        raise RuntimeError(f"Failed to load relation '{relation}': {exc}") from exc


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Great Expectations suites against DuckDB output")
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=DEFAULT_WAREHOUSE,
        help=f"Path to DuckDB warehouse file (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--suite-dir",
        type=Path,
        default=DEFAULT_SUITE_DIR,
        help=f"Directory containing expectation suites (default: {DEFAULT_SUITE_DIR})",
    )
    parser.add_argument(
        "--relation",
        action="append",
        help="Limit validation to the specified fully-qualified relation (can be provided multiple times)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    warehouse = args.warehouse
    if not warehouse.exists():
        parser.error(f"Warehouse file not found: {warehouse}")
    suite_dir = args.suite_dir
    if not suite_dir.exists():
        parser.error(f"Suite directory not found: {suite_dir}")

    suites = _resolve_suites(suite_dir, args.relation)

    connection = duckdb.connect(str(warehouse))
    failures: Dict[str, ExpectationFailedError] = {}

    for relation, suite_path in suites.items():
        if not suite_path.exists():
            error = ExpectationFailedError(suite_path.stem, [])
            error.args = (f"Expectation suite missing: {suite_path}",)
            failures[relation] = error
            print(f"[great-expectations] {relation}: MISSING SUITE {suite_path}", file=sys.stderr)
            continue
        df = _load_dataframe(connection, relation)
        try:
            enforce_expectation_suite(df, suite_path, suite_name=suite_path.stem)
            print(f"[great-expectations] {relation}: PASS ({suite_path.name})")
        except ExpectationFailedError as exc:
            failures[relation] = exc
            print(f"[great-expectations] {relation}: FAIL - {exc}", file=sys.stderr)

    if failures:
        print(
            f"Great Expectations detected {len(failures)} failing suite(s): "
            + ", ".join(failures.keys()),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
