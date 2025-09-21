#!/usr/bin/env python3
"""Validate the Airflow Variable mapping file."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if SRC_PATH.exists():  # allow invocation without installing
    sys.path.insert(0, str(SRC_PATH))

from aurum.airflow_utils.variables import load_variable_mapping, validate_variable_mapping


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        default="config/airflow_variables.json",
        help="Path to the variable mapping JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    path = Path(args.file)
    mapping = load_variable_mapping(path)
    errors = validate_variable_mapping(mapping)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(f"Validated {path} with {len(mapping)} variables")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
