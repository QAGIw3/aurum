#!/usr/bin/env python3
"""Apply a matrix of Airflow Variables from a JSON mapping.

Usage:
  python scripts/airflow/set_variables.py --file config/airflow_variables.json --apply

If --apply is not provided, prints the airflow CLI commands to set variables.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--file", default="config/airflow_variables.json", help="Path to JSON mapping of key->value")
    p.add_argument("--apply", action="store_true", help="Execute 'airflow variables set' for each entry")
    return p.parse_args(argv)


def load_mapping(path: Path) -> dict[str, str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Expected top-level object mapping")
    # normalize to str
    mapping: dict[str, str] = {str(k): str(v) for k, v in data.items()}
    return mapping


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    path = Path(args.file)
    if not path.exists():
        print(f"Mapping file not found: {path}", file=sys.stderr)
        return 1
    mapping = load_mapping(path)
    if args.apply:
        for key, value in mapping.items():
            cmd = ["airflow", "variables", "set", key, value]
            print(" ".join(cmd))
            subprocess.run(cmd, check=True)
    else:
        for key, value in mapping.items():
            print(f"airflow variables set {key} {value}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

