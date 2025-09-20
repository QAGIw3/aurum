#!/usr/bin/env python3
"""Ensure scenario Avro schemas stay in sync with committed snapshots."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

SCHEMA_PAIRS = [
    (
        Path("kafka/schemas/scenario.request.v1.avsc"),
        Path("tests/kafka/snapshots/scenario.request.v1.avsc"),
    ),
    (
        Path("kafka/schemas/scenario.output.v1.avsc"),
        Path("tests/kafka/snapshots/scenario.output.v1.avsc"),
    ),
]


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate scenario Avro schemas against snapshots")
    parser.add_argument("--update", action="store_true", help="Refresh snapshots from current schema files")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    exit_code = 0

    for live, snapshot in SCHEMA_PAIRS:
        if args.update:
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            snapshot.write_text(live.read_text(encoding="utf-8"), encoding="utf-8")
            display_path = snapshot
            if snapshot.is_absolute():  # pragma: no cover - defensive for unusual setups
                try:
                    display_path = snapshot.relative_to(Path.cwd())
                except ValueError:
                    pass
            print(f"Updated snapshot {display_path}")
            continue

        if not live.exists():
            print(f"Missing schema file: {live}")
            exit_code = 1
            continue
        if not snapshot.exists():
            print(f"Missing snapshot file: {snapshot}")
            exit_code = 1
            continue

        if _load(live) != _load(snapshot):
            print(
                "Scenario schema mismatch: "
                f"{live} differs from {snapshot}. Run 'python scripts/ci/verify_scenario_schema.py --update'"
            )
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
