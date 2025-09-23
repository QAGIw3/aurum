#!/usr/bin/env python3
"""Manage Airflow Variables from ``config/airflow_variables.json``.

Examples
--------
Validate without touching Airflow::

    python scripts/airflow/set_variables.py --check

Preview the commands that would be executed::

    python scripts/airflow/set_variables.py
    python scripts/airflow/set_variables.py --apply --dry-run

Apply the desired state (and optionally prune variables not present)::

    python scripts/airflow/set_variables.py --apply
    python scripts/airflow/set_variables.py --apply --prune
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if SRC_PATH.exists():  # allow running without installing the package
    sys.path.insert(0, str(SRC_PATH))

from aurum.airflow_utils.variables import load_variable_mapping, validate_variable_mapping


@dataclass(frozen=True)
class Action:
    op: str  # "set" or "delete"
    key: str
    value: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        default="config/airflow_variables.json",
        help="Path to the variable mapping JSON",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the mapping file and exit",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply differences using the Airflow CLI",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands that would run instead of executing them",
    )
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Delete Airflow variables that are not present in the mapping (requires --apply)",
    )
    return parser.parse_args(argv)


def _airflow_cli_tokens() -> list[str]:
    """Return the Airflow CLI command as tokens (supports AIRFLOW_CLI wrapper)."""
    raw = os.getenv("AIRFLOW_CLI", "airflow")
    try:
        return shlex.split(raw)
    except Exception:
        return [raw]


def fetch_existing_variables(*, require: bool) -> dict[str, str] | None:
    """Export existing Airflow variables via the CLI."""

    tmp_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmp_path = Path(tmp_file.name)
    tmp_file.close()
    try:
        subprocess.run(
            [*_airflow_cli_tokens(), "variables", "export", str(tmp_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        if require:
            raise RuntimeError("Airflow CLI not found. Install 'airflow' or set AIRFLOW_CLI to a wrapper (e.g., 'docker compose -f <file> exec -T <svc> airflow').") from exc
        return None
    except subprocess.CalledProcessError as exc:
        if require:
            stderr = exc.stderr.strip() if exc.stderr else str(exc)
            raise RuntimeError(f"Failed to export Airflow variables: {stderr}") from exc
        return None

    try:
        raw = json.loads(tmp_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    finally:
        tmp_path.unlink(missing_ok=True)

    if not isinstance(raw, Mapping):
        return {}
    existing: dict[str, str] = {}
    for key, value in raw.items():
        existing[str(key)] = "" if value is None else str(value)
    return existing


def build_actions(
    desired: Mapping[str, str],
    existing: Mapping[str, str] | None,
    *,
    prune: bool,
) -> list[Action]:
    actions: list[Action] = []
    if existing is None:
        for key, value in desired.items():
            actions.append(Action("set", key, value))
        return actions

    for key, value in desired.items():
        current = existing.get(key)
        if current != value:
            actions.append(Action("set", key, value))

    if prune:
        for key in sorted(set(existing) - set(desired)):
            actions.append(Action("delete", key))
    return actions


def print_actions(actions: Iterable[Action]) -> None:
    cli = _airflow_cli_tokens()
    for action in actions:
        if action.op == "set":
            value = action.value if action.value is not None else ""
            cmd = [*cli, "variables", "set", action.key, value]
            print(" ".join(cmd))
        elif action.op == "delete":
            cmd = [*cli, "variables", "delete", action.key]
            print(" ".join(cmd))
        else:  # pragma: no cover - defensive
            print(f"# unknown action for {action.key}")


def execute_actions(actions: Iterable[Action]) -> None:
    cli = _airflow_cli_tokens()
    for action in actions:
        if action.op == "set":
            value = action.value if action.value is not None else ""
            cmd = [*cli, "variables", "set", action.key, value]
        elif action.op == "delete":
            cmd = [*cli, "variables", "delete", action.key]
        else:  # pragma: no cover - defensive
            continue
        print(" ".join(cmd))
        subprocess.run(cmd, check=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    mapping_path = Path(args.file)
    desired = load_variable_mapping(mapping_path)

    validation_errors = validate_variable_mapping(desired)
    if validation_errors:
        for error in validation_errors:
            print(error, file=sys.stderr)
        return 1

    if args.check and not args.apply:
        print(f"Validated {mapping_path} with {len(desired)} variables")
        return 0

    if not args.apply:
        print_actions(Action("set", key, value) for key, value in desired.items())
        return 0

    try:
        existing = fetch_existing_variables(require=not args.dry_run or args.prune)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    actions = build_actions(desired, existing, prune=args.prune)
    if not actions:
        print("Airflow variables already match the desired state; nothing to do.")
        return 0

    if args.dry_run:
        print_actions(actions)
        return 0

    execute_actions(actions)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
