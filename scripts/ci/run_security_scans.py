#!/usr/bin/env python3
"""Run security scanning tools with centrally managed exceptions."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

try:
    import yaml
except ImportError as exc:  # pragma: no cover - guidance for missing dep
    raise SystemExit("pyyaml is required to run security scans") from exc

EXCEPTIONS_FILE = Path(".security-exceptions.yml")


def _load_exception_ids() -> List[str]:
    if not EXCEPTIONS_FILE.exists():
        return []

    data = yaml.safe_load(EXCEPTIONS_FILE.read_text(encoding="utf-8")) or {}
    exceptions = data.get("exceptions", {})
    if isinstance(exceptions, dict):
        return [key for key in exceptions.keys() if isinstance(key, str)]
    if isinstance(exceptions, Iterable):  # pragma: no cover - defensive
        return [item for item in exceptions if isinstance(item, str)]
    return []


def _run_command(cmd: List[str]) -> int:
    print(f"[security] $ {' '.join(cmd)}", flush=True)
    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        print(f"[security] command failed with exit code {completed.returncode}", flush=True)
    return completed.returncode


def run_pip_audit(ignore: Iterable[str]) -> int:
    cmd = [
        "pip-audit",
        "--progress-spinner",
        "off",
        "--strict",
    ]
    for vuln in ignore:
        cmd.extend(["--ignore-vuln", vuln])
    return _run_command(cmd)


def run_bandit() -> int:
    cmd = [
        "bandit",
        "-q",
        "-r",
        "src/",
        "-s",
        "B101,B601",
    ]
    return _run_command(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run security scanning pipelines")
    parser.add_argument(
        "--tool",
        choices={"pip-audit", "bandit", "all"},
        default="all",
        help="Which tool to execute (default: all)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ignored = _load_exception_ids()

    exit_code = 0
    if args.tool in {"pip-audit", "all"}:
        result = run_pip_audit(ignored)
        exit_code = result if result != 0 else exit_code
    if args.tool in {"bandit", "all"}:
        result = run_bandit()
        exit_code = result if result != 0 else exit_code
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
