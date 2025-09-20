#!/usr/bin/env python3
"""Validate EIA ingestion config against the harvested catalog.

Checks:
- dataset `path` exists in `config/eia_catalog.json`
- optional `data_path` is consistent (same base with `/data` suffix)
- `frequency` matches one of the dataset's supported frequencies
- presence of `param_overrides` for datasets that expose a single `value` column

Exits non-zero if any errors are found; prints warnings for soft issues.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = REPO_ROOT / "config" / "eia_catalog.json"
CONFIG_PATH = REPO_ROOT / "config" / "eia_ingest_datasets.json"


@dataclass
class Issue:
    level: str  # ERROR or WARN
    source_name: str
    message: str


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Missing required file: {path}", file=sys.stderr)
        raise SystemExit(2)
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in {path}: {exc}", file=sys.stderr)
        raise SystemExit(2)


def main() -> int:
    catalog = load_json(CATALOG_PATH)
    cfg = load_json(CONFIG_PATH)

    datasets_cfg = cfg.get("datasets") or []
    if not isinstance(datasets_cfg, list):
        print(f"Invalid datasets array in {CONFIG_PATH}", file=sys.stderr)
        return 2

    catalog_index: dict[str, dict] = {d["path"]: d for d in catalog.get("datasets", [])}
    issues: list[Issue] = []

    for entry in datasets_cfg:
        source = entry.get("source_name", "<unknown>")
        path = entry.get("path")
        if not path:
            issues.append(Issue("ERROR", source, "Missing required 'path'"))
            continue
        cd = catalog_index.get(path)
        if not cd:
            issues.append(Issue("ERROR", source, f"Path not found in catalog: {path}"))
            continue

        # Check data_path base alignment
        data_path = entry.get("data_path")
        if data_path and not data_path.endswith("/data"):
            issues.append(Issue("WARN", source, f"data_path does not end with '/data': {data_path}"))
        if data_path and not data_path.startswith(path):
            issues.append(Issue("WARN", source, f"data_path base mismatch: {data_path} vs {path}"))

        # Frequency check (case-insensitive)
        cfg_freq = (entry.get("frequency") or "").lower()
        supported = {f.get("id", "").lower() for f in cd.get("frequencies", [])}
        if cfg_freq and supported and cfg_freq not in supported:
            issues.append(Issue("ERROR", source, f"frequency '{entry.get('frequency')}' not in supported {sorted(supported)}"))

        # If catalog exposes only a 'value' column, ensure param_overrides includes data[0]=value
        columns = cd.get("data_columns") or []
        if columns == ["value"]:
            overrides = entry.get("param_overrides") or []
            flattened = {}
            for item in overrides if isinstance(overrides, list) else []:
                if isinstance(item, dict):
                    flattened.update(item)
            if flattened.get("data[0]") != "value":
                issues.append(Issue("WARN", source, "dataset has single 'value' column; add param_overrides [{\"data[0]\": \"value\"}]"))

    errors = [i for i in issues if i.level == "ERROR"]
    for i in issues:
        print(f"{i.level}: {i.source_name}: {i.message}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())

