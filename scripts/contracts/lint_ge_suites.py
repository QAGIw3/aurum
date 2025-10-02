#!/usr/bin/env python3
"""Lint GE suites for consistency and drift.

Checks:
- Drift: ge/expectations content matches libs/contracts/ge/expectations
- Suite name equals file stem
- Tier A suites only use minimal-validator supported expectation types
- Optional: env_overrides variables exist in .env.example (warning)
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_DIR = REPO_ROOT / "libs/contracts/ge/expectations"
TARGET_DIR = REPO_ROOT / "ge/expectations"
ENV_EXAMPLE = REPO_ROOT / ".env.example"


SUPPORTED_TYPES: set[str] = {
    "expect_table_columns_to_match_set",
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_unique",
    "expect_compound_columns_to_be_unique",
    "expect_multicolumn_values_to_be_unique",
    "expect_column_values_to_match_regex",
    "expect_column_proportion_of_nonnulls_to_be_between",
    "expect_column_values_to_be_of_type",
    "expect_column_pair_values_to_be_equal",
    "expect_column_pair_values_A_to_be_greater_than_B",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_json_suite(path: Path) -> Tuple[str, List[str], List[str]]:
    data = json.loads(_read(path))
    name = data.get("expectation_suite_name") or path.stem
    types = [e.get("expectation_type") for e in data.get("expectations", []) if isinstance(e, dict)]
    overrides: List[str] = []
    for e in data.get("expectations", []) or []:
        meta = e.get("meta") if isinstance(e, dict) else None
        if not isinstance(meta, dict):
            continue
        env_map = meta.get("env_overrides")
        if isinstance(env_map, dict):
            overrides.extend([str(v) for v in env_map.values()])
    return name, [t for t in types if t], overrides


def _parse_yaml_suite_name_and_types(raw: str) -> Tuple[Optional[str], List[str]]:
    m = re.search(r"^\s*expectation_suite_name\s*:\s*['\"]?([A-Za-z0-9_\-]+)['\"]?\s*$",
                  raw, flags=re.MULTILINE)
    name = m.group(1) if m else None
    types = re.findall(r"expectation_type\s*:\s*([A-Za-z0-9_]+)", raw)
    return name, types


def _load_env_example() -> set[str]:
    if not ENV_EXAMPLE.exists():
        return set()
    content = _read(ENV_EXAMPLE)
    keys: set[str] = set()
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            keys.add(line.split("=", 1)[0].strip())
    return keys


def main(argv: List[str]) -> int:
    errors: List[str] = []
    warnings: List[str] = []

    # 1) Drift check
    canonical_files = {p.name: p for p in CANONICAL_DIR.glob("*.*")}
    target_files = {p.name: p for p in TARGET_DIR.glob("*.*")}

    # Missing in target
    for name in sorted(set(canonical_files) - set(target_files)):
        errors.append(f"Missing in target: ge/expectations/{name}")

    # Extra in target
    for name in sorted(set(target_files) - set(canonical_files)):
        warnings.append(f"Extra file in target (not in canonical): ge/expectations/{name}")

    # Content drift
    for name in sorted(set(canonical_files) & set(target_files)):
        c_bytes = canonical_files[name].read_bytes()
        t_bytes = target_files[name].read_bytes()
        if c_bytes != t_bytes:
            errors.append(f"Content drift: {name} differs from canonical")

    # 2) Duplicate suite names within each directory
    def _collect_suite_names(dir_map: Dict[str, Path]) -> Dict[str, List[str]]:
        names: Dict[str, List[str]] = {}
        for name, path in dir_map.items():
            stem = path.stem
            if path.suffix.lower() == ".json":
                try:
                    suite_name, _, _ = _parse_json_suite(path)
                except Exception:
                    suite_name = stem
            else:
                raw = _read(path)
                suite_name, _ = _parse_yaml_suite_name_and_types(raw)
                if not suite_name:
                    suite_name = stem
            names.setdefault(suite_name, []).append(name)
        return names

    canon_suites = _collect_suite_names(canonical_files)
    target_suites = _collect_suite_names(target_files)
    for suite_name, files in sorted(canon_suites.items()):
        if len(files) > 1:
            errors.append(f"Canonical duplicate suite name '{suite_name}' in files: {sorted(files)}")
    for suite_name, files in sorted(target_suites.items()):
        if len(files) > 1:
            errors.append(f"Target duplicate suite name '{suite_name}' in files: {sorted(files)}")

    # 3) Suite name equals file stem + Tier A type checks
    env_keys = _load_env_example()
    for name, path in sorted(target_files.items()):
        stem = path.stem
        if path.suffix.lower() == ".json":
            try:
                suite_name, types, overrides = _parse_json_suite(path)
            except Exception as exc:
                errors.append(f"Invalid JSON: {name}: {exc}")
                continue
        else:
            raw = _read(path)
            suite_name, types = _parse_yaml_suite_name_and_types(raw)
            overrides = []

        if suite_name and suite_name != stem:
            errors.append(f"Suite name mismatch in {name}: '{suite_name}' != '{stem}'")

        # YAML suites are Tier C; skip type enforcement for them
        if path.suffix.lower() in {".yml", ".yaml"}:
            continue

        # Tier inference: if any unsupported type present, consider GE-only (Tier B)
        unsupported = [t for t in types if t not in SUPPORTED_TYPES]
        if unsupported:
            # Note: informational unless explicitly required; make it an error to be strict.
            warnings.append(f"{name}: GE-only expectation types present: {sorted(set(unsupported))}")

        # env_overrides keys defined?
        for key in overrides:
            if key and key not in env_keys:
                warnings.append(f"{name}: env_overrides references undefined var '{key}' in .env.example")

    # Report
    if warnings:
        for w in warnings:
            print(f"WARNING: {w}")
    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        return 1

    print("GE suites lint passed: no errors")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
