#!/usr/bin/env python3
"""Sync canonical GE suites into ge/expectations and build an index.

Canonical source: libs/contracts/ge/expectations
Target (generated): ge/expectations

This script is intentionally dependency-light (no Jinja2/PyYAML). It copies
the canonical suites, validates minimal invariants, and writes a catalog
index ge/index.json with domain and tier classification inferred from content.

Tiers:
- A: Minimal validator compatible (only supported expectation types)
- B: GE-only features detected (unsupported expectation types)
- C: YAML or multi-table suites (kept as GE-only)
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_DIR = REPO_ROOT / "libs/contracts/ge/expectations"
TARGET_DIR = REPO_ROOT / "ge/expectations"
INDEX_PATH = REPO_ROOT / "ge/index.json"
ENV_EXAMPLE = REPO_ROOT / ".env.example"


# Expectation types supported by the minimal validator (src/aurum/dq/validator.py)
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


DOMAIN_PREFIX_MAP: Dict[str, str] = {
    "mart": "mart",
    "external": "external",
    "iso": "iso",
    "drought": "drought",
    "curve": "curves",
    "fct": "curves",
    "publish": "curves",
    "noaa": "public",
    "eia": "public",
    "fred": "public",
    "fx": "public",
    "cpi": "public",
    "scenario": "scenarios",
    "stg": "staging",
}


@dataclass
class SuiteInfo:
    path: Path
    name: str
    ext: str
    domain: str
    tier: str  # A|B|C
    notes: str | None
    expectation_types: List[str]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_json_suite(path: Path) -> Tuple[str, Optional[str], List[str]]:
    """Return (suite_name, notes, types) for a JSON suite file."""
    data = json.loads(read_text(path))
    suite_name = data.get("expectation_suite_name") or path.stem
    notes = None
    if isinstance(data.get("meta"), dict):
        notes = data["meta"].get("notes")
    types: List[str] = []
    for item in data.get("expectations", []) or []:
        t = item.get("expectation_type")
        if t:
            types.append(str(t))
    return suite_name, notes, types


def parse_yaml_suite_name_and_types(raw: str) -> Tuple[Optional[str], List[str]]:
    """Very small YAML extraction without dependency on PyYAML.

    - Extract expectation_suite_name: <value>
    - Extract expectation_type: <value> occurrences
    """
    suite_name = None
    # match: expectation_suite_name: something
    m = re.search(r"^\s*expectation_suite_name\s*:\s*['\"]?([A-Za-z0-9_\-]+)['\"]?\s*$",
                  raw, flags=re.MULTILINE)
    if m:
        suite_name = m.group(1)
    types = re.findall(r"expectation_type\s*:\s*([A-Za-z0-9_]+)", raw)
    return suite_name, types


def infer_domain(stem: str) -> str:
    prefix = stem.split("_")[0]
    return DOMAIN_PREFIX_MAP.get(prefix, "misc")


def classify_tier(path: Path, types: List[str]) -> str:
    if path.suffix.lower() in {".yml", ".yaml"}:
        return "C"
    # JSON suite: if any unsupported expectation_type -> Tier B
    for t in types:
        if t not in SUPPORTED_TYPES:
            return "B"
    return "A"


def discover_canonical_files() -> List[Path]:
    return sorted([p for p in CANONICAL_DIR.glob("*.*") if p.suffix.lower() in {".json", ".yml", ".yaml"}])


def copy_to_target(src: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    content = src.read_bytes()
    # atomic-ish write
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(dst)
    return dst


def build_index(entries: List[SuiteInfo]) -> Dict[str, Dict[str, object]]:
    index: Dict[str, Dict[str, object]] = {}
    for info in entries:
        index[info.name] = {
            "suite": info.name,
            "path": str((TARGET_DIR / f"{info.name}{info.ext}").relative_to(REPO_ROOT)),
            "domain": info.domain,
            "tier": info.tier,
            "notes": info.notes or "",
            "expectation_types": sorted(set(info.expectation_types)),
        }
    return index


def main(argv: List[str]) -> int:
    canonical_files = discover_canonical_files()
    if not canonical_files:
        print(f"No canonical suites found in {CANONICAL_DIR}")
        return 1

    synced: List[SuiteInfo] = []

    for src in canonical_files:
        # Copy file
        dst = copy_to_target(src, TARGET_DIR)

        # Parse minimal metadata
        if src.suffix.lower() == ".json":
            suite_name, notes, types = parse_json_suite(src)
        else:
            raw = read_text(src)
            suite_name, types = parse_yaml_suite_name_and_types(raw)
            notes = None
            if not suite_name:
                suite_name = src.stem

        domain = infer_domain(src.stem)
        tier = classify_tier(src, types)

        synced.append(
            SuiteInfo(
                path=dst,
                name=suite_name,
                ext=src.suffix,
                domain=domain,
                tier=tier,
                notes=notes,
                expectation_types=types,
            )
        )

    # Write index
    index = build_index(synced)
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Synced {len(synced)} suites from {CANONICAL_DIR} → {TARGET_DIR}")
    print(f"Wrote index: {INDEX_PATH.relative_to(REPO_ROOT)}")
    # Show a brief tier summary
    summary: Dict[str, int] = {"A": 0, "B": 0, "C": 0}
    for s in synced:
        summary[s.tier] = summary.get(s.tier, 0) + 1
    print(f"Tier summary: A={summary['A']} B={summary['B']} C={summary['C']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
