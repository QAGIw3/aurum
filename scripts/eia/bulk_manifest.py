#!/usr/bin/env python3
"""Fetch the EIA bulk manifest and summarise dataset freshness."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_URL = "https://www.eia.gov/opendata/bulk/manifest.txt"
DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "eia_bulk_manifest.json"
DEFAULT_CONFIG = REPO_ROOT / "config" / "eia_bulk_datasets.json"


class ManifestError(RuntimeError):
    """Raised when manifest fetching or parsing fails."""


def fetch_manifest(url: str, *, timeout: int = 60) -> dict[str, Any]:
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        raise ManifestError(f"Failed to fetch manifest: {response.status_code} {response.text[:200]}")
    try:
        return response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise ManifestError("Manifest response was not valid JSON") from exc


def _file_stem(url: str) -> str:
    name = url.rsplit("/", 1)[-1]
    return name.lower()


def _locate_entry(manifest: dict[str, Any], dataset_url: str) -> dict[str, Any] | None:
    target = _file_stem(dataset_url)
    for entry in manifest.get("dataset", {}).values():
        access_url = str(entry.get("accessURL", ""))
        if _file_stem(access_url) == target:
            return entry
    return None


def summarise_datasets(manifest: dict[str, Any], config_path: Path) -> dict[str, Dict[str, Any]]:
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    datasets = cfg.get("datasets", [])
    if not isinstance(datasets, list):
        raise ManifestError(f"Invalid datasets array in {config_path}")

    summary: dict[str, Dict[str, Any]] = {}
    for entry in datasets:
        if not isinstance(entry, dict):
            continue
        source_name = entry.get("source_name")
        url = entry.get("url")
        if not source_name or not url:
            continue
        manifest_entry = _locate_entry(manifest, url)
        last_updated = None
        if manifest_entry:
            last_updated = manifest_entry.get("last_updated") or manifest_entry.get("modified")
        summary[source_name] = {
            "url": url,
            "last_updated": last_updated,
            "title": manifest_entry.get("title") if manifest_entry else None,
            "access_url": manifest_entry.get("accessURL") if manifest_entry else None,
        }
    return summary


def update_config_with_timestamps(config_path: Path, summary: dict[str, Dict[str, Any]]) -> None:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    datasets = payload.get("datasets", [])
    if not isinstance(datasets, list):
        raise ManifestError(f"Invalid datasets array in {config_path}")

    for entry in datasets:
        if not isinstance(entry, dict):
            continue
        source_name = entry.get("source_name")
        if not source_name or source_name not in summary:
            continue
        entry["last_modified"] = summary[source_name].get("last_updated")
    config_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-url", default=DEFAULT_MANIFEST_URL, help="Manifest URL (default: EIA bulk manifest)")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to write manifest summary (default: artifacts/eia_bulk_manifest.json)")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to eia_bulk_datasets.json for alignment")
    parser.add_argument("--update-config", action="store_true", help="Persist last_modified timestamps back into the config file")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = fetch_manifest(args.manifest_url)
    summary = summarise_datasets(manifest, Path(args.config))

    result = {
        "manifest_url": args.manifest_url,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": summary,
    }
    output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote manifest summary for {len(summary)} dataset(s) -> {output_path}")

    if args.update_config:
        update_config_with_timestamps(Path(args.config), summary)
        print(f"Updated last_modified metadata in {args.config}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except ManifestError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
