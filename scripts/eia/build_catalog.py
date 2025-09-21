"""Harvest metadata for all EIA API v2 datasets and persist a catalog."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable

import requests


BASE_URL = "https://api.eia.gov/v2"
DEFAULT_OUTPUT = "config/eia_catalog.json"
DEFAULT_REPORT = "artifacts/eia_catalog_report.json"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.05  # be nice to the API
MAX_RETRIES = 5
BASIC_BACKOFF = 1.5


class CatalogError(RuntimeError):
    """Raised when required configuration is missing or fetching fails."""


@dataclass
class Dataset:
    path: str
    name: str | None
    description: str | None
    frequencies: list[dict[str, Any]]
    facets: list[dict[str, Any]]
    data_columns: list[str]
    start_period: str | None
    end_period: str | None
    default_frequency: str | None
    default_date_format: str | None
    browser_total: int | None
    browser_frequency: str | None
    browser_date_format: str | None
    warnings: list[str]


def _build_url(segments: Iterable[str]) -> str:
    path = "/".join(segment.strip("/") for segment in segments if segment)
    if path:
        return f"{BASE_URL}/{path}/"
    return f"{BASE_URL}/"


def _fetch(
    segments: tuple[str, ...], api_key: str, *, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    url = _build_url(segments)
    backoff = BASIC_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params={"api_key": api_key, **(params or {})},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:  # pragma: no cover - network failure
            if attempt == MAX_RETRIES:
                raise CatalogError(f"Failed to fetch {url}: {exc}") from exc
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code == 200:
            payload: dict[str, Any] = response.json()
            return payload.get("response", {})

        if response.status_code == 429 and attempt < MAX_RETRIES:
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else backoff
            time.sleep(delay)
            backoff *= 2
            continue

        if attempt == MAX_RETRIES:
            raise CatalogError(
                f"Non-200 response for {url}: {response.status_code} {response.text[:200]}"
            )

        time.sleep(backoff)
        backoff *= 2

    raise CatalogError(f"Unable to fetch {url} after {MAX_RETRIES} attempts")


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fetch_dataset_stats(path: tuple[str, ...], api_key: str) -> tuple[int | None, str | None, str | None, list[str]]:
    warnings: list[str] = []
    try:
        payload = _fetch(path + ("data",), api_key, params={"length": 0})
    except CatalogError as exc:
        warnings.append(str(exc))
        return (None, None, None, warnings)

    response = payload or {}
    total = _coerce_int(response.get("total"))
    frequency = response.get("frequency")
    date_format = response.get("dateFormat")

    return (total, frequency, date_format, warnings)


def _normalize_dataset(path: tuple[str, ...], metadata: dict[str, Any], *, api_key: str, include_counts: bool) -> Dataset:
    data_section = metadata.get("data", {})
    columns = list(data_section.keys()) if isinstance(data_section, dict) else []
    browser_total: int | None = None
    browser_frequency: str | None = None
    browser_date_format: str | None = None
    warnings: list[str] = []

    if include_counts:
        browser_total, browser_frequency, browser_date_format, warnings = _fetch_dataset_stats(path, api_key)

    return Dataset(
        path="/".join(path),
        name=metadata.get("name") or metadata.get("id"),
        description=metadata.get("description"),
        frequencies=list(metadata.get("frequency", [])),
        facets=list(metadata.get("facets", [])),
        data_columns=columns,
        start_period=metadata.get("startPeriod"),
        end_period=metadata.get("endPeriod"),
        default_frequency=metadata.get("defaultFrequency"),
        default_date_format=metadata.get("defaultDateFormat"),
        browser_total=browser_total,
        browser_frequency=browser_frequency,
        browser_date_format=browser_date_format,
        warnings=warnings,
    )


def build_catalog(api_key: str, *, include_counts: bool = False) -> dict[str, Any]:
    datasets: list[Dataset] = []
    queue: deque[tuple[str, ...]] = deque([()])

    while queue:
        segments = queue.popleft()
        metadata = _fetch(segments, api_key)

        routes = metadata.get("routes") or []
        for route in routes:
            route_id = route.get("id")
            if not route_id:
                continue
            queue.append(segments + (str(route_id),))

        if metadata.get("data") is not None:
            datasets.append(
                _normalize_dataset(
                    segments,
                    metadata,
                    api_key=api_key,
                    include_counts=include_counts,
                )
            )

        time.sleep(SLEEP_SECONDS)

    datasets.sort(key=lambda d: d.path)
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "generated_at": generated_at,
        "dataset_count": len(datasets),
        "base_url": BASE_URL,
        "datasets": [asdict(dataset) for dataset in datasets],
    }


def _load_json(path: str) -> dict[str, Any] | None:
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _catalog_index(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["path"]: entry for entry in catalog.get("datasets", [])}


def _diff_catalogs(previous: dict[str, Any] | None, current: dict[str, Any]) -> dict[str, Any]:
    if not previous:
        return {
            "added_paths": sorted(entry["path"] for entry in current.get("datasets", [])),
            "removed_paths": [],
            "changed": [],
        }

    prev_index = _catalog_index(previous)
    curr_index = _catalog_index(current)

    prev_paths = set(prev_index)
    curr_paths = set(curr_index)

    added = sorted(curr_paths - prev_paths)
    removed = sorted(prev_paths - curr_paths)

    monitored_fields = [
        "name",
        "description",
        "frequencies",
        "facets",
        "data_columns",
        "start_period",
        "end_period",
        "default_frequency",
        "default_date_format",
        "browser_total",
        "browser_frequency",
        "browser_date_format",
    ]

    changed: list[dict[str, Any]] = []
    for path in sorted(prev_paths & curr_paths):
        prev_entry = prev_index[path]
        curr_entry = curr_index[path]
        differences: dict[str, Any] = {}
        for field in monitored_fields:
            if prev_entry.get(field) != curr_entry.get(field):
                differences[field] = {
                    "previous": prev_entry.get(field),
                    "current": curr_entry.get(field),
                }
        if differences:
            changed.append({"path": path, "differences": differences})

    return {
        "added_paths": added,
        "removed_paths": removed,
        "changed": changed,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Path to write the catalog JSON (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--report",
        default=DEFAULT_REPORT,
        help=f"Optional path to write a validation report (default: {DEFAULT_REPORT})",
    )
    parser.add_argument(
        "--include-counts",
        action="store_true",
        help="Fetch dataset /data routes to capture browser totals and date metadata",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise CatalogError("EIA_API_KEY environment variable is required")

    previous = _load_json(args.output)
    catalog = build_catalog(api_key, include_counts=args.include_counts)

    output_path = args.output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(catalog, fp, indent=2)
        fp.write("\n")

    report_path = args.report
    if args.include_counts:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        summary = {
            "generated_at": catalog["generated_at"],
            "dataset_count": catalog["dataset_count"],
            "with_counts": True,
            "missing_counts": [
                ds["path"]
                for ds in catalog["datasets"]
                if ds.get("browser_total") is None
            ],
            "warnings": {
                ds["path"]: ds.get("warnings", [])
                for ds in catalog["datasets"]
                if ds.get("warnings")
            },
            "diff": _diff_catalogs(previous, catalog),
        }
        with open(report_path, "w", encoding="utf-8") as fp:
            json.dump(summary, fp, indent=2)
            fp.write("\n")

    print(
        f"Wrote catalog with {catalog['dataset_count']} datasets to {output_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
