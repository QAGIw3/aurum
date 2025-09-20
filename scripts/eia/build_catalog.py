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


def _build_url(segments: Iterable[str]) -> str:
    path = "/".join(segment.strip("/") for segment in segments if segment)
    if path:
        return f"{BASE_URL}/{path}/"
    return f"{BASE_URL}/"


def _fetch(segments: tuple[str, ...], api_key: str) -> dict[str, Any]:
    url = _build_url(segments)
    backoff = BASIC_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params={"api_key": api_key},
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


def _normalize_dataset(path: tuple[str, ...], metadata: dict[str, Any]) -> Dataset:
    data_section = metadata.get("data", {})
    columns = list(data_section.keys()) if isinstance(data_section, dict) else []
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
    )


def build_catalog(api_key: str) -> dict[str, Any]:
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
            datasets.append(_normalize_dataset(segments, metadata))

        time.sleep(SLEEP_SECONDS)

    datasets.sort(key=lambda d: d.path)
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "generated_at": generated_at,
        "dataset_count": len(datasets),
        "base_url": BASE_URL,
        "datasets": [asdict(dataset) for dataset in datasets],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Path to write the catalog JSON (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise CatalogError("EIA_API_KEY environment variable is required")

    catalog = build_catalog(api_key)

    output_path = args.output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(catalog, fp, indent=2)
        fp.write("\n")

    print(
        f"Wrote catalog with {catalog['dataset_count']} datasets to {output_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
