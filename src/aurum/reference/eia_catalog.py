"""Helpers for loading the harvested EIA dataset catalog."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import json


CATALOG_PATH = Path(__file__).resolve().parents[3] / "config" / "eia_catalog.json"


@dataclass(frozen=True)
class EiaDataset:
    """Lightweight view of an EIA dataset definition."""

    path: str
    name: str | None
    description: str | None
    frequencies: tuple[dict[str, Any], ...]
    facets: tuple[dict[str, Any], ...]
    data_columns: tuple[str, ...]
    start_period: str | None
    end_period: str | None
    default_frequency: str | None
    default_date_format: str | None


class CatalogNotFoundError(FileNotFoundError):
    """Raised when the catalog JSON cannot be located."""


class DatasetNotFoundError(LookupError):
    """Raised when a dataset path is absent from the catalog."""


def _load_catalog_dict(path: Path | None = None) -> dict[str, Any]:
    catalog_path = Path(path) if path else CATALOG_PATH
    if not catalog_path.exists():
        raise CatalogNotFoundError(f"EIA catalog not found at {catalog_path}")
    data = json.loads(catalog_path.read_text(encoding="utf-8"))
    return data


@lru_cache(maxsize=1)
def load_catalog(path: Path | None = None) -> dict[str, Any]:
    """Load the catalog JSON, caching the result for subsequent lookups."""

    return _load_catalog_dict(path)


def iter_datasets(path: Path | None = None) -> Iterable[EiaDataset]:
    """Yield :class:`EiaDataset` entries from the catalog."""

    catalog = load_catalog(path)
    for dataset in catalog.get("datasets", []):
        yield EiaDataset(
            path=dataset["path"],
            name=dataset.get("name"),
            description=dataset.get("description"),
            frequencies=tuple(dataset.get("frequencies", [])),
            facets=tuple(dataset.get("facets", [])),
            data_columns=tuple(dataset.get("data_columns", [])),
            start_period=dataset.get("start_period"),
            end_period=dataset.get("end_period"),
            default_frequency=dataset.get("default_frequency"),
            default_date_format=dataset.get("default_date_format"),
        )


def get_dataset(dataset_path: str, *, catalog_path: Path | None = None) -> EiaDataset:
    """Return a single dataset entry by path."""

    for dataset in iter_datasets(catalog_path):
        if dataset.path == dataset_path:
            return dataset
    raise DatasetNotFoundError(dataset_path)
