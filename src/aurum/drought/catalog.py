from __future__ import annotations

"""Catalog helpers for Drought.gov raster and vector datasets."""

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional

from datetime import date, datetime
from urllib.parse import urlparse

import logging
import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatencyWindow:
    """Represents documented latency windows for a feed."""

    minimum_days: int
    maximum_days: int

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LatencyWindow":
        return cls(minimum_days=int(payload.get("min", 0)), maximum_days=int(payload.get("max", 0)))


@dataclass(frozen=True)
class RasterIndexDataset:
    dataset: str
    provider: str
    description: str
    indices: List[str]
    timescales: List[str]
    discovery: Mapping[str, Any]
    assets: Mapping[str, Any]
    latency: LatencyWindow
    source_url: str
    license: Optional[str]
    update_frequency: Optional[str]
    notes: Optional[str] = None

    def info_url(self, index: str, timescale: str, valid_date: str) -> str:
        template = self.assets.get("info_template")
        if not template:
            raise ValueError(f"Dataset {self.dataset} does not define an info_template")
        return template.format(index=index.lower(), timescale=timescale.lower(), valid_date=valid_date)

    def geotiff_url(self, index: str, timescale: str, valid_date: str) -> str:
        template = self.assets.get("geotiff_template")
        if not template:
            raise ValueError(f"Dataset {self.dataset} does not define a geotiff_template")
        return template.format(index=index.lower(), timescale=timescale.lower(), valid_date=valid_date)

    def xyz_url(self, index: str, timescale: str, valid_date: str, z: int, x: int, y: int) -> str:
        template = self.assets.get("xyz_template")
        if not template:
            raise ValueError(f"Dataset {self.dataset} does not define an xyz_template")
        return template.format(index=index.lower(), timescale=timescale.lower(), valid_date=valid_date, z=z, x=x, y=y)

    def legend_url(self, index: str, timescale: str, valid_date: str) -> Optional[str]:
        template = self.assets.get("legend_template")
        if not template:
            return None
        return template.format(index=index.lower(), timescale=timescale.lower(), valid_date=valid_date)

    def zoom_levels(self) -> List[int]:
        levels = self.assets.get("zoom_levels")
        return list(levels) if isinstance(levels, Iterable) else []


@dataclass(frozen=True)
class VectorLayer:
    layer: str
    provider: str
    description: str
    update_frequency: Optional[str]
    formats: List[str]
    discovery: Mapping[str, Any]
    assets: Mapping[str, Any]
    latency: LatencyWindow
    notes: Optional[str] = None

    def asset_templates(self) -> Mapping[str, Any]:
        return self.assets


@dataclass(frozen=True)
class FoundationalBucket:
    name: str
    description: str
    gcs_path: str
    latency: LatencyWindow
    notes: Optional[str] = None


@dataclass(frozen=True)
class RegionSet:
    region_type: str
    description: str
    geometry_source: str
    crs: str


@dataclass
class DroughtCatalog:
    generated_at: str
    notes: Optional[str]
    raster_indices: List[RasterIndexDataset]
    vector_layers: List[VectorLayer]
    foundational_buckets: List[FoundationalBucket]
    region_sets: List[RegionSet]

    def raster_by_name(self, dataset: str) -> RasterIndexDataset:
        for item in self.raster_indices:
            if item.dataset == dataset:
                return item
        raise KeyError(f"Unknown raster dataset: {dataset}")

    def vector_by_name(self, layer: str) -> VectorLayer:
        for item in self.vector_layers:
            if item.layer == layer:
                return item
        raise KeyError(f"Unknown vector layer: {layer}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "notes": self.notes,
            "raster_indices": [dataclass_asdict(item) for item in self.raster_indices],
            "vector_layers": [dataclass_asdict(item) for item in self.vector_layers],
            "foundational_buckets": [dataclass_asdict(item) for item in self.foundational_buckets],
            "region_sets": [dataclass_asdict(item) for item in self.region_sets],
        }

    def fetch_raster_manifest(self, dataset: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
        """Download the manifest payload referenced by the catalog discovery block."""

        session = session or requests.Session()
        record = self.raster_by_name(dataset)
        table_url = record.discovery.get("table_url")
        if not table_url:
            raise ValueError(f"Dataset {dataset} missing discovery.table_url")

        logger.debug("Fetching raster manifest", extra={"dataset": dataset, "table_url": table_url})
        response = session.get(table_url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        entries = payload
        if isinstance(payload, Mapping):
            entries = payload.get("items") or payload.get("rows") or payload.get("data") or []
        if not isinstance(entries, list):
            raise ValueError(f"Unexpected manifest format for {dataset}: {type(entries)!r}")
        return entries

    def infer_valid_date(self, raw: Mapping[str, Any], dataset: str) -> date:
        record = self.raster_by_name(dataset)
        field = record.discovery.get("valid_date_field", "validDate")
        raw_value = raw.get(field)
        if not raw_value:
            raise ValueError(f"Manifest row missing {field!r} for dataset {dataset}")
        parsed = _parse_date(raw_value)
        if not parsed:
            raise ValueError(f"Could not parse valid date {raw_value!r} for dataset {dataset}")
        return parsed

    def asset_bundle(self, dataset: str, index: str, timescale: str, valid_date: date) -> Dict[str, Any]:
        record = self.raster_by_name(dataset)
        slug = valid_date.strftime("%Y-%m-%d")
        return {
            "info": record.info_url(index, timescale, slug),
            "geotiff": record.geotiff_url(index, timescale, slug),
            "legend": record.legend_url(index, timescale, slug),
            "xyz": {
                "template": record.assets.get("xyz_template"),
                "zoom_levels": record.zoom_levels(),
            },
        }

    def validate_source(self, url: str) -> None:
        """Ensure a URL is http(s) and points to an expected domain."""

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported scheme for catalog asset: {url}")


def dataclass_asdict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, LatencyWindow):
        return {"min": obj.minimum_days, "max": obj.maximum_days}
    if hasattr(obj, "__dict__"):
        result: Dict[str, Any] = {}
        for key, value in obj.__dict__.items():
            if isinstance(value, list):
                result[key] = [dataclass_asdict(item) for item in value]
            elif isinstance(value, LatencyWindow):
                result[key] = {"min": value.minimum_days, "max": value.maximum_days}
            else:
                result[key] = value
        return result
    raise TypeError(f"Cannot convert {type(obj)!r} to dict")


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value).date()
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def load_catalog(path: str | Path) -> DroughtCatalog:
    payload = json.loads(Path(path).read_text())
    raster = [
        RasterIndexDataset(
            dataset=item["dataset"],
            provider=item.get("provider", ""),
            description=item.get("description", ""),
            indices=list(item.get("indices", [])),
            timescales=list(item.get("timescales", [])),
            discovery=item.get("discovery", {}),
            assets=item.get("assets", {}),
            latency=LatencyWindow.from_mapping(item.get("latency_days", {})),
            source_url=item.get("source_url", ""),
            license=item.get("license"),
            update_frequency=item.get("update_frequency"),
            notes=item.get("notes"),
        )
        for item in payload.get("raster_indices", [])
    ]

    vectors = [
        VectorLayer(
            layer=item["layer"],
            provider=item.get("provider", ""),
            description=item.get("description", ""),
            update_frequency=item.get("update_frequency"),
            formats=list(item.get("formats", [])),
            discovery=item.get("discovery", {}),
            assets=item.get("assets", {}),
            latency=LatencyWindow.from_mapping(item.get("latency_days", {})),
            notes=item.get("notes"),
        )
        for item in payload.get("vector_layers", [])
    ]

    buckets = [
        FoundationalBucket(
            name=item["name"],
            description=item.get("description", ""),
            gcs_path=item.get("gcs_path", ""),
            latency=LatencyWindow.from_mapping(item.get("latency_days", {})),
            notes=item.get("notes"),
        )
        for item in payload.get("foundational_buckets", [])
    ]

    regions = [
        RegionSet(
            region_type=item["region_type"],
            description=item.get("description", ""),
            geometry_source=item.get("geometry_source", ""),
            crs=item.get("crs", "EPSG:4326"),
        )
        for item in payload.get("region_sets", [])
    ]

    return DroughtCatalog(
        generated_at=payload.get("generated_at", ""),
        notes=payload.get("notes"),
        raster_indices=raster,
        vector_layers=vectors,
        foundational_buckets=buckets,
        region_sets=regions,
    )


__all__ = [
    "DroughtCatalog",
    "RasterIndexDataset",
    "VectorLayer",
    "FoundationalBucket",
    "RegionSet",
    "load_catalog",
]
