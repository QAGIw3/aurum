"""ISO location registry utilities for LMP enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Optional

import csv


REGISTRY_PATH = Path(__file__).resolve().parents[3] / "config" / "iso_nodes.csv"


@dataclass(frozen=True)
class IsoLocation:
    iso: str
    location_id: str
    location_name: str
    location_type: str
    zone: Optional[str]
    hub: Optional[str]
    timezone: Optional[str]


def _normalise_key(iso: str, location_id: str) -> str:
    return f"{iso.strip().upper()}::{location_id.strip().upper()}"


@lru_cache(maxsize=1)
def _load_registry(path: Path = REGISTRY_PATH) -> Dict[str, IsoLocation]:
    if not path.exists():
        raise FileNotFoundError(f"ISO location registry not found at {path}")

    registry: Dict[str, IsoLocation] = {}
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {"iso", "location_id", "location_name", "location_type"}
        if not required.issubset(reader.fieldnames or {}):
            missing = required - set(reader.fieldnames or [])
            raise ValueError(f"ISO location registry missing columns: {sorted(missing)}")

        for row in reader:
            iso = (row.get("iso") or "").strip()
            loc_id = (row.get("location_id") or "").strip()
            if not iso or not loc_id:
                continue
            key = _normalise_key(iso, loc_id)
            registry[key] = IsoLocation(
                iso=iso,
                location_id=loc_id,
                location_name=(row.get("location_name") or "").strip(),
                location_type=(row.get("location_type") or "").strip().upper(),
                zone=(row.get("zone") or None),
                hub=(row.get("hub") or None),
                timezone=(row.get("timezone") or None),
            )
    return registry


def get_location(iso: str, location_id: str) -> Optional[IsoLocation]:
    """Return the location metadata for the given ISO and node id."""
    if not iso or not location_id:
        return None
    registry = _load_registry()
    return registry.get(_normalise_key(iso, location_id))


def iter_locations(iso: Optional[str] = None) -> Iterable[IsoLocation]:
    """Iterate through known locations, optionally filtering by ISO."""
    registry = _load_registry()
    if iso:
        prefix = iso.strip().upper() + "::"
        for key, location in registry.items():
            if key.startswith(prefix):
                yield location
    else:
        yield from registry.values()


__all__ = ["IsoLocation", "get_location", "iter_locations"]

