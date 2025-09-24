from __future__ import annotations

"""Service layer for v2 Metadata endpoints.

Provides dimensions (via Trino), ISO locations (via reference registry),
canonical units (via units mapping), and calendars (via reference config).
"""

from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date as _date

from .config import CacheConfig, TrinoConfig
from .state import get_settings
from .service import query_dimensions
from ..reference import iso_locations as ref_iso
from ..reference import units as ref_units
from ..reference import calendars as ref_cal


class MetadataV2Service:
    async def list_dimensions(
        self,
        *,
        asof: Optional[str],
        offset: int,
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        cache_cfg = CacheConfig.from_settings(settings)
        asof_dt = None
        if asof:
            try:
                asof_dt = _date.fromisoformat(asof)
            except Exception:
                asof_dt = None
        values, _counts = query_dimensions(
            trino_cfg,
            cache_cfg,
            asof=asof_dt,
            asset_class=None,
            iso=None,
            location=None,
            market=None,
            product=None,
            block=None,
            tenor_type=None,
            per_dim_limit=1000,
            include_counts=False,
        )
        ordered_dims = ["asset_class", "iso", "location", "market", "product", "block", "tenor_type"]
        items = [
            {"dimension": dim, "values": list(values.get(dim, [])), "asof": asof or "latest"}
            for dim in ordered_dims
        ]
        total = len(items)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return items[start:end], total

    async def list_locations(
        self,
        *,
        iso: str,
        offset: int,
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        all_locs = list(ref_iso.iter_locations(iso))
        total = len(all_locs)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        page = all_locs[start:end]
        items: List[Dict[str, Any]] = []
        for loc in page:
            items.append(
                {
                    "iso": loc.iso,
                    "location_id": loc.location_id,
                    "name": loc.location_name,
                    "latitude": None,
                    "longitude": None,
                }
            )
        return items, total

    async def list_units(
        self,
        *,
        offset: int,
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        # Load mapping and produce unique canonical pairs
        mapping = ref_units.UnitsMapper._load_mapping(ref_units.DEFAULT_UNITS_PATH)
        pairs = []
        seen = set()
        for rec in mapping.values():
            key = (rec.currency or "", rec.per_unit or "")
            if key not in seen:
                seen.add(key)
                pairs.append({"currency": rec.currency, "per_unit": rec.per_unit})
        pairs.sort(key=lambda x: (x["currency"] or "", x["per_unit"] or ""))
        total = len(pairs)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return pairs[start:end], total

    async def list_calendars(
        self,
        *,
        offset: int,
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        calendars = ref_cal.get_calendars()
        entries: List[Dict[str, Any]] = []
        for name, cal in calendars.items():
            entries.append(
                {
                    "name": cal.name,
                    "timezone": cal.timezone,
                    "blocks": sorted(list(cal.blocks.keys())),
                }
            )
        entries.sort(key=lambda x: x["name"])  # stable order
        total = len(entries)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return entries[start:end], total


async def get_metadata_service() -> MetadataV2Service:
    return MetadataV2Service()

