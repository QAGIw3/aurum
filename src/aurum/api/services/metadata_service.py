from __future__ import annotations

"""Metadata domain service façade.

Routers should depend on this façade rather than importing module-level
services directly. Internally this forwards to the v2 service implementation.
"""

from typing import Any, Dict, List, Tuple


class MetadataService:
    async def list_dimensions(self, *, asof: str | None, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        from aurum.api.metadata_v2_service import MetadataV2Service

        return await MetadataV2Service().list_dimensions(asof=asof, offset=offset, limit=limit)

    async def list_locations(self, *, iso: str, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        from aurum.api.metadata_v2_service import MetadataV2Service

        return await MetadataV2Service().list_locations(iso=iso, offset=offset, limit=limit)

    async def list_units(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        from aurum.api.metadata_v2_service import MetadataV2Service

        return await MetadataV2Service().list_units(offset=offset, limit=limit)

    async def list_calendars(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        from aurum.api.metadata_v2_service import MetadataV2Service

        return await MetadataV2Service().list_calendars(offset=offset, limit=limit)
