from __future__ import annotations

"""ISO domain service façade -> forwards to v2 service implementation."""

from typing import Any, Dict, List


class IsoService:
    async def lmp_last_24h(self, *, iso: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service

        return await IsoV2Service().lmp_last_24h(iso=iso, offset=offset, limit=limit)

    async def lmp_hourly(self, *, iso: str, date: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service

        return await IsoV2Service().lmp_hourly(iso=iso, date=date, offset=offset, limit=limit)

    async def lmp_daily(self, *, iso: str, date: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service

        return await IsoV2Service().lmp_daily(iso=iso, date=date, offset=offset, limit=limit)

