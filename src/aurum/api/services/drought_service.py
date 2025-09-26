from __future__ import annotations

"""Drought domain service façade -> forwards to v2 service implementation."""

from typing import Any, Dict, List, Optional


class DroughtService:
    async def list_indices(
        self,
        *,
        dataset: Optional[str],
        index: Optional[str],
        timescale: Optional[str],
        region: Optional[str],
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        from aurum.api.drought_v2_service import DroughtV2Service

        return await DroughtV2Service().list_indices(
            dataset=dataset,
            index=index,
            timescale=timescale,
            region=region,
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=limit,
        )

    async def list_usdm(
        self,
        *,
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        from aurum.api.drought_v2_service import DroughtV2Service

        return await DroughtV2Service().list_usdm(
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=limit,
        )
