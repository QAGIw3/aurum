from __future__ import annotations

"""Service layer for v2 Drought endpoints."""

from datetime import date as _date
from typing import Any, Dict, List, Optional

from .config import TrinoConfig
from .state import get_settings
from .service import query_drought_indices, query_drought_usdm


class DroughtV2Service:
    async def list_indices(
        self,
        *,
        dataset: Optional[str],
        index: Optional[str],
        timescale: Optional[str],
        region: Optional[str],  # unused placeholder for future region sets
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)

        def _to_date(value: Optional[str]) -> Optional[_date]:
            if not value:
                return None
            try:
                return _date.fromisoformat(value)
            except Exception:
                return None

        # Over-fetch to emulate offset, then slice
        fetch_limit = max(1, int(offset) + int(limit))
        rows, _elapsed = query_drought_indices(
            trino_cfg,
            region_type=region_type,
            region_id=region_id,
            dataset=dataset,
            index_id=index,
            timescale=timescale,
            start_date=_to_date(start),
            end_date=_to_date(end),
            limit=fetch_limit,
        )
        return rows[int(offset) : int(offset) + int(limit)]

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
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)

        def _to_date(value: Optional[str]) -> Optional[_date]:
            if not value:
                return None
            try:
                return _date.fromisoformat(value)
            except Exception:
                return None

        fetch_limit = max(1, int(offset) + int(limit))
        rows, _elapsed = query_drought_usdm(
            trino_cfg,
            region_type=region_type,
            region_id=region_id,
            start_date=_to_date(start),
            end_date=_to_date(end),
            limit=fetch_limit,
        )
        return rows[int(offset) : int(offset) + int(limit)]


async def get_drought_service() -> DroughtV2Service:
    return DroughtV2Service()

