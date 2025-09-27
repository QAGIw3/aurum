from __future__ import annotations

"""Drought domain service backed by DroughtDao."""

from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

from ..config import TrinoConfig
from ..dao.drought_dao import DroughtDao


class DroughtService:
    """Orchestrates drought queries with domain-specific defaults."""

    def __init__(self, dao: Optional[DroughtDao] = None) -> None:
        self._dao = dao or DroughtDao()

    async def query_indices(
        self,
        *,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[list[Dict[str, Any]], float]:
        return await self._dao.query_indices(
            trino_cfg=trino_cfg,
            region_type=region_type,
            region_id=region_id,
            dataset=dataset,
            index_id=index_id,
            timescale=timescale,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    async def query_usdm(
        self,
        *,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[list[Dict[str, Any]], float]:
        return await self._dao.query_usdm(
            trino_cfg=trino_cfg,
            region_type=region_type,
            region_id=region_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    async def query_vector_events(
        self,
        *,
        layer: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[list[Dict[str, Any]], float]:
        return await self._dao.query_vector_events(
            trino_cfg=trino_cfg,
            layer=layer,
            region_type=region_type,
            region_id=region_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )


__all__ = ["DroughtService"]
