from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from libs.storage.trino import TrinoAnalyticRepo
from libs.services.base_service import BaseTrinoService
from libs.services.query_builder import build_series_catalog_query


class CatalogService(BaseTrinoService):
    """Domain service for catalog series discovery queries."""

    def __init__(self, trino: Optional[TrinoAnalyticRepo] = None) -> None:
        super().__init__(trino=trino)

    async def list_series(
        self,
        *,
        tenant_id: str,
        filters: Dict[str, Any],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        # Use overfetch by 1 to detect has_more
        overfetch_limit = max(1, min(1000, limit) + 1)
        sql = build_series_catalog_query(
            tenant_id=tenant_id,
            filters=filters,
            limit=overfetch_limit,
            offset=offset,
        )
        rows = await self._trino.execute_query(sql)
        has_more = len(rows) > limit
        materialized = rows[:limit] if has_more else rows
        return materialized, has_more
