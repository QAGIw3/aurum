from __future__ import annotations

"""Curves domain service façade.

Routers should depend on this façade; internally it forwards to the v2 curves
service implementation to keep the HTTP surface decoupled from storage.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple


class CurvesService:
    async def list_curves(self, *, offset: int, limit: int, name_filter: Optional[str] = None) -> List[Any]:
        from aurum.api.curves_v2_service import CurvesV2Service

        return await CurvesV2Service().list_curves(offset=offset, limit=limit, name_filter=name_filter)

    async def get_curve_diff(self, *, curve_id: str, from_timestamp: str, to_timestamp: str) -> Any:
        from aurum.api.curves_v2_service import CurvesV2Service

        return await CurvesV2Service().get_curve_diff(curve_id=curve_id, from_timestamp=from_timestamp, to_timestamp=to_timestamp)

    async def stream_curve_export(
        self,
        *,
        asof: Optional[str],
        iso: Optional[str],
        market: Optional[str],
        location: Optional[str],
        product: Optional[str],
        block: Optional[str],
        chunk_size: int = 1000,
    ):
        """Async generator yielding export rows for curves.

        Wraps the Trino streaming client behind a façade to decouple the router
        from the transport details.
        """
        from aurum.api.query import build_curve_export_query
        from aurum.api.database import get_trino_client

        query = build_curve_export_query(
            asof=asof,
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
        )
        client = get_trino_client()
        async for chunk in client.stream_query(query, chunk_size=chunk_size):
            for row in chunk:
                yield row
