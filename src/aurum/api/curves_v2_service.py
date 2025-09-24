from __future__ import annotations

"""Service layer for v2 Curves endpoints.

Thin orchestration over the DAO: resolves pagination inputs, calls the DAO, and
returns Pydantic models suitable for the v2 router.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .curves_v2_dao import list_curves as dao_list_curves
from .query import build_curve_diff_query
from .database.trino_client import get_trino_client
from datetime import date, datetime


class CurveItem(BaseModel):
    id: str = Field(..., description="Curve ID")
    name: str = Field(..., description="Curve name")
    description: Optional[str] = Field(None, description="Curve description")
    data_points: int = Field(..., description="Number of data points")
    created_at: Optional[str] = Field(None, description="Latest ingest timestamp")


class CurvesV2Service:
    async def list_curves(
        self,
        *,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
    ) -> List[CurveItem]:
        rows = await dao_list_curves(offset=offset, limit=limit, name_filter=name_filter)
        items: List[CurveItem] = []
        for row in rows:
            items.append(
                CurveItem(
                    id=str(row.get("id") or ""),
                    name=str(row.get("name") or ""),
                    description=None,
                    data_points=int(row.get("data_points") or 0),
                    created_at=str(row.get("created_at")) if row.get("created_at") is not None else None,
                )
            )
        return items

    async def get_curve_diff(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> CurveItem:
        # Parse timestamps to dates; accept YYYY-MM-DD or full ISO datetimes
        def _to_date(value: str) -> date:
            try:
                return date.fromisoformat(value)
            except Exception:
                dt = datetime.fromisoformat(value)
                return dt.date()

        asof_a = _to_date(from_timestamp)
        asof_b = _to_date(to_timestamp)

        # Build and execute a lightweight diff query
        query = build_curve_diff_query(
            asof_a=asof_a,
            asof_b=asof_b,
            curve_key=curve_id,
            asset_class=None,
            iso=None,
            location=None,
            market=None,
            product=None,
            block=None,
            tenor_type=None,
            limit=100,
            offset=0,
            cursor_after=None,
        )

        client = get_trino_client()
        rows = await client.execute_query(query)

        # Summarize diff into an item; clients can inspect meta for time bounds
        return CurveItem(
            id=curve_id,
            name=curve_id,
            description=f"Diff between {asof_a.isoformat()} and {asof_b.isoformat()}",
            data_points=len(rows or []),
            created_at=None,
        )


async def get_curve_service() -> CurvesV2Service:
    """Factory for the v2 Curves service."""
    return CurvesV2Service()
