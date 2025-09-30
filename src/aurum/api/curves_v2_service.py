from __future__ import annotations

"""Service layer for v2 Curves endpoints.

Thin orchestration over the DAO: resolves pagination inputs, calls the DAO, and
returns Pydantic models suitable for the v2 router.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from pydantic import BaseModel, Field

from datetime import date, datetime

from aurum.data import QueryResult as BackendQueryResult

from .curves_v2_dao import list_curves as dao_list_curves
from .database.trino_client import get_trino_client
from .query import build_curve_diff_query


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
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
        include_debug: bool = False,
    ) -> Tuple[List[CurveItem], Optional[Dict[str, Any]]]:
        """List curves with optional debug metadata from the data backend."""

        result = await dao_list_curves(
            tenant_id=tenant_id,
            offset=offset,
            limit=limit,
            name_filter=name_filter,
        )

        rows: Sequence[Dict[str, Any]]
        debug_meta: Dict[str, Any] = {}

        if isinstance(result, BackendQueryResult):
            columns = result.columns or []
            debug_meta = dict(result.metadata or {})
            rows = []
            for row in result.rows:
                if columns:
                    row_dict = {col: row[idx] for idx, col in enumerate(columns) if idx < len(row)}
                else:
                    row_dict = {str(idx): value for idx, value in enumerate(row)}
                rows.append(row_dict)
        else:
            rows = result  # type: ignore[assignment]

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

        return items, (debug_meta if include_debug else None)

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
