"""Curves Data Access Object - handles curve data operations."""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aurum.core import AurumSettings

from ..contracts import (
    CurvesDiffQuery,
    CurvesQuery,
    Pagination,
    QueryContext,
    QueryResult,
)
from ..database.trino_client import get_trino_client
from ..query import build_curve_diff_query, build_curve_query, build_filter_clause
from ..deps import get_settings  # FastAPI dependency (used only when DAO is used within request context)

LOGGER = logging.getLogger(__name__)


class CurvesDao:
    """Data Access Object for curves operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        # Prefer explicit settings passed by service; fall back to core settings getter
        if settings is not None:
            self._settings = settings
        else:
            try:
                # When used inside a FastAPI request, services should pass settings explicitly.
                # As a very last resort for backwards compatibility in non-request contexts,
                # use core settings which must be pre-configured by startup.
                from aurum.api.state import get_settings as _api_get_settings
                self._settings = _api_get_settings()
            except Exception:
                from aurum.core.settings import AurumSettings as _AurumSettings
                self._settings = _AurumSettings.from_env()
        self._trino_client = get_trino_client()

    async def _execute(
        self,
        sql: str,
        params: Dict[str, Any],
        context: Optional[QueryContext],
    ) -> tuple[float, List[Dict[str, Any]]]:
        start = time.perf_counter()
        rows = await self._trino_client.execute_query(sql, params, use_cache=True)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, [dict(row) for row in rows]

    async def query_curves(
        self,
        *,
        query: CurvesQuery,
        pagination: Pagination,
        context: QueryContext | None = None,
    ) -> QueryResult[List[Dict[str, Any]]]:
        sql = build_curve_query(
            asof=query.asof,
            curve_key=query.curve_key,
            asset_class=query.asset_class,
            iso=query.iso,
            location=query.location,
            market=query.market,
            product=query.product,
            block=query.block,
            tenor_type=query.tenor_type,
            limit=pagination.limit,
            offset=pagination.offset,
            cursor_after=(
                pagination.cursor_after.as_params() if pagination.cursor_after else None
            ),
            cursor_before=(
                pagination.cursor_before.as_params() if pagination.cursor_before else None
            ),
            descending=pagination.descending,
        )
        params = query.as_params()
        elapsed, rows = await self._execute(sql, params, context)
        return QueryResult(data=rows, elapsed_ms=elapsed, raw_query=sql)

    async def list_catalog_entries(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], float, Optional[str]]:
        sql_parts = [
            "SELECT id, name, description, data_points, created_at",
            "FROM iceberg.market.curves_catalog",
            "WHERE tenant_id = :tenant_id",
        ]
        params: Dict[str, Any] = {"tenant_id": tenant_id, "offset": offset, "limit": limit}
        if name_filter:
            sql_parts.append("AND name ILIKE :name_filter")
            params["name_filter"] = f"%{name_filter}%"
        sql_parts.append("ORDER BY name OFFSET :offset LIMIT :limit")
        query = "\n".join(sql_parts)
        elapsed_ms, rows = await self._execute(query, params, None)
        return rows, elapsed_ms, query

    async def query_curves_diff(
        self,
        *,
        query: CurvesDiffQuery,
        pagination: Pagination,
        context: QueryContext | None = None,
    ) -> QueryResult[List[Dict[str, Any]]]:
        sql = build_curve_diff_query(
            asof_a=query.asof_a,
            asof_b=query.asof_b,
            curve_key=query.curve_key,
            asset_class=query.asset_class,
            iso=query.iso,
            location=query.location,
            market=query.market,
            product=query.product,
            block=query.block,
            tenor_type=query.tenor_type,
            limit=pagination.limit,
            offset=pagination.offset,
            cursor_after=(
                pagination.cursor_after.as_params() if pagination.cursor_after else None
            ),
        )
        params = query.as_params()
        elapsed, rows = await self._execute(sql, params, context)
        return QueryResult(data=rows, elapsed_ms=elapsed, raw_query=sql)

    async def get_curve_diff_details(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> Tuple[Dict[str, Any], float, Optional[str]]:
        sql = (
            "SELECT id, name, description, data_points, created_at "
            "FROM iceberg.market.curves_diff WHERE id = :curve_id "
            "AND from_ts = :from_ts AND to_ts = :to_ts"
        )
        params = {"curve_id": curve_id, "from_ts": from_timestamp, "to_ts": to_timestamp}
        elapsed_ms, rows = await self._execute(sql, params, None)
        return (rows[0] if rows else {}), elapsed_ms, sql

    async def query_curve_strips(
        self,
        *,
        query: CurvesQuery,
        pagination: Pagination,
        context: QueryContext | None = None,
    ) -> QueryResult[List[Dict[str, Any]]]:
        base = "iceberg.market.curve_observation"
        filters = query.as_params()
        where_clause = build_filter_clause(filters)
        if where_clause:
            where_clause = where_clause.replace("WHERE", "WHERE")
        group_by = "GROUP BY iso, market, location, product, block, strip, asof"
        order_clause = "ORDER BY iso, market, location, product, strip"
        offset_clause = f"OFFSET {max(0, pagination.offset)}"
        limit_clause = f"LIMIT {max(1, pagination.limit)}"
        sql = (
            "SELECT iso, market, location, product, block, strip, asof, "
            "AVG(forward_value) AS avg_value, "
            "MIN(forward_value) AS min_value, "
            "MAX(forward_value) AS max_value, "
            "COUNT(*) AS point_count, "
            "MIN(forward_date) AS first_date, "
            "MAX(forward_date) AS last_date "
            f"FROM {base} {where_clause} {group_by} {order_clause} "
            f"{offset_clause} {limit_clause}"
        )
        elapsed, rows = await self._execute(sql, filters, context)
        return QueryResult(data=rows, elapsed_ms=elapsed, raw_query=sql)

    async def export_curves(
        self,
        *,
        asof: Optional[str] = None,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        where: List[str] = []
        params: Dict[str, Any] = {}
        if asof:
            where.append("asof_date = DATE :asof")
            params["asof"] = asof
        if iso:
            where.append("iso = :iso")
            params["iso"] = iso
        if market:
            where.append("market = :market")
            params["market"] = market
        if location:
            where.append("location = :location")
            params["location"] = location
        if product:
            where.append("product = :product")
            params["product"] = product
        if block:
            where.append("block = :block")
            params["block"] = block

        predicate = f"WHERE {' AND '.join(where)}" if where else ""
        query = (
            "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
            f"FROM iceberg.market.curves_export {predicate} ORDER BY curve_key, tenor_label, asof_date"
        )
        _, rows = await self._execute(query, params, None)
        return rows

    async def invalidate_curve_cache(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Dict[str, int]:
        """Invalidate curve cache."""
        from ..cache.consolidated_manager import get_unified_cache_manager
        from ..container import get_service

        cache_manager = get_unified_cache_manager()

        # Build cache key pattern based on parameters
        key_pattern = "curves:"

        if iso:
            key_pattern += f"{hash(iso)}:"
        else:
            key_pattern += "*:"

        if market:
            key_pattern += f"{hash(market)}:"
        else:
            key_pattern += "*:"

        if location:
            key_pattern += f"{hash(location)}"
        else:
            key_pattern += "*"

        # Delete matching cache keys
        deleted_count = await cache_manager.delete_pattern(key_pattern)

        LOGGER.info(
            "Invalidated curve cache",
            extra={
                "iso": iso,
                "market": market,
                "location": location,
                "pattern": key_pattern,
                "deleted_keys": deleted_count,
            }
        )

        return {"curves": deleted_count}
