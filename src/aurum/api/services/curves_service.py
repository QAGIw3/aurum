from __future__ import annotations

"""Curves domain service with DAO pattern implementation.

Phase 1.3 Service Layer Decomposition: Extracted from monolithic service.py
Provides clean business logic layer with data access through DAO pattern.
"""

import hashlib
import json
import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional, AsyncGenerator, Tuple

from aurum.telemetry import get_tracer

from .base_service import QueryableServiceInterface, ExportableServiceInterface
from ..dao.curves_dao import CurvesDao
from ..config import CacheConfig, TrinoConfig
from ..cache.global_manager import get_global_cache_manager
from ..cache.utils import cache_get_sync, cache_set_sync, cache_get_or_set_sync
from ..query import build_curve_query, build_curve_diff_query

LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as _PromCounter
except Exception:  # pragma: no cover - metrics optional
    _PromCounter = None  # type: ignore[assignment]

if _PromCounter:
    SERVICE_CACHE_COUNTER = _PromCounter(
        "aurum_service_cache_operations_total",
        "Service cache operations",
        ["endpoint", "event"]
    )
else:
    SERVICE_CACHE_COUNTER = None

def _cache_key(params: Dict[str, Any]) -> str:
    """Generate cache key from parameters."""
    normalized = {}
    for k, v in params.items():
        if v is not None:
            if isinstance(v, date):
                normalized[k] = v.isoformat()
            else:
                normalized[k] = str(v)
    
    data = json.dumps(normalized, sort_keys=True)
    return hashlib.md5(data.encode("utf-8")).hexdigest()

def _execute_trino_query(trino_cfg: TrinoConfig, sql: str) -> List[Dict[str, Any]]:
    """Execute Trino query - simplified implementation for migration."""
    # This would normally execute the actual query
    # For now, return empty result to maintain structure
    return []


class CurvesService(QueryableServiceInterface, ExportableServiceInterface):
    """Curves domain service implementing business logic and data access through DAO.
    
    Handles curve observations, diffs, and strips with caching and export capabilities.
    """
    
    def __init__(self):
        self._dao = CurvesDao()

    async def list_curves(
        self, 
        *, 
        offset: int, 
        limit: int, 
        name_filter: Optional[str] = None
    ) -> List[Any]:
        """List curves with optional name filtering."""
        # For backward compatibility, delegate to query_data
        filters = {"iso": name_filter} if name_filter else None
        return await self.query_data(offset=offset, limit=limit, filters=filters)

    async def get_curve_diff(
        self, 
        *, 
        curve_id: str, 
        from_timestamp: str, 
        to_timestamp: str
    ) -> Any:
        """Get curve diff between two timestamps."""
        # Parse curve_id for filtering (simplified implementation)
        filters = {"iso": curve_id.split(":")[0] if ":" in curve_id else None}
        
        results = await self._dao.query_curves_diff(
            iso=filters.get("iso"),
            from_asof=from_timestamp,
            to_asof=to_timestamp,
            limit=1000
        )
        
        return {"diff_data": results, "from_asof": from_timestamp, "to_asof": to_timestamp}

    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query curve data with pagination and filtering (QueryableServiceInterface)."""
        if filters is None:
            filters = {}
            
        return await self._dao.query_curves(
            iso=filters.get("iso"),
            market=filters.get("market"),
            location=filters.get("location"),
            product=filters.get("product"),
            block=filters.get("block"),
            asof=filters.get("asof"),
            strip=filters.get("strip"),
            offset=offset,
            limit=limit,
        )

    async def export_data(
        self,
        *,
        format: str = "json",
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Export curve data in specified format (ExportableServiceInterface)."""
        if filters is None:
            filters = {}
        
        offset = 0
        
        while True:
            chunk = await self._dao.query_curves(
                iso=filters.get("iso"),
                market=filters.get("market"),
                location=filters.get("location"),
                product=filters.get("product"),
                block=filters.get("block"),
                asof=filters.get("asof"),
                strip=filters.get("strip"),
                offset=offset,
                limit=chunk_size,
            )
            
            if not chunk:
                break
                
            if format == "json":
                for row in chunk:
                    yield row
            else:
                # For other formats, yield as batch
                yield {"format": format, "data": chunk, "offset": offset}
            
            offset += chunk_size
            
            # Safety break for large datasets
            if len(chunk) < chunk_size:
                break

    async def stream_curve_export(
        self,
        *,
        asof: Optional[str] = None,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Async generator yielding export rows for curves (legacy method)."""
        filters = {
            "asof": asof,
            "iso": iso,
            "market": market,
            "location": location,
            "product": product,
            "block": block,
        }
        
        async for item in self.export_data(filters=filters, chunk_size=chunk_size):
            yield item

    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate curve-related caches (ServiceInterface)."""
        return await self._dao.invalidate_curve_cache()

    def query_curves(
        self,
        trino_cfg: TrinoConfig,
        cache_cfg: CacheConfig,
        *,
        asof: Optional[date],
        curve_key: Optional[str],
        asset_class: Optional[str],
        iso: Optional[str],
        location: Optional[str],
        market: Optional[str],
        product: Optional[str],
        block: Optional[str],
        tenor_type: Optional[str],
        limit: int,
        offset: int = 0,
        cursor_after: Optional[Dict[str, Any]] = None,
        cursor_before: Optional[Dict[str, Any]] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query curves with caching and performance tracking."""
        params = {
            "asof": asof,
            "curve_key": curve_key,
            "asset_class": asset_class,
            "iso": iso,
            "location": location,
            "market": market,
            "product": product,
            "block": block,
            "tenor_type": tenor_type,
            "limit": limit,
            "offset": offset,
            "cursor_after": cursor_after,
            "cursor_before": cursor_before,
            "descending": descending,
        }
        sql = build_curve_query(
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=limit,
            offset=offset,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )

        # try cache via CacheManager
        manager = get_global_cache_manager()
        cache_key = f"curves:{_cache_key({**params, 'sql': sql})}"
        if manager is not None:
            cached_rows = cache_get_sync(manager, cache_key)
            if cached_rows is not None:
                if SERVICE_CACHE_COUNTER:
                    try:
                        SERVICE_CACHE_COUNTER.labels(endpoint="curves", event="hit").inc()
                    except Exception:
                        pass
                return cached_rows, 0.0

        effective_limit = max(int(limit), 0)
        effective_offset = max(int(offset), 0)

        tracer = get_tracer("aurum.api.service")
        with tracer.start_as_current_span("query_curves.execute") as span:
            span.set_attribute("aurum.curves.limit", effective_limit)
            span.set_attribute("aurum.curves.offset", effective_offset)
            span.set_attribute("aurum.curves.has_cursor", bool(cursor_after or cursor_before))
            start = time.perf_counter()
            rows = _execute_trino_query(trino_cfg, sql)
            elapsed = (time.perf_counter() - start) * 1000.0
            span.set_attribute("aurum.curves.elapsed_ms", elapsed)
            span.set_attribute("aurum.curves.row_count", len(rows))

        if manager is not None:
            try:
                cache_set_sync(manager, cache_key, rows, cache_cfg.ttl_seconds)
                if SERVICE_CACHE_COUNTER:
                    try:
                        SERVICE_CACHE_COUNTER.labels(endpoint="curves", event="miss").inc()
                    except Exception:
                        pass
            except Exception:
                LOGGER.debug("CacheManager set failed for curves", exc_info=True)

        return rows, elapsed

    def query_curves_diff(
        self,
        trino_cfg: TrinoConfig,
        cache_cfg: CacheConfig,
        *,
        asof_a: date,
        asof_b: date,
        curve_key: Optional[str],
        asset_class: Optional[str],
        iso: Optional[str],
        location: Optional[str],
        market: Optional[str],
        product: Optional[str],
        block: Optional[str],
        tenor_type: Optional[str],
        limit: int,
        offset: int = 0,
        cursor_after: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query curve differences between two dates with caching."""
        params = {
            "asof_a": asof_a,
            "asof_b": asof_b,
            "curve_key": curve_key,
            "asset_class": asset_class,
            "iso": iso,
            "location": location,
            "market": market,
            "product": product,
            "block": block,
            "tenor_type": tenor_type,
            "limit": limit,
            "offset": offset,
            "cursor_after": cursor_after,
        }
        sql = build_curve_diff_query(
            asof_a=asof_a,
            asof_b=asof_b,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=limit,
            offset=offset,
            cursor_after=cursor_after,
        )

        prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
        key = f"{prefix}curves-diff:{_cache_key({**params, 'sql': sql})}"
        start = time.perf_counter()
        manager = get_global_cache_manager()
        rows: List[Dict[str, Any]] = cache_get_or_set_sync(
            manager,
            key,
            lambda: _execute_trino_query(trino_cfg, sql),
            cache_cfg.ttl_seconds,
        )
        # Normalize fields
        for row in rows:
            row.pop("tenant_id", None)
            attribution_val = row.get("attribution")
            if isinstance(attribution_val, str):
                try:
                    row["attribution"] = json.loads(attribution_val)
                except json.JSONDecodeError:
                    row["attribution"] = None
        elapsed = (time.perf_counter() - start) * 1000.0
        
        return rows, elapsed

    async def invalidate_curve_cache(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
    ) -> None:
        """Invalidate curve cache using CacheManager."""
        from ..cache.cache import CacheManager
        from ..container import get_service

        cache_manager = get_service(CacheManager)

        # Build cache key pattern based on parameters
        key_pattern = "curves:"

        if iso:
            key_pattern += f"{hash(iso)}:"
        else:
            key_pattern += "*:"  # Wildcard for any ISO

        if market:
            key_pattern += f"{hash(market)}:"
        else:
            key_pattern += "*:"  # Wildcard for any market

        if location:
            key_pattern += f"{hash(location)}"
        else:
            key_pattern += "*"  # Wildcard for any location

        # Delete matching cache keys
        deleted_count = await cache_manager.delete_pattern(key_pattern)

        LOGGER.info(
            "Invalidated curve cache",
            extra={
                "iso": iso,
                "market": market,
                "location": location,
                "deleted_keys": deleted_count,
            }
        )
