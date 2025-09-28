from __future__ import annotations

"""Curves domain service with DAO pattern implementation.

Phase 1.3 Service Layer Decomposition: Extracted from monolithic service.py
Provides clean business logic layer with data access through DAO pattern.
"""

import asyncio
import json
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aurum.telemetry import get_tracer

from ..cache.unified_cache_manager import CacheNamespace, UnifiedCacheManager, get_unified_cache_manager
from ..contracts import (
    CacheDirective,
    CacheStatus,
    CurvesDiffQuery,
    CurvesQuery,
    KeysetCursor,
    Pagination,
    QueryContext,
    ServiceCallContext,
    ServiceExecutionMetadata,
    ServiceExecutionResult,
)
from ..dao.curves_dao import CurvesDao
from ..logging.structured_logger import get_logger
from .base_service import ExportableServiceInterface, QueryableServiceInterface

logger = get_logger(__name__)


class CurvesService(QueryableServiceInterface, ExportableServiceInterface[Dict[str, Any]]):
    """Curves domain service backed by CurvesDao and unified cache manager."""

    def __init__(self) -> None:
        self._dao = CurvesDao()
        self._cache_manager: Optional[UnifiedCacheManager] = None

    # ------------------------------------------------------------------
    # Contract helpers
    # ------------------------------------------------------------------

    def _resolve_cache_manager(self) -> Optional[UnifiedCacheManager]:
        if self._cache_manager is None:
            try:
                self._cache_manager = get_unified_cache_manager()
            except Exception:  # pragma: no cover - defensive
                self._cache_manager = None
        return self._cache_manager

    def _build_pagination(self, pagination: Optional[Pagination], *, default_limit: int = 100) -> Pagination:
        if pagination is None:
            return Pagination(limit=default_limit, offset=0)
        return Pagination(
            limit=max(1, pagination.limit),
            offset=max(0, pagination.offset),
            cursor_after=pagination.cursor_after,
            cursor_before=pagination.cursor_before,
            descending=pagination.descending,
            overfetch=pagination.overfetch,
        )

    def _build_query_context(self, context: Optional[ServiceCallContext]) -> QueryContext:
        if context is None:
            return QueryContext()
        return QueryContext(
            trace_id=context.trace_id,
            span_name=context.span_name,
            tenant_id=context.tenant_id,
            timeout_seconds=None,
            extra=context.extra,
        )

    def _build_curves_query(self, filters: Optional[Dict[str, Any]]) -> CurvesQuery:
        filters = filters or {}
        return CurvesQuery(
            asof=filters.get("asof"),
            curve_key=filters.get("curve_key"),
            asset_class=filters.get("asset_class"),
            iso=filters.get("iso"),
            location=filters.get("location"),
            market=filters.get("market"),
            product=filters.get("product"),
            block=filters.get("block"),
            tenor_type=filters.get("tenor_type"),
        )

    def _curve_cache_key(self, *, query: CurvesQuery, pagination: Pagination) -> str:
        payload = {**query.as_params(), "limit": pagination.limit, "offset": pagination.offset}
        if pagination.cursor_after:
            payload["cursor_after"] = pagination.cursor_after.as_params()
        if pagination.cursor_before:
            payload["cursor_before"] = pagination.cursor_before.as_params()
        if pagination.descending:
            payload["descending"] = True
        return json.dumps(payload, sort_keys=True, default=str)

    def _curve_diff_cache_key(self, *, query: CurvesDiffQuery, pagination: Pagination) -> str:
        payload = {**query.as_params(), "limit": pagination.limit, "offset": pagination.offset}
        return json.dumps(payload, sort_keys=True, default=str)

    async def _cache_get(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        manager = self._resolve_cache_manager()
        if manager is None:
            return None
        namespaced_key = CacheNamespace.CURVES.value + ":" + cache_key
        return await manager.get(namespaced_key, CacheNamespace.CURVES)

    async def _cache_set(self, cache_key: str, payload: List[Dict[str, Any]], ttl: int) -> None:
        if ttl <= 0:
            return
        manager = self._resolve_cache_manager()
        if manager is None:
            return
        namespaced_key = CacheNamespace.CURVES.value + ":" + cache_key
        await manager.set(namespaced_key, payload, ttl=ttl, namespace=CacheNamespace.CURVES)

    def _run_sync(self, coro):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            new_loop = asyncio.new_event_loop()
            try:
                return new_loop.run_until_complete(coro)
            finally:
                new_loop.close()
        return asyncio.run(coro)

    # ------------------------------------------------------------------
    # QueryableServiceInterface
    # ------------------------------------------------------------------

    async def query_data(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        pagination: Optional[Pagination] = None,
        context: Optional[ServiceCallContext] = None,
    ) -> ServiceExecutionResult[List[Dict[str, Any]]]:
        curves_query = self._build_curves_query(filters)
        page = self._build_pagination(pagination)
        query_context = self._build_query_context(context)

        manager = self._resolve_cache_manager()
        cache_directive = context.cache_directive if context and context.cache_directive else None
        allow_bypass = cache_directive.allow_bypass if cache_directive else False
        ttl_seconds = cache_directive.ttl_seconds if cache_directive else (manager.config.ttl_seconds if manager and getattr(manager, "config", None) else 0)
        cache_key = self._curve_cache_key(query=curves_query, pagination=page)
        cache_status = CacheStatus.BYPASS

        if manager and not allow_bypass:
            cached_rows = await self._cache_get(cache_key)
            if cached_rows is not None:
                metadata = ServiceExecutionMetadata(
                    elapsed_ms=0.0,
                    cache_status=CacheStatus.HIT,
                    cache_key=cache_key,
                    cache_version=cache_directive.version if cache_directive else None,
                    row_count=len(cached_rows),
                )
                return ServiceExecutionResult(data=cached_rows, metadata=metadata)

        tracer = get_tracer("aurum.api.curves")
        with tracer.start_as_current_span("curves.list") as span:
            span.set_attribute("aurum.curves.limit", page.limit)
            span.set_attribute("aurum.curves.offset", page.offset)
            span.set_attribute("aurum.curves.has_cursor", bool(page.cursor_after or page.cursor_before))
            dao_result = await self._dao.query_curves(
                query=curves_query,
                pagination=page,
                context=query_context,
            )

        if manager and ttl_seconds > 0 and dao_result.data:
            try:
                await self._cache_set(cache_key, dao_result.data, ttl=ttl_seconds)
                cache_status = CacheStatus.MISS
            except Exception:
                logger.debug("Failed to populate curves cache", exc_info=True)
                cache_status = CacheStatus.BYPASS

        metadata = ServiceExecutionMetadata(
            elapsed_ms=dao_result.elapsed_ms,
            cache_status=cache_status,
            cache_key=cache_key if cache_directive or (manager and ttl_seconds > 0) else None,
            cache_version=cache_directive.version if cache_directive else None,
            backend=getattr(self._dao._trino_client, "backend_label", None),
            row_count=len(dao_result.data),
        )
        return ServiceExecutionResult(data=dao_result.data, metadata=metadata)

    async def query_diff(
        self,
        *,
        diff: CurvesDiffQuery,
        pagination: Optional[Pagination] = None,
        context: Optional[ServiceCallContext] = None,
    ) -> ServiceExecutionResult[List[Dict[str, Any]]]:
        page = self._build_pagination(pagination)
        query_context = self._build_query_context(context)

        manager = self._resolve_cache_manager()
        cache_directive = context.cache_directive if context and context.cache_directive else None
        allow_bypass = cache_directive.allow_bypass if cache_directive else False
        ttl_seconds = (
            cache_directive.ttl_seconds
            if cache_directive
            else (manager.config.ttl_seconds if manager else 0)
        )
        cache_key = self._curve_diff_cache_key(query=diff, pagination=page)
        cache_status = CacheStatus.BYPASS

        if manager and not allow_bypass:
            cached_rows = await self._cache_get(cache_key)
            if cached_rows is not None:
                metadata = ServiceExecutionMetadata(
                    elapsed_ms=0.0,
                    cache_status=CacheStatus.HIT,
                    cache_key=cache_key,
                    cache_version=cache_directive.version if cache_directive else None,
                    row_count=len(cached_rows),
                )
                return ServiceExecutionResult(data=cached_rows, metadata=metadata)

        tracer = get_tracer("aurum.api.curves")
        with tracer.start_as_current_span("curves.diff") as span:
            dao_result = await self._dao.query_curves_diff(
                query=diff,
                pagination=page,
                context=query_context,
            )

        if manager and ttl_seconds > 0 and dao_result.data:
            try:
                await self._cache_set(cache_key, dao_result.data, ttl=ttl_seconds)
                cache_status = CacheStatus.MISS
            except Exception:
                logger.debug("Failed to populate curves diff cache", exc_info=True)
                cache_status = CacheStatus.BYPASS

        metadata = ServiceExecutionMetadata(
            elapsed_ms=dao_result.elapsed_ms,
            cache_status=cache_status,
            cache_key=cache_key if cache_directive or (manager and ttl_seconds > 0) else None,
            cache_version=cache_directive.version if cache_directive else None,
            backend=getattr(self._dao._trino_client, "backend_label", None),
            row_count=len(dao_result.data),
        )
        return ServiceExecutionResult(data=dao_result.data, metadata=metadata)

    # ------------------------------------------------------------------
    # ExportableServiceInterface
    # ------------------------------------------------------------------

    async def export_data(
        self,
        *,
        format: str = "json",
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000,
        context: Optional[ServiceCallContext] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Stream curve data in chunks; awaited per chunk by callers."""
        page = Pagination(limit=max(1, chunk_size), offset=0)
        while True:
            result = await self.query_data(filters=filters, pagination=page, context=context)
            rows = result.data
            if not rows:
                break
            if format == "json":
                for row in rows:
                    yield row
            else:
                yield {"format": format, "data": rows, "offset": page.offset}
            if len(rows) < page.limit:
                break
            page = Pagination(limit=page.limit, offset=page.offset + page.limit)

    # ------------------------------------------------------------------
    # Legacy bridge methods (used by routers that still expect sync semantics)
    # ------------------------------------------------------------------

    def query_curves(
        self,
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
        cache_ttl: int = 120,
    ) -> Tuple[List[Dict[str, Any]], float]:
        curves_query = CurvesQuery(
            asof=asof.isoformat() if isinstance(asof, date) else asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
        )
        page = Pagination(
            limit=limit,
            offset=offset,
            cursor_after=KeysetCursor.from_dict(cursor_after) if cursor_after else None,
            cursor_before=KeysetCursor.from_dict(cursor_before) if cursor_before else None,
            descending=descending,
        )

        async def _runner() -> Tuple[List[Dict[str, Any]], float]:
            result = await self.query_data(
                filters=curves_query.as_params(),
                pagination=page,
                context=ServiceCallContext(
                    cache_directive=CacheDirective(namespace=CacheNamespace.CURVES.value, ttl_seconds=cache_ttl)
                ),
            )
            return result.data, result.metadata.elapsed_ms

        return self._run_sync(_runner())

    def query_curves_diff(
        self,
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
        cache_ttl: int = 120,
    ) -> Tuple[List[Dict[str, Any]], float]:
        diff_query = CurvesDiffQuery(
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
        )
        page = Pagination(limit=limit, offset=offset)

        async def _runner() -> Tuple[List[Dict[str, Any]], float]:
            result = await self.query_diff(
                diff=diff_query,
                pagination=page,
                context=ServiceCallContext(
                    cache_directive=CacheDirective(namespace=CacheNamespace.CURVES.value, ttl_seconds=cache_ttl)
                ),
            )
            return result.data, result.metadata.elapsed_ms

        return self._run_sync(_runner())

    async def invalidate_cache(
        self,
        *,
        scope: Optional[str] = None,
        tenant_id: Optional[str] = None,
        context: Optional[ServiceCallContext] = None,
    ) -> Dict[str, int]:
        manager = self._resolve_cache_manager()
        if manager is None:
            return {"curves": 0}
        prefix = CacheNamespace.CURVES.value
        await manager.invalidate_pattern(f"{prefix}:*")
        logger.info("Curves cache invalidated", extra={"scope": scope, "tenant": tenant_id})
        return {"curves": 0}
