from __future__ import annotations

"""Service layer for v2 Curves endpoints.

Delegates to the shared ``libs.services.curves_service`` implementation and
adapts dataclasses into Pydantic models expected by the router layer.
"""

from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

from libs.services.cache_support import AsyncCacheProtocol
from libs.services.curves_service import Curve as SharedCurve
from libs.services.curves_service import CurvesService as SharedCurvesService


class CurveItem(BaseModel):
    id: str = Field(..., description="Curve ID")
    name: str = Field(..., description="Curve name")
    description: Optional[str] = Field(None, description="Curve description")
    data_points: int = Field(..., description="Number of data points")
    created_at: Optional[str] = Field(None, description="Latest ingest timestamp")


class CurvesV2Service:
    def __init__(self, service: Optional[SharedCurvesService] = None) -> None:
        self._service = service or SharedCurvesService()

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

        result, debug_meta = await self._service.list_curves(
            tenant_id=tenant_id,
            offset=offset,
            limit=limit,
            name_filter=name_filter,
            include_debug=include_debug,
        )

        items = [self._to_curve_item(curve) for curve in result.data]
        return items, (debug_meta if include_debug else None)

    async def get_curve_diff(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> CurveItem:
        result = await self._service.get_curve_diff(
            curve_id=curve_id,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )
        curve = result.data
        fallback_description = f"Diff between {from_timestamp} and {to_timestamp}"
        return self._to_curve_item(curve, description_override=curve.description or fallback_description)

    @staticmethod
    def _to_curve_item(curve: SharedCurve, *, description_override: Optional[str] = None) -> CurveItem:
        return CurveItem(
            id=curve.id,
            name=curve.name,
            description=description_override if description_override is not None else curve.description,
            data_points=curve.data_points,
            created_at=curve.created_at,
        )


async def get_curve_service() -> CurvesV2Service:
    """Factory for the v2 Curves service."""

    from aurum.telemetry import get_tracer

    from ..cache.unified_cache_manager import CacheNamespace, get_unified_cache_manager
    from ..dao.curves_dao import CurvesDao

    class UnifiedCacheAdapter(AsyncCacheProtocol):
        def __init__(self, manager, namespace: CacheNamespace) -> None:
            self._manager = manager
            self._namespace = namespace

        async def get(self, key: str, *, namespace: Optional[str] = None) -> Optional[Any]:
            target = CacheNamespace(namespace) if namespace else self._namespace
            return await self._manager.get(key, namespace=target)

        async def set(
            self,
            key: str,
            value: Any,
            *,
            ttl_seconds: Optional[int] = None,
            namespace: Optional[str] = None,
        ) -> bool:
            target = CacheNamespace(namespace) if namespace else self._namespace
            await self._manager.set(key, value, namespace=target, ttl_seconds=ttl_seconds)
            return True

        async def invalidate(self, key: str, *, namespace: Optional[str] = None) -> int:
            target = CacheNamespace(namespace) if namespace else self._namespace
            deleted = await self._manager.delete(key, namespace=target)
            return 1 if deleted else 0

    cache_adapter: Optional[AsyncCacheProtocol] = None
    try:
        cache_manager = get_unified_cache_manager()
    except Exception:
        cache_manager = None

    if cache_manager is not None:
        cache_adapter = UnifiedCacheAdapter(cache_manager, CacheNamespace.CURVES)

    shared_service = SharedCurvesService(
        dao=CurvesDao(),
        cache=cache_adapter,
        tracer=get_tracer("aurum.api.curves"),
        cache_namespace=CacheNamespace.CURVES.value,
    )
    return CurvesV2Service(shared_service)
