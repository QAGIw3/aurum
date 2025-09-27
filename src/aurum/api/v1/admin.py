"""v1 Admin endpoints split from the monolith for maintainability.

Endpoints implemented:
- POST /v1/admin/cache/scenario/{scenario_id}/invalidate (204)
- POST /v1/admin/cache/curves/invalidate (CachePurgeResponse)
- POST /v1/admin/cache/eia/series/invalidate (CachePurgeResponse)

Enable via AURUM_API_V1_SPLIT_ADMIN=1 in app wiring.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request, Response

from ..config import CacheConfig
from ..http import respond_with_etag
from ..models import CachePurgeDetail, CachePurgeResponse, Meta
from ..service import _maybe_redis_client, invalidate_scenario_outputs_cache, invalidate_eia_series_cache
from ..state import get_settings
from ...telemetry.context import get_request_id

# Reuse route helpers for admin enforcement
from ..routes import _require_admin  # type: ignore


router = APIRouter()


def _get_principal(request: Request) -> Dict[str, Any] | None:
    principal = getattr(request.state, "principal", None)
    return principal if isinstance(principal, dict) else None


@router.post("/v1/admin/cache/scenario/{scenario_id}/invalidate", status_code=204)
def invalidate_scenario_cache(
    scenario_id: str,
    request: Request,
    tenant_id: Optional[str] = None,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> Response:
    """Invalidate cached scenario outputs for a specific scenario (admin-only)."""
    _require_admin(principal)
    effective_tenant = tenant_id or (principal or {}).get("tenant")
    cache_cfg = CacheConfig.from_settings(get_settings())
    invalidate_scenario_outputs_cache(cache_cfg, effective_tenant, scenario_id)
    return Response(status_code=204)


@router.post("/v1/admin/cache/curves/invalidate", response_model=CachePurgeResponse)
def invalidate_curve_cache_admin(request: Request) -> CachePurgeResponse:
    """Invalidate curve caches in Redis (admin-only)."""
    _require_admin(getattr(request.state, "principal", None) or {})
    request_id = get_request_id() or "unknown"
    cache_cfg = CacheConfig.from_settings(get_settings())
    client = _maybe_redis_client(cache_cfg)

    removed_counts: Dict[str, int] = {"curves": 0, "curves-diff": 0}
    prefixes = (("curves:", "curves"), ("curves-diff:", "curves-diff"))

    if client is not None:
        for prefix, scope in prefixes:
            pattern = f"{prefix}*"
            keys: list[Any] = []
            iterator = getattr(client, "scan_iter", None)
            try:
                if callable(iterator):
                    keys = list(iterator(pattern))
                elif hasattr(client, "keys"):
                    keys = list(client.keys(pattern))
            except Exception:
                keys = []

            if keys:
                try:
                    deleted = client.delete(*keys)
                    removed_counts[scope] = int(deleted or 0)
                except Exception:
                    pass

    details = [
        CachePurgeDetail(scope="curves", redis_keys_removed=removed_counts["curves"], local_entries_removed=0),
        CachePurgeDetail(scope="curves-diff", redis_keys_removed=removed_counts["curves-diff"], local_entries_removed=0),
    ]
    model = CachePurgeResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=details)
    # Return with ETag for idempotent responses
    return respond_with_etag(model, request, Response())


@router.post("/v1/admin/cache/eia/series/invalidate", response_model=CachePurgeResponse)
def invalidate_eia_series_cache_admin(request: Request) -> CachePurgeResponse:
    """Invalidate cached EIA series queries in Redis (admin-only)."""
    _require_admin(getattr(request.state, "principal", None) or {})
    request_id = get_request_id() or "unknown"
    cache_cfg = CacheConfig.from_settings(get_settings())
    results = invalidate_eia_series_cache(cache_cfg)
    data = [
        CachePurgeDetail(scope="eia-series", redis_keys_removed=results.get("eia-series", 0), local_entries_removed=0),
        CachePurgeDetail(scope="eia-series-dimensions", redis_keys_removed=results.get("eia-series-dimensions", 0), local_entries_removed=0),
    ]
    model = CachePurgeResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=data)
    return respond_with_etag(model, request, Response())

