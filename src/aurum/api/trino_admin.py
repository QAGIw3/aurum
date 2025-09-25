"""Admin endpoints for Trino client health and pool metrics.

Loaded optionally by the app factory. These routes require admin access
and expose non-sensitive operational metrics useful for debugging and SRE.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query

from .trino_client import get_trino_client
from .routes import _get_principal, _require_admin
from ..telemetry.context import get_request_id


router = APIRouter()


def _admin_guard(principal=Depends(_get_principal)) -> None:
    _require_admin(principal)


@router.get("/v1/admin/trino/health", dependencies=[Depends(_admin_guard)])
async def trino_health(detailed: bool = Query(False)) -> Dict[str, Any]:
    """Comprehensive Trino client health status and metrics."""
    start = time.perf_counter()
    try:
        client = get_trino_client()
        status = await client.get_health_status()
        query_time_ms = (time.perf_counter() - start) * 1000
        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "detailed": detailed,
            },
            "data": status,
        }
    except Exception as exc:  # pragma: no cover - defensive
        query_time_ms = (time.perf_counter() - start) * 1000
        raise HTTPException(status_code=503, detail={"error": str(exc), "query_time_ms": round(query_time_ms, 2)})


@router.get("/v1/admin/trino/pool", dependencies=[Depends(_admin_guard)])
async def trino_pool() -> Dict[str, Any]:
    """Return Trino connection pool metrics."""
    start = time.perf_counter()
    try:
        client = get_trino_client()
        metrics = await client.get_pool_metrics()
        query_time_ms = (time.perf_counter() - start) * 1000
        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": metrics,
        }
    except Exception as exc:  # pragma: no cover - defensive
        query_time_ms = (time.perf_counter() - start) * 1000
        raise HTTPException(status_code=503, detail={"error": str(exc), "query_time_ms": round(query_time_ms, 2)})

