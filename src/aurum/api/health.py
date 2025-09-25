"""Health check and monitoring endpoints."""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from ..telemetry.context import get_request_id
from .config import CacheConfig, TrinoConfig
from .health_checks import (
    check_redis_ready,
    check_schema_registry_ready,
    check_timescale_ready,
    check_trino_deep_health,
)
from .state import get_settings

router = APIRouter()


@router.get("/health")
@router.get("/healthz")
@router.get("/health/live")
async def health_check() -> Dict[str, Any]:
    """Basic liveness probe."""
    return {"status": "ok", "request_id": get_request_id()}


@router.get("/live")
@router.get("/livez")
async def live_check() -> Dict[str, Any]:
    """Legacy alias for liveness probes."""
    return {"status": "alive", "request_id": get_request_id()}


@router.get("/ready")
@router.get("/readyz")
async def readiness_check() -> Dict[str, Any]:
    """Comprehensive readiness probe covering core dependencies."""
    settings = get_settings()
    trino_cfg = TrinoConfig.from_settings(settings)
    cache_cfg = CacheConfig.from_settings(settings)
    schema_registry_url = getattr(settings.kafka, "schema_registry_url", None)

    trino_future = asyncio.create_task(check_trino_deep_health(trino_cfg))
    schema_future = asyncio.create_task(check_schema_registry_ready(schema_registry_url))
    timescale_future = asyncio.to_thread(check_timescale_ready, settings.database.timescale_dsn)
    redis_future = asyncio.to_thread(check_redis_ready, cache_cfg)

    trino_status, schema_status, timescale_status, redis_status = await asyncio.gather(
        trino_future,
        schema_future,
        timescale_future,
        redis_future,
        return_exceptions=True,
    )

    def _norm(value: object) -> object:
        if isinstance(value, Exception):
            return {"status": "unavailable", "error": str(value)}
        return value

    checks: Dict[str, Any] = {
        "trino": _norm(trino_status),
        "schema_registry": _norm(schema_status),
        "timescale": _norm(timescale_status),
        "redis": _norm(redis_status),
    }

    def _is_ok(value: object) -> bool:
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, dict):
            status = str(value.get("status", "")).lower()  # type: ignore[call-arg]
            return status in {"healthy", "disabled", "ready", "ok"}
        return False

    if all(_is_ok(result) for result in checks.values()):
        return {
            "status": "ready",
            "checks": checks,
            "request_id": get_request_id(),
        }

    raise HTTPException(
        status_code=503,
        detail={"status": "unavailable", "checks": checks},
    )


__all__ = [
    "router",
    "health_check",
    "live_check",
    "readiness_check",
]
