"""Health check and monitoring endpoints."""

from __future__ import annotations

import json
from typing import Dict, List

from fastapi import APIRouter, HTTPException

from ..telemetry.context import get_request_id
from .state import get_settings

router = APIRouter()


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy", "request_id": get_request_id()}


@router.get("/ready")
async def readiness_check() -> Dict[str, str]:
    """Readiness probe for load balancer checks."""
    settings = get_settings()

    # Check critical dependencies
    checks = {}

    # Check Trino connectivity
    try:
        from .config import TrinoConfig
        from .routes import _check_trino_ready as _routes_check_trino_ready

        trino_cfg = TrinoConfig.from_settings(settings)
        checks["trino"] = "ready" if (await _routes_check_trino_ready(trino_cfg)) else "unavailable"
    except Exception:
        checks["trino"] = "unavailable"

    # Check Redis connectivity
    try:
        from .service import _maybe_redis_client
        import asyncio as _asyncio
        cache_cfg = settings.api.cache
        client = _maybe_redis_client(cache_cfg)
        if client:
            # Offload blocking ping call
            await _asyncio.to_thread(client.ping)
            checks["redis"] = "ready"
        else:
            checks["redis"] = "disabled"
    except Exception:
        checks["redis"] = "unavailable"

    # Overall status
    unavailable_checks = [service for service, status in checks.items() if status == "unavailable"]
    if unavailable_checks:
        raise HTTPException(
            status_code=503,
            detail={"status": "unavailable", "checks": checks}
        )

    return {"status": "ready", "checks": checks, "request_id": get_request_id()}
