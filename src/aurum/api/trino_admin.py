"""Admin endpoints for Trino client health and pool metrics."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from .routes import _get_principal, _require_admin
from .trino_client import get_trino_client
from .state import get_settings


router = APIRouter()


def _admin_guard(principal=Depends(_get_principal)) -> None:
    """Require administrator access for Trino admin endpoints."""
    _require_admin(principal)


@router.get("/v1/admin/trino/health", dependencies=[Depends(_admin_guard)])
async def trino_health() -> dict:
    """Return Trino client health status and last error info, if any."""
    try:
        settings = get_settings()
        client = get_trino_client()
        status = await client.get_health_status()
        return {
            "meta": {
                "catalog": settings.trino.catalog,
                "schema": settings.trino.database_schema,
            },
            "data": status,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"error": str(exc)})


@router.get("/v1/admin/trino/pool", dependencies=[Depends(_admin_guard)])
async def trino_pool_status() -> dict:
    """Expose basic connection pool metrics for Trino client."""
    try:
        client = get_trino_client()
        pool = client.connection_pool
        return {
            "data": {
                "min_size": pool.min_size,
                "max_size": pool.max_size,
                "idle_timeout_seconds": pool.idle_timeout_seconds,
                "wait_timeout_seconds": pool.wait_timeout_seconds,
                "queue_size": pool._pool.qsize(),
            }
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"error": str(exc)})

