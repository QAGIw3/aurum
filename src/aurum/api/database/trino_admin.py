"""Admin endpoints for Trino client health, pool metrics, and performance monitoring."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from aurum.telemetry.context import get_request_id
from ..routes import _get_principal, _require_admin
from .trino_client import get_trino_client
from ..state import get_settings
from .config import TrinoCatalogType, TrinoAccessLevel


router = APIRouter()


def _admin_guard(principal=Depends(_get_principal)) -> None:
    """Require administrator access for Trino admin endpoints."""
    _require_admin(principal)


class TrinoPoolMetrics(BaseModel):
    """Trino connection pool metrics."""
    active_connections: int
    idle_connections: int
    total_connections: int
    max_connections: int
    pool_utilization: float
    queue_size: int
    wait_queue_size: int
    connection_acquire_time_ms: float
    connection_errors: int


class TrinoQueryStats(BaseModel):
    """Trino query statistics."""
    running_queries: int
    queued_queries: int
    failed_queries: int
    total_queries: int
    avg_query_time_seconds: float
    p95_query_time_seconds: float
    p99_query_time_seconds: float


class TrinoHealthStatus(BaseModel):
    """Comprehensive Trino health status."""
    healthy: bool
    circuit_breaker_state: str
    pool_saturation: float
    pool_saturation_status: str
    last_error: Optional[str] = None
    uptime_seconds: float
    total_queries_executed: int
    total_query_errors: int
    avg_response_time_seconds: float


@router.get("/v1/admin/trino/health", dependencies=[Depends(_admin_guard)])
async def trino_health(
    detailed: bool = Query(False, description="Include detailed metrics")
) -> Dict:
    """Return comprehensive Trino client health status and metrics."""
    start_time = time.perf_counter()

    try:
        settings = get_settings()
        client = get_trino_client()
        health_status = await client.get_health_status()

        # Get enhanced pool metrics
        pool_metrics = await client.get_pool_metrics()

        # Determine health status
        is_healthy = (
            health_status["healthy"] and
            pool_metrics.pool_utilization < 0.9 and
            health_status.get("circuit_breaker_state", "CLOSED") == "CLOSED"
        )

        # Determine pool saturation status
        saturation_status = "healthy"
        if pool_metrics.pool_utilization > 0.9:
            saturation_status = "critical"
        elif pool_metrics.pool_utilization > 0.7:
            saturation_status = "warning"
        elif pool_metrics.pool_utilization > 0.5:
            saturation_status = "elevated"

        query_time_ms = (time.perf_counter() - start_time) * 1000

        response_data = {
            "healthy": is_healthy,
            "circuit_breaker_state": health_status.get("circuit_breaker_state", "UNKNOWN"),
            "pool_saturation": pool_metrics.pool_utilization,
            "pool_saturation_status": saturation_status,
            "uptime_seconds": health_status.get("uptime_seconds", 0),
            "total_queries_executed": health_status.get("total_queries", 0),
            "total_query_errors": health_status.get("total_errors", 0),
            "avg_response_time_seconds": health_status.get("avg_response_time", 0),
        }

        if health_status.get("last_error"):
            response_data["last_error"] = health_status["last_error"]

        if detailed:
            response_data.update({
                "pool_metrics": pool_metrics.model_dump(),
                "query_stats": await client.get_query_stats(),
            })

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "catalog": settings.trino.catalog,
                "schema": settings.trino.database_schema,
                "detailed": detailed,
            },
            "data": response_data
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.get("/v1/admin/trino/pool", dependencies=[Depends(_admin_guard)])
async def trino_pool_status(
    detailed: bool = Query(False, description="Include detailed pool statistics")
) -> Dict:
    """Get comprehensive Trino connection pool metrics."""
    start_time = time.perf_counter()

    try:
        settings = get_settings()
        client = get_trino_client()
        pool_metrics = await client.get_pool_metrics()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        response_data = {
            "active_connections": pool_metrics.active_connections,
            "idle_connections": pool_metrics.idle_connections,
            "total_connections": pool_metrics.total_connections,
            "max_connections": pool_metrics.max_connections,
            "pool_utilization": pool_metrics.pool_utilization,
            "queue_size": pool_metrics.queue_size,
            "wait_queue_size": pool_metrics.wait_queue_size,
            "connection_acquire_time_ms": pool_metrics.connection_acquire_time_ms,
            "connection_errors": pool_metrics.connection_errors,
        }

        if detailed:
            # Add additional pool statistics
            response_data.update({
                "connection_creation_rate": await client.get_connection_creation_rate(),
                "connection_destroy_rate": await client.get_connection_destroy_rate(),
                "pool_efficiency": await client.get_pool_efficiency(),
            })

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "detailed": detailed,
            },
            "data": response_data,
            "config": {
                "min_size": settings.api.concurrency.trino_connection_pool_min_size,
                "max_size": settings.api.concurrency.trino_connection_pool_max_size,
                "idle_timeout_seconds": settings.api.concurrency.trino_connection_pool_idle_timeout_seconds,
                "wait_timeout_seconds": settings.api.concurrency.trino_connection_pool_wait_timeout_seconds,
            }
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.get("/v1/admin/trino/stats", dependencies=[Depends(_admin_guard)])
async def trino_query_stats(
    time_range: str = Query("15m", description="Time range for statistics (15m, 1h, 24h)"),
    include_errors: bool = Query(True, description="Include error statistics")
) -> Dict:
    """Get comprehensive Trino query statistics and performance metrics."""
    start_time = time.perf_counter()

    try:
        client = get_trino_client()
        query_stats = await client.get_query_stats()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "time_range": time_range,
                "include_errors": include_errors,
            },
            "data": query_stats.model_dump()
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.post("/v1/admin/trino/reset", dependencies=[Depends(_admin_guard)])
async def trino_reset_circuit_breaker() -> Dict:
    """Reset Trino circuit breaker to recover from failures."""
    start_time = time.perf_counter()

    try:
        client = get_trino_client()
        await client.reset_circuit_breaker()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Trino circuit breaker reset successfully",
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.get("/v1/admin/trino/config", dependencies=[Depends(_admin_guard)])
async def trino_config_status() -> Dict:
    """Get Trino client configuration and runtime settings."""
    start_time = time.perf_counter()

    try:
        settings = get_settings()
        client = get_trino_client()

        config_data = {
            "trino_host": settings.trino.host,
            "trino_port": settings.trino.port,
            "trino_catalog": settings.trino.catalog,
            "trino_schema": settings.trino.database_schema,
            "connection_pool_min_size": settings.api.concurrency.trino_connection_pool_min_size,
            "connection_pool_max_size": settings.api.concurrency.trino_connection_pool_max_size,
            "connection_pool_idle_timeout_seconds": settings.api.concurrency.trino_connection_pool_idle_timeout_seconds,
            "connection_pool_wait_timeout_seconds": settings.api.concurrency.trino_connection_pool_wait_timeout_seconds,
            "query_timeout_seconds": settings.api.concurrency.trino_query_timeout_seconds,
            "circuit_breaker_failure_threshold": settings.api.concurrency.trino_circuit_breaker_failure_threshold,
            "circuit_breaker_timeout_seconds": settings.api.concurrency.trino_circuit_breaker_timeout_seconds,
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": config_data
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.get("/v1/admin/trino/catalogs", dependencies=[Depends(_admin_guard)])
async def trino_catalog_status() -> Dict:
    """Get status of all configured Trino catalogs."""
    start_time = time.perf_counter()

    try:
        from .trino_client import get_trino_catalog_config

        # Get configuration for both catalogs
        raw_config = get_trino_catalog_config(TrinoCatalogType.RAW.value)
        market_config = get_trino_catalog_config(TrinoCatalogType.MARKET.value)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                TrinoCatalogType.RAW.value: {
                    "catalog": raw_config.catalog,
                    "schema": raw_config.schema,
                    "access_level": raw_config.access_level.value,
                    "lineage_tags": raw_config.lineage_tags,
                    "description": "Raw external data catalog (read-only)"
                },
                TrinoCatalogType.MARKET.value: {
                    "catalog": market_config.catalog,
                    "schema": market_config.schema,
                    "access_level": market_config.access_level.value,
                    "lineage_tags": market_config.lineage_tags,
                    "description": "Processed market data catalog (read-write)"
                }
            }
        }
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.get("/v1/admin/trino/catalogs/{catalog_type}", dependencies=[Depends(_admin_guard)])
async def trino_catalog_info(
    catalog_type: str,
    include_access_control: bool = Query(False, description="Include access control details")
) -> Dict:
    """Get detailed information about a specific Trino catalog."""
    start_time = time.perf_counter()

    try:
        from .trino_client import get_trino_catalog_config

        # Validate catalog type
        if catalog_type not in [TrinoCatalogType.RAW.value, TrinoCatalogType.MARKET.value]:
            raise HTTPException(status_code=400, detail=f"Unknown catalog type: {catalog_type}")

        config = get_trino_catalog_config(catalog_type)
        client = get_trino_client(catalog_type)

        # Get health status for this catalog
        health_status = await client.get_health_status()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        response_data = {
            "catalog": config.catalog,
            "schema": config.schema,
            "access_level": config.access_level.value,
            "lineage_tags": config.lineage_tags,
            "health_status": health_status,
            "description": "Raw external data catalog (read-only)" if catalog_type == TrinoCatalogType.RAW.value else "Processed market data catalog (read-write)"
        }

        if include_access_control:
            response_data["access_control"] = {
                "read_allowed": config.access_level in [TrinoAccessLevel.READ_ONLY, TrinoAccessLevel.READ_WRITE, TrinoAccessLevel.ADMIN],
                "write_allowed": config.access_level in [TrinoAccessLevel.READ_WRITE, TrinoAccessLevel.ADMIN],
                "admin_allowed": config.access_level == TrinoAccessLevel.ADMIN,
                "user": config.user,
            }

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "catalog_type": catalog_type,
                "include_access_control": include_access_control,
            },
            "data": response_data
        }
    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )


@router.post("/v1/admin/trino/catalogs/{catalog_type}/validate-access", dependencies=[Depends(_admin_guard)])
async def validate_catalog_access(
    catalog_type: str,
    operation: str = Query("read", description="Operation to validate (read, write)")
) -> Dict:
    """Validate access permissions for a specific catalog operation."""
    start_time = time.perf_counter()

    try:
        from .trino_client import get_trino_catalog_config

        # Validate catalog type
        if catalog_type not in [TrinoCatalogType.RAW.value, TrinoCatalogType.MARKET.value]:
            raise HTTPException(status_code=400, detail=f"Unknown catalog type: {catalog_type}")

        config = get_trino_catalog_config(catalog_type)
        client = get_trino_client(catalog_type)

        # Validate the operation
        await client.validate_access(operation)

        # Check permissions based on access level
        access_allowed = False
        if operation == "read":
            access_allowed = config.access_level in [TrinoAccessLevel.READ_ONLY, TrinoAccessLevel.READ_WRITE, TrinoAccessLevel.ADMIN]
        elif operation == "write":
            access_allowed = config.access_level in [TrinoAccessLevel.READ_WRITE, TrinoAccessLevel.ADMIN]
        else:
            raise HTTPException(status_code=400, detail=f"Unknown operation: {operation}")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "catalog_type": catalog_type,
                "operation": operation,
            },
            "data": {
                "catalog": config.catalog,
                "operation": operation,
                "access_level": config.access_level.value,
                "access_allowed": access_allowed,
                "lineage_tags": config.lineage_tags,
                "validation_performed": True
            }
        }
    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(exc),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        )
