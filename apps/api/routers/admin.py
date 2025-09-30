"""Admin router for system management operations."""
from __future__ import annotations

from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from libs.storage import TimescaleSeriesRepo, PostgresMetaRepo, TrinoAnalyticRepo, RedisCacheRepo
from libs.common.config import get_settings
from ..main import get_timescale_repo, get_postgres_repo, get_trino_repo

router = APIRouter()


@router.get("/health")
async def detailed_health_check(
    timescale_repo: TimescaleSeriesRepo = Depends(get_timescale_repo),
    postgres_repo: PostgresMetaRepo = Depends(get_postgres_repo),
    trino_repo: TrinoAnalyticRepo = Depends(get_trino_repo),
) -> dict:
    """Detailed health check for all system components."""
    
    health_status = {
        "status": "healthy",
        "components": {}
    }
    
    # Check TimescaleDB
    try:
        # Simple query to check connectivity
        await timescale_repo.list_curves(limit=1)
        health_status["components"]["timescale"] = {"status": "healthy"}
    except Exception as e:
        health_status["components"]["timescale"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check PostgreSQL
    try:
        await postgres_repo.list_scenarios(limit=1)
        health_status["components"]["postgres"] = {"status": "healthy"}
    except Exception as e:
        health_status["components"]["postgres"] = {
            "status": "unhealthy", 
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Trino
    try:
        await trino_repo.execute_query("SELECT 1")
        health_status["components"]["trino"] = {"status": "healthy"}
    except Exception as e:
        health_status["components"]["trino"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status


@router.get("/stats")
async def get_system_stats(
    timescale_repo: TimescaleSeriesRepo = Depends(get_timescale_repo),
    postgres_repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """Get high-level system statistics."""
    
    stats = {}
    
    try:
        # Get curve count
        _, curve_count = await timescale_repo.list_curves(limit=0)
        stats["curves_total"] = curve_count
        
        # Get scenario count
        _, scenario_count = await postgres_repo.list_scenarios(limit=0)
        stats["scenarios_total"] = scenario_count
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system stats: {str(e)}")


@router.post("/cache/clear")
async def clear_cache(
    tenant_id: Optional[str] = None,
    scope: Optional[str] = "all",
) -> dict:
    """Clear application caches.

    - scope: one of [all, market, catalog, metadata]
    - tenant_id: if provided, clears only keys matching that tenant
    """
    settings = get_settings()
    cache = RedisCacheRepo(settings.redis)

    patterns = []
    if scope in (None, "all"):
        patterns.append("aurum:*")
    elif scope == "market":
        patterns.append("aurum:*market:*")
    elif scope == "catalog":
        patterns.append("aurum:*catalog:*")
    elif scope == "metadata":
        patterns.append("aurum:*metadata:*")
    else:
        patterns.append(f"aurum:*{scope}*")

    total = 0
    for pat in patterns:
        if tenant_id:
            pat = pat.replace("*", f"*{tenant_id}*")
        total += await cache.invalidate(pat)

    return {
        "message": "Cache clear executed",
        "scope": scope,
        "tenant_id": tenant_id,
        "invalidated": total,
    }


@router.get("/config")
async def get_config() -> dict:
    """Get current application configuration (sanitized)."""
    
    from libs.common.config import get_settings
    
    settings = get_settings()
    
    # Return sanitized config without secrets
    return {
        "environment": settings.environment,
        "debug": settings.debug,
        "api": {
            "title": settings.api.title,
            "version": settings.api.version,
            "host": settings.api.host,
            "port": settings.api.port,
        },
        "features": {
            "enable_v2_only": settings.enable_v2_only,
            "enable_timescale_caggs": settings.enable_timescale_caggs,
            "enable_iceberg_time_travel": settings.enable_iceberg_time_travel,
        },
        "pagination": {
            "default_size": settings.pagination_default_size,
            "max_size": settings.pagination_max_size,
        }
    }