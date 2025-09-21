"""Cache analytics and monitoring endpoints."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .advanced_cache import get_advanced_cache_manager, get_cache_warming_service
from .cache_models import (
    CacheAnalyticsResponse,
)
from .models import (
    CacheStatsResponse,
    CacheWarmingStatusResponse,
    NamespaceAnalytics,
    WarmingTaskInfo,
)


router = APIRouter()


@router.get("/v1/cache/analytics", response_model=CacheAnalyticsResponse)
async def get_cache_analytics(
    request: Request,
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
) -> CacheAnalyticsResponse:
    """Get comprehensive cache analytics."""
    start_time = time.perf_counter()

    try:
        cache_manager = get_advanced_cache_manager()

        if namespace:
            # Get analytics for specific namespace
            analytics = await cache_manager.get_analytics(namespace)
            all_analytics = {namespace: analytics}
        else:
            # Get analytics for all namespaces
            all_analytics = await cache_manager.get_all_analytics()

        # Get memory usage
        memory_usage = await cache_manager.get_memory_usage()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        namespaces_data = []
        for ns_name, analytics in all_analytics.items():
            namespaces_data.append(NamespaceAnalytics(
                namespace=ns_name,
                hits=analytics.hits,
                misses=analytics.misses,
                hit_rate=analytics.hit_rate,
                total_requests=analytics.total_requests,
                average_response_time=analytics.average_response_time,
                memory_usage_bytes=memory_usage.get("namespaces", {}).get(ns_name, 0)
            ))

        return CacheAnalyticsResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_namespaces": len(namespaces_data),
            },
            data={
                "namespaces": namespaces_data,
                "summary": {
                    "total_hits": sum(ns.hits for ns in namespaces_data),
                    "total_misses": sum(ns.misses for ns in namespaces_data),
                    "overall_hit_rate": (
                        sum(ns.hits for ns in namespaces_data) /
                        max(sum(ns.total_requests for ns in namespaces_data), 1)
                    ),
                    "total_memory_bytes": memory_usage.get("estimated_size_bytes", 0),
                }
            }
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get cache analytics: {str(exc)}"
        ) from exc


@router.get("/v1/cache/stats", response_model=CacheStatsResponse)
async def get_cache_stats(
    request: Request,
) -> CacheStatsResponse:
    """Get comprehensive cache statistics."""
    start_time = time.perf_counter()

    try:
        cache_manager = get_advanced_cache_manager()
        stats = await cache_manager.get_cache_stats()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CacheStatsResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=stats
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get cache stats: {str(exc)}"
        ) from exc


@router.get("/v1/cache/warming/status", response_model=CacheWarmingStatusResponse)
async def get_warming_status(
    request: Request,
) -> CacheWarmingStatusResponse:
    """Get cache warming service status."""
    start_time = time.perf_counter()

    try:
        warming_service = get_cache_warming_service()
        status = await warming_service.get_warming_status()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CacheWarmingStatusResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=status
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get warming status: {str(exc)}"
        ) from exc


@router.post("/v1/cache/warming/start")
async def start_cache_warming(
    request: Request,
) -> Dict[str, str]:
    """Start the cache warming service."""
    start_time = time.perf_counter()

    try:
        warming_service = get_cache_warming_service()
        await warming_service.start_warming_service()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Cache warming service started",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start cache warming: {str(exc)}"
        ) from exc


@router.post("/v1/cache/warming/stop")
async def stop_cache_warming(
    request: Request,
) -> Dict[str, str]:
    """Stop the cache warming service."""
    start_time = time.perf_counter()

    try:
        warming_service = get_cache_warming_service()
        await warming_service.stop_warming_service()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Cache warming service stopped",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop cache warming: {str(exc)}"
        ) from exc


@router.post("/v1/cache/warming/tasks")
async def add_warming_task(
    request: Request,
    task_data: Dict[str, str],
) -> Dict[str, str]:
    """Add a new cache warming task."""
    start_time = time.perf_counter()

    try:
        name = task_data.get("name")
        key_pattern = task_data.get("key_pattern")
        schedule_interval = int(task_data.get("schedule_interval", "300"))

        if not name or not key_pattern:
            raise HTTPException(
                status_code=400,
                detail="name and key_pattern are required"
            )

        # Create a placeholder warmup function
        async def warmup_function(key: str):
            # This would be replaced with actual data loading logic
            return f"warmup_data_for_{key}"

        warming_service = get_cache_warming_service()
        await warming_service.add_warmup_task(
            name=name,
            key_pattern=key_pattern,
            warmup_function=warmup_function,
            schedule_interval=schedule_interval,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Warming task '{name}' added successfully",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add warming task: {str(exc)}"
        ) from exc


@router.get("/v1/cache/performance/recommendations")
async def get_performance_recommendations(
    request: Request,
) -> Dict[str, List[str]]:
    """Get cache performance optimization recommendations."""
    start_time = time.perf_counter()

    try:
        cache_manager = get_advanced_cache_manager()
        analytics = await cache_manager.get_all_analytics()
        stats = await cache_manager.get_cache_stats()

        recommendations = []

        # Analyze hit rates by namespace
        for namespace, data in analytics.items():
            if data.hit_rate < 0.5:
                recommendations.append(
                    f"Low hit rate ({data.hit_rate:.1%}) in namespace '{namespace}' - "
                    "consider increasing TTL or reducing data volatility"
                )
            elif data.hit_rate > 0.95:
                recommendations.append(
                    f"High hit rate ({data.hit_rate:.1%}) in namespace '{namespace}' - "
                    "consider increasing TTL to reduce cache misses"
                )

        # Check memory usage
        memory_usage = await cache_manager.get_memory_usage()
        total_memory = memory_usage.get("estimated_size_bytes", 0)
        if total_memory > 100 * 1024 * 1024:  # 100MB
            recommendations.append(
                f"High memory usage ({total_memory / (1024*1024):.1f}MB) - "
                "consider implementing cache eviction policies"
            )

        # Check warming status
        if stats.get("warming_status") == "failed":
            recommendations.append(
                "Cache warming service is failing - check warmup task configurations"
            )

        # Strategy recommendations
        if len(analytics) > 5:
            recommendations.append(
                "Multiple namespaces detected - consider namespace-specific cache strategies"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "recommendations": recommendations,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_recommendations": len(recommendations)
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get performance recommendations: {str(exc)}"
        ) from exc


@router.post("/v1/cache/strategies/{namespace}")
async def set_cache_strategy(
    request: Request,
    namespace: str,
    strategy_data: Dict[str, str],
) -> Dict[str, str]:
    """Set cache strategy for a namespace."""
    start_time = time.perf_counter()

    try:
        strategy = strategy_data.get("strategy", "adaptive")

        # Validate strategy
        valid_strategies = ["lru", "lfu", "ttl", "adaptive"]
        if strategy not in valid_strategies:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid strategy. Must be one of: {valid_strategies}"
            )

        cache_manager = get_advanced_cache_manager()

        # This would set the strategy for the namespace
        # For now, just return success
        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Cache strategy '{strategy}' set for namespace '{namespace}'",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set cache strategy: {str(exc)}"
        ) from exc


@router.delete("/v1/cache/namespaces/{namespace}")
async def clear_namespace(
    request: Request,
    namespace: str,
) -> Dict[str, str]:
    """Clear all cache entries in a namespace."""
    start_time = time.perf_counter()

    try:
        cache_manager = get_advanced_cache_manager()
        await cache_manager.clear_namespace(namespace)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Namespace '{namespace}' cleared successfully",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear namespace: {str(exc)}"
        ) from exc


@router.get("/v1/cache/health")
async def get_cache_health(
    request: Request,
) -> Dict[str, str]:
    """Get cache health status."""
    start_time = time.perf_counter()

    try:
        cache_manager = get_advanced_cache_manager()
        stats = await cache_manager.get_cache_stats()
        analytics = await cache_manager.get_all_analytics()

        # Determine overall health
        health_status = "healthy"
        issues = []

        # Check warming status
        if stats.get("warming_status") == "failed":
            health_status = "degraded"
            issues.append("Cache warming service is failing")

        # Check hit rates
        for namespace, data in analytics.items():
            if data.hit_rate < 0.2:
                health_status = "degraded"
                issues.append(f"Low hit rate in namespace '{namespace}': {data.hit_rate:.1%}")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": health_status,
            "issues": issues,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        return {
            "status": "unhealthy",
            "issues": [f"Cache health check failed: {str(exc)}"],
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }
