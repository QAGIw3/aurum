"""Database query analysis endpoints."""

import time
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .database_monitor import get_database_monitor, QueryPerformanceLevel

router = APIRouter()


@router.get("/v1/admin/db/slow-queries")
async def get_slow_queries(
    request: Request,
    limit: int = Query(50, description="Number of slow queries to return"),
    since: Optional[str] = Query(None, description="ISO timestamp to filter from"),
    endpoint: Optional[str] = Query(None, description="Filter by endpoint"),
) -> Dict[str, str]:
    """Get slow queries with filtering options."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()

        # Parse since timestamp
        since_dt = None
        if since:
            from datetime import datetime
            since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))

        slow_queries = await monitor.get_slow_queries(limit=limit, since=since_dt)

        # Filter by endpoint if specified
        if endpoint:
            slow_queries = [q for q in slow_queries if endpoint in q.endpoint]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_slow_queries": len(slow_queries),
                "filters": {
                    "since": since,
                    "endpoint": endpoint,
                }
            },
            "data": [
                {
                    "query_hash": q.query_hash,
                    "query_text": q.query_text,
                    "execution_time": q.execution_time,
                    "performance_level": q.performance_level.value,
                    "timestamp": q.timestamp.isoformat(),
                    "endpoint": q.endpoint,
                    "user_id": q.user_id,
                    "tenant_id": q.tenant_id,
                    "result_count": q.result_count,
                    "error_message": q.error_message,
                }
                for q in slow_queries
            ]
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get slow queries: {str(exc)}"
        ) from exc


@router.get("/v1/admin/db/query-patterns")
async def get_query_patterns(
    request: Request,
    limit: int = Query(20, description="Number of patterns to return"),
    min_executions: int = Query(5, description="Minimum executions to include"),
    sort_by: str = Query("performance_score", description="Sort criteria"),
    performance_level: Optional[str] = Query(None, description="Filter by performance level"),
) -> Dict[str, str]:
    """Get query patterns with performance analysis."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()
        patterns = await monitor.get_query_patterns(
            limit=limit,
            min_executions=min_executions,
            sort_by=sort_by
        )

        # Filter by performance level if specified
        if performance_level:
            level_enum = QueryPerformanceLevel(performance_level)
            patterns = [
                p for p in patterns
                if any(
                    q.performance_level == level_enum
                    for q in monitor.recent_queries
                    if q.query_hash == p.query_hash
                )
            ]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_patterns": len(patterns),
                "filters": {
                    "min_executions": min_executions,
                    "performance_level": performance_level,
                },
                "sort_by": sort_by,
            },
            "data": [
                {
                    "query_hash": p.query_hash,
                    "pattern_text": p.pattern_text,
                    "total_executions": p.total_executions,
                    "total_time": p.total_time,
                    "avg_time": p.avg_time,
                    "min_time": p.min_time,
                    "max_time": p.max_time,
                    "error_count": p.error_count,
                    "error_rate": p.error_count / p.total_executions if p.total_executions > 0 else 0,
                    "performance_score": p.get_performance_score(),
                    "first_seen": p.first_seen.isoformat(),
                    "last_seen": p.last_seen.isoformat(),
                    "endpoints": p.endpoints,
                }
                for p in patterns
            ]
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get query patterns: {str(exc)}"
        ) from exc
