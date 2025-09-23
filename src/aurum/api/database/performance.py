"""Database performance monitoring endpoints."""

import time
from typing import Dict

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .database_monitor import get_database_monitor

router = APIRouter()


@router.get("/v1/admin/db/performance")
async def get_database_performance(
    request: Request,
    time_range: str = Query("1h", description="Time range (1h, 24h, 7d)"),
) -> Dict[str, str]:
    """Get comprehensive database performance metrics."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()
        performance_summary = await monitor.get_performance_summary()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "time_range": time_range,
            },
            "data": performance_summary
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database performance: {str(exc)}"
        ) from exc
