"""Database performance monitoring endpoints."""

import time
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .database_monitor import get_database_monitor
from ..state import request_settings

try:  # pragma: no cover - optional dependency
    import asyncpg
except ImportError:  # pragma: no cover - optional dependency
    asyncpg = None  # type: ignore[assignment]

router = APIRouter()


@router.get("/v1/admin/db/performance")
async def get_database_performance(
    request: Request,
    time_range: str = Query("1h", description="Time range (1h, 24h, 7d)"),
) -> Dict[str, Any]:
    """Get comprehensive database performance metrics."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()
        performance_summary = await monitor.get_performance_summary()

        settings = request_settings(request)
        slow_queries = await _sample_timescale_slow_queries(settings.database.timescale_dsn)
        performance_summary["timescale_slow_queries"] = slow_queries

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


async def _sample_timescale_slow_queries(
    dsn: str,
    limit: int = 10,
    min_mean_ms: float = 250.0,
) -> List[Dict[str, Any]]:
    """Return top slow Timescale queries from pg_stat_statements."""
    if asyncpg is None or not dsn:
        return []

    query = (
        "SELECT query, calls, total_time, mean_time, rows, shared_blks_hit, "
        "shared_blks_read FROM pg_stat_statements "
        "WHERE calls > 0 ORDER BY mean_time DESC LIMIT $1"
    )

    try:
        conn = await asyncpg.connect(dsn)
    except Exception:  # pragma: no cover - defensive
        return []

    try:
        records = await conn.fetch(query, limit)
    except Exception:  # pragma: no cover - extension missing
        records = []
    finally:
        await conn.close()

    slow_queries: List[Dict[str, Any]] = []
    for record in records:
        mean_time = float(record.get("mean_time", 0.0))
        if mean_time < min_mean_ms:
            continue
        text = str(record.get("query", "")).strip()
        if len(text) > 500:
            text = f"{text[:497]}..."
        slow_queries.append(
            {
                "query": text,
                "calls": int(record.get("calls", 0)),
                "mean_time_ms": mean_time,
                "total_time_ms": float(record.get("total_time", 0.0)),
                "rows": int(record.get("rows", 0)),
                "shared_blocks_hit": int(record.get("shared_blks_hit", 0)),
                "shared_blocks_read": int(record.get("shared_blks_read", 0)),
            }
        )

    return slow_queries
