"""Database connection management endpoints."""

import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .trino_client import get_trino_client

router = APIRouter()


@router.get("/v1/admin/db/connections")
async def get_database_connections(
    request: Request,
) -> Dict[str, Any]:
    """Get database connection pool statistics."""
    start_time = time.perf_counter()

    try:
        connections: Dict[str, Any] = {}

        # Trino connection pool metrics
        try:
            trino_client = get_trino_client()
            pool_metrics = await trino_client.get_pool_metrics()
            connections["trino"] = {
                "active": pool_metrics.get("active_connections", 0),
                "idle": pool_metrics.get("idle_connections", 0),
                "total": pool_metrics.get("total_connections", 0),
                "max": pool_metrics.get("max_connections", 0),
                "utilization": pool_metrics.get("pool_utilization", 0.0),
                "queue_depth": pool_metrics.get("request_queue_depth", 0),
                "queue_capacity": pool_metrics.get("queue_capacity", 0),
                "queue_utilization": pool_metrics.get("queue_utilization", 0.0),
            }
        except Exception as exc:  # pragma: no cover - defensive observation path
            connections["trino"] = {"error": str(exc)}

        aggregate = {
            "active": 0,
            "idle": 0,
            "total": 0,
            "max": 0,
        }

        for entry in connections.values():
            if isinstance(entry, dict) and "active" in entry:
                aggregate["active"] += int(entry.get("active", 0))
                aggregate["idle"] += int(entry.get("idle", 0))
                aggregate["total"] += int(entry.get("total", 0))
                aggregate["max"] += int(entry.get("max", 0))

        aggregate["utilization"] = (
            aggregate["active"] / aggregate["max"] if aggregate["max"] else 0.0
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "backends": list(connections.keys()),
            },
            "data": {
                "connections": connections,
                "summary": aggregate,
            },
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database connections: {str(exc)}"
        ) from exc


@router.get("/v1/admin/db/indexes")
async def get_database_indexes(
    request: Request,
    table_name: Optional[str] = Query(None, description="Filter by table name"),
) -> Dict[str, str]:
    """Get database index usage and recommendations."""
    start_time = time.perf_counter()

    try:
        # In a real implementation, this would query database metadata
        # For now, return mock data based on query patterns
        monitor = get_database_monitor()

        # Analyze query patterns to suggest indexes
        index_suggestions = []
        for pattern in monitor.query_patterns.values():
            if pattern.avg_time > 1.0:  # Slow queries
                # Extract potential index columns from WHERE clauses
                normalized_query = pattern.pattern_text.lower()
                if "where" in normalized_query:
                    # Simple heuristic to extract column names
                    where_start = normalized_query.find("where")
                    where_clause = normalized_query[where_start:]
                    # Look for column = value patterns
                    import re
                    columns = re.findall(r'(\w+)\s*=\s*\?', where_clause)
                    if columns:
                        index_suggestions.append({
                            "table": "unknown",  # Would be determined from actual schema
                            "columns": columns[:3],  # Limit to 3 columns for composite index
                            "query_pattern": pattern.pattern_text,
                            "avg_execution_time": pattern.avg_time,
                            "total_executions": pattern.total_executions,
                            "impact_score": min(1.0, pattern.avg_time / 10.0),  # Higher time = higher impact
                        })

        # Sort by impact score
        index_suggestions.sort(key=lambda x: x["impact_score"], reverse=True)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_suggestions": len(index_suggestions),
            },
            "data": {
                "index_suggestions": index_suggestions[:20],  # Top 20 suggestions
                "unused_indexes": [],  # Would analyze index usage
                "redundant_indexes": [],  # Would detect duplicate indexes
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database indexes: {str(exc)}"
        ) from exc
