"""Database optimization endpoints."""

import time
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .database_monitor import get_database_monitor, OptimizationType

router = APIRouter()


@router.get("/v1/admin/db/optimizations")
async def get_optimization_suggestions(
    request: Request,
    limit: int = Query(20, description="Number of suggestions to return"),
    min_impact: float = Query(0.5, description="Minimum impact score (0-1)"),
    suggestion_type: Optional[str] = Query(None, description="Filter by suggestion type"),
) -> Dict[str, str]:
    """Get database optimization suggestions."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()
        suggestions = await monitor.get_optimization_suggestions(
            limit=limit,
            min_impact=min_impact
        )

        # Filter by type if specified
        if suggestion_type:
            type_enum = OptimizationType(suggestion_type)
            suggestions = [s for s in suggestions if s.suggestion_type == type_enum]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_suggestions": len(suggestions),
                "filters": {
                    "min_impact": min_impact,
                    "suggestion_type": suggestion_type,
                }
            },
            "data": [
                {
                    "query_hash": s.query_hash,
                    "suggestion_type": s.suggestion_type.value,
                    "title": s.title,
                    "description": s.description,
                    "impact": s.impact,
                    "effort": s.effort,
                    "confidence": s.confidence,
                    "priority_score": s.get_priority_score(),
                    "created_at": s.created_at.isoformat(),
                    "implemented": s.implemented,
                }
                for s in suggestions
            ]
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get optimization suggestions: {str(exc)}"
        ) from exc


@router.post("/v1/admin/db/optimizations/{query_hash}/implement")
async def implement_optimization(
    request: Request,
    query_hash: str,
    suggestion_id: str,
) -> Dict[str, str]:
    """Mark an optimization suggestion as implemented."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()

        # Find the suggestion
        suggestion = None
        if query_hash in monitor.optimization_suggestions:
            for s in monitor.optimization_suggestions[query_hash]:
                if s.title.replace(" ", "_").lower() == suggestion_id.replace("-", "_"):
                    suggestion = s
                    break

        if not suggestion:
            raise HTTPException(status_code=404, detail=f"Suggestion {suggestion_id} not found")

        # Mark as implemented
        suggestion.implemented = True

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Optimization '{suggestion.title}' marked as implemented",
            "query_hash": query_hash,
            "suggestion_id": suggestion_id,
            "suggestion_type": suggestion.suggestion_type.value,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to implement optimization: {str(exc)}"
        ) from exc
