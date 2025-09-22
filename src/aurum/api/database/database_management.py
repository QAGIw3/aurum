"""Database optimization management endpoints for monitoring and performance analysis."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .database_monitor import get_database_monitor, QueryPerformanceLevel, OptimizationType
from ..cache.cache import CacheManager


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


@router.get("/v1/admin/db/connections")
async def get_database_connections(
    request: Request,
) -> Dict[str, str]:
    """Get database connection pool statistics."""
    start_time = time.perf_counter()

    try:
        # In a real implementation, this would query actual database connection pools
        # For now, return mock data
        connections_data = {
            "active_connections": 25,
            "idle_connections": 10,
            "total_connections": 35,
            "max_connections": 100,
            "connection_utilization": 0.35,
            "average_connection_age": 1800,  # 30 minutes
            "connection_errors": 2,
            "connection_timeouts": 1,
            "database_engines": ["trino", "postgresql"],
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": connections_data
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


@router.get("/v1/admin/db/alerts")
async def get_database_alerts(
    request: Request,
    hours: int = Query(24, description="Time range in hours"),
) -> Dict[str, str]:
    """Get database performance alerts and issues."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()

        # Get performance summary
        performance = await monitor.get_performance_summary()

        # Generate alerts based on thresholds
        alerts = []
        issues = []

        # Slow query rate alert
        slow_query_rate = performance.get("slow_query_percentage", 0)
        if slow_query_rate > 10:  # More than 10% slow queries
            alerts.append({
                "level": "warning",
                "type": "slow_query_rate",
                "message": f"High slow query rate: {slow_query_rate:.1f}%",
                "threshold": "10%",
                "current_value": f"{slow_query_rate:.1f}%",
            })
            issues.append("High percentage of slow queries detected")

        # Average query time alert
        avg_time = performance.get("average_query_time", 0)
        if avg_time > 1.0:  # Average > 1 second
            alerts.append({
                "level": "warning",
                "type": "average_query_time",
                "message": f"High average query time: {avg_time:.3f}s",
                "threshold": "1.0s",
                "current_value": f"{avg_time:.3f}s",
            })
            issues.append("Average query execution time is high")

        # Connection issues (mock data)
        connection_issues = [
            {
                "level": "info",
                "type": "connection_pool",
                "message": "Connection pool utilization at 35%",
                "threshold": "80%",
                "current_value": "35%",
            }
        ]

        alerts.extend(connection_issues)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "time_range_hours": hours,
            },
            "data": {
                "alerts": alerts,
                "issues": issues,
                "total_alerts": len(alerts),
                "performance_summary": performance,
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database alerts: {str(exc)}"
        ) from exc


@router.get("/v1/admin/db/recommendations")
async def get_database_recommendations(
    request: Request,
) -> Dict[str, str]:
    """Get comprehensive database optimization recommendations."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()

        # Get performance data
        performance = await monitor.get_performance_summary()
        patterns = await monitor.get_query_patterns(limit=10)
        suggestions = await monitor.get_optimization_suggestions(limit=10)

        # Generate recommendations
        recommendations = []

        # Query optimization recommendations
        if performance.get("slow_query_percentage", 0) > 5:
            recommendations.append({
                "category": "query_optimization",
                "priority": "high",
                "title": "Optimize Slow Queries",
                "description": f"{performance.get('slow_queries', 0)} slow queries detected. Review and optimize the slowest performing queries.",
                "actions": [
                    "Review the top slow queries in the performance dashboard",
                    "Add appropriate indexes for frequently filtered columns",
                    "Consider query rewriting for complex queries",
                    "Use materialized views for expensive aggregations"
                ]
            })

        # Index recommendations
        if len(patterns) > 0:
            recommendations.append({
                "category": "indexing",
                "priority": "medium",
                "title": "Database Indexing Strategy",
                "description": "Analyze query patterns and ensure proper indexing for optimal performance.",
                "actions": [
                    "Review the index suggestions in the indexes dashboard",
                    "Add composite indexes for queries with multiple WHERE conditions",
                    "Remove unused indexes to reduce maintenance overhead",
                    "Consider partial indexes for queries with common filter values"
                ]
            })

        # Connection pool recommendations
        recommendations.append({
            "category": "connection_management",
            "priority": "medium",
            "title": "Connection Pool Optimization",
            "description": "Optimize database connection pool settings for better performance and resource utilization.",
            "actions": [
                "Monitor connection pool utilization",
                "Adjust pool size based on application load",
                "Configure connection timeouts appropriately",
                "Implement connection retry logic for transient failures"
            ]
        })

        # Caching recommendations
        if performance.get("average_query_time", 0) > 0.5:
            recommendations.append({
                "category": "caching",
                "priority": "medium",
                "title": "Implement Query Result Caching",
                "description": "Cache results of frequently executed queries to improve performance.",
                "actions": [
                    "Identify frequently executed queries with consistent results",
                    "Implement appropriate cache TTL values",
                    "Use cache warming for critical queries",
                    "Monitor cache hit rates and adjust strategy"
                ]
            })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "recommendations": recommendations,
                "performance_metrics": performance,
                "top_patterns": [
                    {
                        "query_hash": p.query_hash,
                        "avg_time": p.avg_time,
                        "performance_score": p.get_performance_score(),
                    }
                    for p in patterns[:5]
                ],
                "top_suggestions": [
                    {
                        "type": s.suggestion_type.value,
                        "title": s.title,
                        "impact": s.impact,
                        "effort": s.effort,
                    }
                    for s in suggestions[:5]
                ]
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database recommendations: {str(exc)}"
        ) from exc


@router.get("/v1/admin/db/health")
async def get_database_health(
    request: Request,
) -> Dict[str, str]:
    """Get comprehensive database health status."""
    start_time = time.perf_counter()

    try:
        monitor = get_database_monitor()

        # Get monitoring stats
        monitor_stats = await monitor.get_monitoring_stats()
        performance = await monitor.get_performance_summary()

        # Calculate health score
        health_score = 100.0
        health_issues = []
        recommendations = []

        # Check slow query rate
        slow_rate = performance.get("slow_query_percentage", 0)
        if slow_rate > 20:
            health_score -= 30
            health_issues.append("Critical slow query rate")
        elif slow_rate > 10:
            health_score -= 15
            health_issues.append("High slow query rate")
        elif slow_rate > 5:
            health_score -= 5
            recommendations.append("Monitor slow query trends")

        # Check average query time
        avg_time = performance.get("average_query_time", 0)
        if avg_time > 5.0:
            health_score -= 25
            health_issues.append("Very high average query time")
        elif avg_time > 2.0:
            health_score -= 15
            health_issues.append("High average query time")
        elif avg_time > 1.0:
            health_score -= 5
            recommendations.append("Consider query optimization")

        # Check error rates in patterns
        error_patterns = [
            p for p in monitor.query_patterns.values()
            if p.error_count / max(p.total_executions, 1) > 0.1
        ]
        if error_patterns:
            health_score -= 10
            health_issues.append(f"Query patterns with high error rates: {len(error_patterns)}")

        # Determine health status
        if health_score >= 90:
            health_status = "excellent"
        elif health_score >= 75:
            health_status = "good"
        elif health_score >= 50:
            health_status = "fair"
        elif health_score >= 25:
            health_status = "poor"
        else:
            health_status = "critical"

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": health_status,
            "health_score": round(health_score, 1),
            "database_monitor": monitor_stats,
            "performance_summary": performance,
            "health_issues": health_issues,
            "recommendations": recommendations,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Database health check failed: {str(exc)}"
        ) from exc
