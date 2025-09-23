"""Database health check endpoints."""

import time
from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

from aurum.telemetry.context import get_request_id
from .database_monitor import get_database_monitor

router = APIRouter()


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
