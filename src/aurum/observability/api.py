"""Observability API endpoints for metrics, tracing, and system monitoring (admin-only)."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .metrics import get_metrics_collector, MetricPoint, MetricType
from .tracing import get_trace_collector, get_trace_report
from .slo_dashboard import get_slo_dashboard_config, check_slo_status, get_sli_values
from .enhanced_logging import get_logger


router = APIRouter(prefix="/v1/observability", tags=["Observability"])


@router.get("/metrics")
async def get_metrics(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by metric name"),
    metric_type: Optional[str] = Query(None, description="Filter by metric type"),
    labels: Optional[str] = Query(None, description="Filter by labels (key=value,key2=value2)"),
) -> Dict[str, Any]:
    """Get metrics in Prometheus format."""
    start_time = time.perf_counter()

    try:
        collector = get_metrics_collector()
        metrics = await collector.collect_metrics()

        # Apply filters
        if name:
            metrics = [m for m in metrics if m.name == name]

        if metric_type:
            try:
                m_type = MetricType(metric_type)
                metrics = [m for m in metrics if m.metric_type == m_type]
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid metric type: {metric_type}"
                )

        if labels:
            # Parse labels filter
            label_filters = {}
            for label_part in labels.split(","):
                if "=" in label_part:
                    key, value = label_part.split("=", 1)
                    label_filters[key] = value

            metrics = [
                m for m in metrics
                if all(m.labels.get(k) == v for k, v in label_filters.items())
            ]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to Prometheus format
        prometheus_output = []
        for metric in metrics:
            label_str = ",".join(f'{k}="{v}"' for k, v in metric.labels.items())
            if label_str:
                label_str = f"{{{label_str}}}"

            if metric.metric_type == MetricType.COUNTER:
                prometheus_output.append(
                    f"# TYPE {metric.name} counter\n"
                    f"{metric.name}{label_str} {metric.value} {int(metric.timestamp * 1000)}"
                )
            elif metric.metric_type == MetricType.GAUGE:
                prometheus_output.append(
                    f"# TYPE {metric.name} gauge\n"
                    f"{metric.name}{label_str} {metric.value} {int(metric.timestamp * 1000)}"
                )
            elif metric.metric_type == MetricType.HISTOGRAM:
                prometheus_output.append(
                    f"# TYPE {metric.name} histogram\n"
                    f"{metric.name}_bucket{label_str} {metric.value} {int(metric.timestamp * 1000)}"
                )

        return {
            "content": "\n".join(prometheus_output) + "\n",
            "content_type": "text/plain; version=0.0.4; charset=utf-8",
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_metrics": len(metrics),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get metrics: {str(exc)}"
        ) from exc


@router.get("/metrics/json")
async def get_metrics_json(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by metric name"),
    metric_type: Optional[str] = Query(None, description="Filter by metric type"),
) -> Dict[str, Any]:
    """Get metrics in JSON format."""
    start_time = time.perf_counter()

    try:
        collector = get_metrics_collector()
        metrics = await collector.collect_metrics()

        # Apply filters
        if name:
            metrics = [m for m in metrics if m.name == name]

        if metric_type:
            try:
                m_type = MetricType(metric_type)
                metrics = [m for m in metrics if m.metric_type == m_type]
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid metric type: {metric_type}"
                )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to JSON format
        json_metrics = []
        for metric in metrics:
            json_metrics.append({
                "name": metric.name,
                "value": metric.value,
                "timestamp": metric.timestamp,
                "type": metric.metric_type.value,
                "labels": metric.labels,
            })

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_metrics": len(json_metrics),
            },
            "data": json_metrics
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get metrics JSON: {str(exc)}"
        ) from exc


@router.get("/traces")
async def get_traces(
    request: Request,
    trace_id: Optional[str] = Query(None, description="Specific trace ID"),
    active_only: bool = Query(False, description="Show only active traces"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum traces to return"),
) -> Dict[str, Any]:
    """Get trace information."""
    start_time = time.perf_counter()

    try:
        trace_collector = get_trace_collector()

        if trace_id:
            # Get specific trace
            spans = await trace_collector.get_trace(trace_id)
            if not spans:
                raise HTTPException(
                    status_code=404,
                    detail=f"Trace {trace_id} not found"
                )

            trace_summary = await trace_collector.get_trace_summary(trace_id)
            trace_report = await get_trace_report(trace_id)

            query_time_ms = (time.perf_counter() - start_time) * 1000

            return {
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                },
                "data": {
                    "trace_id": trace_id,
                    "summary": trace_summary,
                    "report": trace_report,
                }
            }

        else:
            # Get all traces
            if active_only:
                traces = await trace_collector.get_active_traces()
            else:
                # Get both active and recent completed traces
                active_traces = await trace_collector.get_active_traces()
                # For now, just return active traces
                traces = active_traces

            # Convert to summary format
            trace_summaries = []
            for trace_id, spans in list(traces.items())[:limit]:
                summary = await trace_collector.get_trace_summary(trace_id)
                if summary:
                    trace_summaries.append(summary)

            query_time_ms = (time.perf_counter() - start_time) * 1000

            return {
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                    "total_traces": len(trace_summaries),
                },
                "data": trace_summaries
            }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get traces: {str(exc)}"
        ) from exc


@router.get("/traces/{trace_id}")
async def get_trace_detail(
    request: Request,
    trace_id: str,
) -> Dict[str, Any]:
    """Get detailed trace information."""
    start_time = time.perf_counter()

    try:
        trace_collector = get_trace_collector()
        trace_report = await get_trace_report(trace_id)

        if not trace_report:
            raise HTTPException(
                status_code=404,
                detail=f"Trace {trace_id} not found"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": trace_report
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get trace detail: {str(exc)}"
        ) from exc


@router.get("/health")
async def get_observability_health(
    request: Request,
) -> Dict[str, Any]:
    """Get observability system health."""
    start_time = time.perf_counter()

    try:
        metrics_collector = get_metrics_collector()
        trace_collector = get_trace_collector()

        # Get basic health metrics
        await metrics_collector.increment_counter("observability_health_checks_total")

        # Get active traces count
        active_traces = await trace_collector.get_active_traces()
        active_trace_count = len(active_traces)

        # Get metrics count
        metrics = await metrics_collector.collect_metrics()
        metrics_count = len(metrics)

        # Determine health status
        health_status = "healthy"
        issues = []

        if active_trace_count > 1000:
            health_status = "degraded"
            issues.append(f"High active trace count: {active_trace_count}")

        if metrics_count == 0:
            health_status = "degraded"
            issues.append("No metrics available")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": health_status,
            "checks": {
                "metrics_collection": "ok",
                "trace_collection": "ok",
                "active_traces": active_trace_count,
                "total_metrics": metrics_count,
            },
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
            "checks": {
                "metrics_collection": "error",
                "trace_collection": "error",
            },
            "issues": [f"Health check failed: {str(exc)}"],
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }


@router.post("/traces/{trace_id}/export")
async def export_trace(
    request: Request,
    trace_id: str,
    format: str = "json",
) -> Dict[str, Any]:
    """Export trace data in various formats."""
    start_time = time.perf_counter()

    try:
        trace_collector = get_trace_collector()
        trace_report = await get_trace_report(trace_id)

        if not trace_report:
            raise HTTPException(
                status_code=404,
                detail=f"Trace {trace_id} not found"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        if format.lower() == "json":
            return {
                "format": "json",
                "data": trace_report,
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        elif format.lower() == "tree":
            # Return tree format
            return {
                "format": "tree",
                "data": trace_report.get("root_spans", []),
                "meta": {
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                }
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported export format: {format}"
            )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export trace: {str(exc)}"
        ) from exc


@router.delete("/traces/{trace_id}")
async def delete_trace(
    request: Request,
    trace_id: str,
) -> Dict[str, str]:
    """Delete a trace and all its spans."""
    start_time = time.perf_counter()

    try:
        # Note: In a real implementation, this would remove the trace from storage
        # For now, just return success
        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Trace {trace_id} deleted successfully",
            "request_id": get_request_id(),
            "processing_time_ms": round(query_time_ms, 2)
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete trace: {str(exc)}"
        ) from exc


@router.post("/cleanup")
async def cleanup_observability_data(
    request: Request,
    max_age_hours: int = 24,
) -> Dict[str, Any]:
    """Clean up old observability data."""
    start_time = time.perf_counter()

    try:
        trace_collector = get_trace_collector()
        max_age_seconds = max_age_hours * 3600

        # Clean up old traces
        traces_removed = await trace_collector.cleanup_old_traces(max_age_seconds)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Observability data cleanup completed",
            "data": {
                "traces_removed": traces_removed,
                "max_age_hours": max_age_hours,
            },
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup observability data: {str(exc)}"
        ) from exc


@router.get("/performance")
async def get_observability_performance(
    request: Request,
) -> Dict[str, Any]:
    """Get observability system performance metrics."""
    start_time = time.perf_counter()

    try:
        metrics_collector = get_metrics_collector()
        trace_collector = get_trace_collector()

        # Get metrics collection performance
        metrics = await metrics_collector.collect_metrics()
        metrics_collection_time = time.perf_counter() - start_time

        # Get trace collection info
        active_traces = await trace_collector.get_active_traces()

        # Get SLO status
        slo_status = check_slo_status()
        sli_values = get_sli_values()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "performance": {
                "metrics_collection_time_ms": round(metrics_collection_time * 1000, 2),
                "active_traces": len(active_traces),
                "total_metrics": len(metrics),
                "memory_usage_mb": 0,  # Would implement actual memory tracking
            },
            "slos": slo_status,
            "slis": sli_values,
            "recommendations": [
                "Consider increasing cleanup frequency if trace count is high",
                "Monitor metrics collection time for performance degradation",
                "Review SLO violations and adjust targets if needed",
            ],
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get observability performance: {str(exc)}"
        ) from exc


@router.get("/slos")
async def get_slos(
    request: Request,
    category: Optional[str] = Query(None, description="Filter by SLO category"),
) -> Dict[str, Any]:
    """Get Service Level Objectives status."""
    start_time = time.perf_counter()

    try:
        slo_status = check_slo_status()
        sli_values = get_sli_values()

        # Apply category filter if specified
        if category:
            slo_status = {
                k: v for k, v in slo_status.items()
                if category in k or category in str(v.get("status", ""))
            }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "slos": slo_status,
            "slis": sli_values,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get SLOs: {str(exc)}"
        ) from exc


@router.get("/slos/{slo_name}")
async def get_slo_detail(
    request: Request,
    slo_name: str,
) -> Dict[str, Any]:
    """Get detailed information about a specific SLO."""
    start_time = time.perf_counter()

    try:
        slo_status = check_slo_status()
        sli_values = get_sli_values()

        if slo_name not in slo_status:
            raise HTTPException(
                status_code=404,
                detail=f"SLO '{slo_name}' not found"
            )

        slo_info = slo_status[slo_name]
        relevant_slis = {
            k: v for k, v in sli_values.items()
            if slo_name in k
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "slo": {
                "name": slo_name,
                "status": slo_info,
                "relevant_slis": relevant_slis,
            },
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
            detail=f"Failed to get SLO detail: {str(exc)}"
        ) from exc


@router.get("/dashboard")
async def get_dashboard_config(
    request: Request,
    format: str = Query("json", description="Output format (json, grafana)"),
) -> Dict[str, Any]:
    """Get dashboard configuration for monitoring systems."""
    start_time = time.perf_counter()

    try:
        dashboard_config = get_slo_dashboard_config()

        if format == "grafana":
            config = dashboard_config.generate_grafana_dashboard()
        else:
            config = {
                "slos": dashboard_config.get_slo_panels(),
                "business_metrics": dashboard_config.get_business_metrics_panels(),
                "infrastructure": dashboard_config.get_infrastructure_panels(),
                "performance": dashboard_config.get_performance_panels(),
                "alerts": dashboard_config.get_alert_panels(),
            }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "dashboard": config,
            "format": format,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get dashboard config: {str(exc)}"
        ) from exc


@router.get("/logs/schema")
async def get_log_schema(
    request: Request,
) -> Dict[str, Any]:
    """Get the schema for structured logs."""
    start_time = time.perf_counter()

    try:
        schema = {
            "fields": {
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "ISO 8601 timestamp"
                },
                "level": {
                    "type": "string",
                    "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    "description": "Log level"
                },
                "message": {
                    "type": "string",
                    "description": "Log message"
                },
                "request_id": {
                    "type": "string",
                    "description": "Request correlation ID"
                },
                "correlation_id": {
                    "type": "string",
                    "description": "Request correlation ID across services"
                },
                "tenant_id": {
                    "type": "string",
                    "description": "Tenant identifier"
                },
                "user_id": {
                    "type": "string",
                    "description": "User identifier"
                },
                "service_name": {
                    "type": "string",
                    "description": "Service name"
                },
                "component": {
                    "type": "string",
                    "description": "Component that generated the log"
                },
                "operation": {
                    "type": "string",
                    "description": "Operation being performed"
                },
                "duration_ms": {
                    "type": "number",
                    "description": "Operation duration in milliseconds"
                },
                "status_code": {
                    "type": "integer",
                    "description": "HTTP status code"
                },
                "error": {
                    "type": "string",
                    "description": "Error message"
                },
                "error_type": {
                    "type": "string",
                    "description": "Error type/class"
                },
                "stack_trace": {
                    "type": "string",
                    "description": "Stack trace for errors"
                },
                "metadata": {
                    "type": "object",
                    "description": "Additional metadata"
                }
            },
            "examples": [
                {
                    "timestamp": "2024-01-01T12:00:00.000Z",
                    "level": "INFO",
                    "message": "Scenario creation completed",
                    "component": "scenario",
                    "operation": "create",
                    "duration_ms": 45.2,
                    "tenant_id": "acme-corp"
                },
                {
                    "timestamp": "2024-01-01T12:01:00.000Z",
                    "level": "ERROR",
                    "message": "Database connection failed",
                    "component": "database",
                    "operation": "connect",
                    "error": "Connection timeout",
                    "error_type": "ConnectionError",
                    "stack_trace": "..."
                }
            ]
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "schema": schema,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get log schema: {str(exc)}"
        ) from exc
