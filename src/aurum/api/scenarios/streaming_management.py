"""Streaming system management endpoints for administrators."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .websocket_manager import WebSocketManager
from .streaming_manager import get_streaming_manager, StreamConfig
from .rate_limiting import create_rate_limit_manager


router = APIRouter()


@router.get("/v1/admin/streaming/streams")
async def list_streaming_streams(
    request: Request,
    stream_type: Optional[str] = Query(None, description="Filter by stream type"),
) -> Dict[str, List[Dict[str, str]]]:
    """List all data streams with their status."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        stream_ids = streaming_manager.list_streams()

        streams_data = []
        for stream_id in stream_ids:
            stream = streaming_manager.get_stream(stream_id)
            if stream:
                stats = await stream.get_stats()
                stream_data = {
                    "stream_id": stream_id,
                    "data_types": ", ".join(stream.config.data_types),
                    "subscriber_count": stream.subscriber_count,
                    "message_count": stream.message_count,
                    "error_count": stream.error_count,
                    "running": stream.running,
                    "update_interval": stream.config.update_interval,
                    "max_subscribers": stream.config.max_subscribers,
                    "enable_history": stream.config.enable_history,
                    "history_size": len(stream.history),
                    "last_update": stream.last_update.isoformat(),
                }
                streams_data.append(stream_data)

        # Filter by stream type if provided
        if stream_type:
            streams_data = [s for s in streams_data if stream_type in s["data_types"]]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_streams": len(streams_data),
            },
            "data": streams_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list streams: {str(exc)}"
        ) from exc


@router.get("/v1/admin/streaming/streams/{stream_id}")
async def get_streaming_stream_info(
    request: Request,
    stream_id: str,
) -> Dict[str, str]:
    """Get detailed information about a specific stream."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        stream = streaming_manager.get_stream(stream_id)

        if not stream:
            raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

        stats = await stream.get_stats()
        history = await stream.get_history(limit=10)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                **stats,
                "config": {
                    "stream_id": stream.config.stream_id,
                    "data_types": stream.config.data_types,
                    "update_interval": stream.config.update_interval,
                    "max_subscribers": stream.config.max_subscribers,
                    "enable_history": stream.config.enable_history,
                    "history_duration": stream.config.history_duration,
                    "filters": stream.config.filters,
                },
                "recent_history": history,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get stream info: {str(exc)}"
        ) from exc


@router.post("/v1/admin/streaming/streams")
async def create_streaming_stream(
    request: Request,
    stream_data: Dict[str, str],
) -> Dict[str, str]:
    """Create a new data stream."""
    start_time = time.perf_counter()

    try:
        # Validate required fields
        required_fields = ["stream_id", "data_types"]
        for field in required_fields:
            if field not in stream_data:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required field: {field}"
                )

        # Create stream configuration
        config = StreamConfig(
            stream_id=stream_data["stream_id"],
            data_types=stream_data["data_types"].split(",") if isinstance(stream_data["data_types"], str) else stream_data["data_types"],
            update_interval=stream_data.get("update_interval", 1.0),
            max_subscribers=stream_data.get("max_subscribers", 1000),
            enable_history=stream_data.get("enable_history", True),
            history_duration=stream_data.get("history_duration", 3600),
            filters=stream_data.get("filters", {})
        )

        streaming_manager = get_streaming_manager()

        # Check if stream already exists
        if streaming_manager.get_stream(config.stream_id):
            raise HTTPException(
                status_code=409,
                detail=f"Stream {config.stream_id} already exists"
            )

        # Register the stream configuration
        await streaming_manager.register_stream_config(config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Stream {config.stream_id} created successfully",
            "stream_id": config.stream_id,
            "data_types": config.data_types,
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
            detail=f"Failed to create stream: {str(exc)}"
        ) from exc


@router.delete("/v1/admin/streaming/streams/{stream_id}")
async def delete_streaming_stream(
    request: Request,
    stream_id: str,
) -> Dict[str, str]:
    """Delete a data stream."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        success = await streaming_manager.delete_stream(stream_id)

        if not success:
            raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Stream {stream_id} deleted successfully",
            "stream_id": stream_id,
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
            detail=f"Failed to delete stream: {str(exc)}"
        ) from exc


@router.get("/v1/admin/streaming/connections")
async def list_websocket_connections(
    request: Request,
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    state: Optional[str] = Query(None, description="Filter by connection state"),
) -> Dict[str, str]:
    """List WebSocket connections with filtering."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        ws_stats = await websocket_manager.get_stats()

        # Get connection details
        connections_data = []
        for connection_id, connection in websocket_manager.active_connections.items():
            conn_data = {
                "connection_id": connection_id,
                "user_id": connection.user_id,
                "tenant_id": connection.tenant_id,
                "state": connection.state.value,
                "authenticated": connection.authenticated,
                "subscription_count": len(connection.subscriptions),
                "created_at": connection.created_at.isoformat(),
                "last_activity": connection.last_activity.isoformat(),
                "message_count": connection.message_count,
                "client_info": connection.client_info,
            }
            connections_data.append(conn_data)

        # Apply filters
        if user_id:
            connections_data = [c for c in connections_data if c["user_id"] == user_id]
        if tenant_id:
            connections_data = [c for c in connections_data if c["tenant_id"] == tenant_id]
        if state:
            connections_data = [c for c in connections_data if c["state"] == state]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_connections": len(connections_data),
                "websocket_stats": ws_stats,
            },
            "data": connections_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list connections: {str(exc)}"
        ) from exc


@router.get("/v1/admin/streaming/connections/{connection_id}")
async def get_websocket_connection_info(
    request: Request,
    connection_id: str,
) -> Dict[str, str]:
    """Get detailed information about a specific WebSocket connection."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        connection = websocket_manager.get_connection(connection_id)

        if not connection:
            raise HTTPException(status_code=404, detail=f"Connection {connection_id} not found")

        # Get subscription details
        subscriptions = []
        for stream_id, subscription in connection.subscriptions.items():
            subscriptions.append({
                "stream_id": stream_id,
                "filters": subscription.filters,
                "data_types": subscription.data_types,
                "created_at": subscription.created_at.isoformat(),
                "last_activity": subscription.last_activity.isoformat(),
            })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "connection_id": connection_id,
                "user_id": connection.user_id,
                "tenant_id": connection.tenant_id,
                "state": connection.state.value,
                "authenticated": connection.authenticated,
                "created_at": connection.created_at.isoformat(),
                "last_activity": connection.last_activity.isoformat(),
                "message_count": connection.message_count,
                "client_info": connection.client_info,
                "subscriptions": subscriptions,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get connection info: {str(exc)}"
        ) from exc


@router.delete("/v1/admin/streaming/connections/{connection_id}")
async def close_websocket_connection(
    request: Request,
    connection_id: str,
) -> Dict[str, str]:
    """Close a specific WebSocket connection."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        connection = websocket_manager.get_connection(connection_id)

        if not connection:
            raise HTTPException(status_code=404, detail=f"Connection {connection_id} not found")

        await websocket_manager.remove_connection(connection_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Connection {connection_id} closed successfully",
            "connection_id": connection_id,
            "user_id": connection.user_id,
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
            detail=f"Failed to close connection: {str(exc)}"
        ) from exc


@router.get("/v1/admin/streaming/statistics")
async def get_streaming_statistics(
    request: Request,
    time_range: str = Query("1h", description="Time range (1h, 24h, 7d)"),
) -> Dict[str, str]:
    """Get comprehensive streaming system statistics."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        streaming_manager = get_streaming_manager()

        ws_stats = await websocket_manager.get_stats()
        stream_stats = await streaming_manager.get_all_stats()

        # Calculate derived statistics
        total_connections = ws_stats.get("total_connections", 0)
        total_subscribers = stream_stats.get("total_subscribers", 0)
        total_messages = stream_stats.get("total_messages", 0)
        connection_utilization = total_connections / ws_stats.get("max_connections", 1000)

        # Connection state distribution
        state_distribution = ws_stats.get("connections_by_state", {})

        # Stream performance
        streams_by_type = {}
        for stream_id, stats in stream_stats.get("streams", {}).items():
            stream = streaming_manager.get_stream(stream_id)
            if stream:
                for data_type in stream.config.data_types:
                    if data_type not in streams_by_type:
                        streams_by_type[data_type] = []
                    streams_by_type[data_type].append({
                        "stream_id": stream_id,
                        "subscriber_count": stats.get("subscriber_count", 0),
                        "message_count": stats.get("message_count", 0),
                        "error_count": stats.get("error_count", 0),
                    })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "time_range": time_range,
            },
            "data": {
                "system_overview": {
                    "total_connections": total_connections,
                    "total_subscribers": total_subscribers,
                    "total_messages": total_messages,
                    "connection_utilization": connection_utilization,
                    "websocket_running": ws_stats.get("running", False),
                    "streaming_running": stream_stats.get("running", False),
                },
                "connection_stats": {
                    "by_state": state_distribution,
                    "authenticated": ws_stats.get("authenticated_connections", 0),
                    "subscribed": ws_stats.get("subscribed_connections", 0),
                },
                "streaming_stats": {
                    "total_streams": stream_stats.get("total_streams", 0),
                    "total_errors": stream_stats.get("total_errors", 0),
                    "by_data_type": streams_by_type,
                },
                "performance_metrics": {
                    "avg_messages_per_connection": total_messages / max(total_connections, 1),
                    "avg_subscribers_per_stream": total_subscribers / max(stream_stats.get("total_streams", 1), 1),
                    "connection_health_score": min(100, connection_utilization * 100 * 1.5),  # Score based on utilization
                }
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get streaming statistics: {str(exc)}"
        ) from exc


@router.get("/v1/admin/streaming/health")
async def get_streaming_system_health(
    request: Request,
) -> Dict[str, str]:
    """Get detailed streaming system health status."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        streaming_manager = get_streaming_manager()

        ws_stats = await websocket_manager.get_stats()
        stream_stats = await streaming_manager.get_all_stats()

        # Determine overall health
        health_status = "healthy"
        issues = []
        recommendations = []

        # Check WebSocket manager health
        if not ws_stats.get("running", False):
            health_status = "unhealthy"
            issues.append("WebSocket manager is not running")

        # Check streaming manager health
        if not stream_stats.get("running", False):
            health_status = "unhealthy"
            issues.append("Streaming manager is not running")

        # Check connection limits
        connection_usage = ws_stats.get("total_connections", 0) / max(ws_stats.get("max_connections", 1000), 1)
        if connection_usage > 0.9:
            health_status = "warning" if health_status == "healthy" else health_status
            issues.append("High connection utilization")
            recommendations.append("Consider increasing max_connections limit")

        # Check for error rates
        total_errors = stream_stats.get("total_errors", 0)
        total_messages = stream_stats.get("total_messages", 0)
        error_rate = total_errors / max(total_messages, 1)

        if error_rate > 0.1:  # 10% error rate
            health_status = "warning" if health_status == "healthy" else health_status
            issues.append("High error rate in streams")
            recommendations.append("Investigate stream error sources")

        # Check for inactive streams
        inactive_streams = 0
        for stream_id, stats in stream_stats.get("streams", {}).items():
            stream = streaming_manager.get_stream(stream_id)
            if stream and not stats.get("running", False):
                inactive_streams += 1

        if inactive_streams > 0:
            issues.append(f"{inactive_streams} inactive streams detected")
            recommendations.append("Restart inactive streams")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": health_status,
            "websocket_manager": {
                "status": "healthy" if ws_stats.get("running", False) else "unhealthy",
                "connections": ws_stats.get("total_connections", 0),
                "max_connections": ws_stats.get("max_connections", 1000),
                "connection_utilization": connection_usage,
            },
            "streaming_manager": {
                "status": "healthy" if stream_stats.get("running", False) else "unhealthy",
                "total_streams": stream_stats.get("total_streams", 0),
                "total_subscribers": stream_stats.get("total_subscribers", 0),
                "total_messages": stream_stats.get("total_messages", 0),
                "total_errors": stream_stats.get("total_errors", 0),
                "error_rate": error_rate,
            },
            "issues": issues,
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
            detail=f"Streaming health check failed: {str(exc)}"
        ) from exc


@router.post("/v1/admin/streaming/broadcast")
async def broadcast_to_streaming_system(
    request: Request,
    broadcast_data: Dict[str, str],
) -> Dict[str, str]:
    """Broadcast a message to all or specific streaming connections."""
    start_time = time.perf_counter()

    try:
        target = broadcast_data.get("target", "all")  # all, user, tenant
        message = broadcast_data.get("message", "")
        user_id = broadcast_data.get("user_id")
        tenant_id = broadcast_data.get("tenant_id")

        if not message:
            raise HTTPException(status_code=400, detail="Message is required")

        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))

        if target == "all":
            # Broadcast to all connections (system-wide announcement)
            # This would require additional implementation
            pass
        elif target == "user" and user_id:
            await websocket_manager.broadcast_to_user(user_id, {"message": message})
        elif target == "tenant" and tenant_id:
            await websocket_manager.broadcast_to_tenant(tenant_id, {"message": message})
        else:
            raise HTTPException(status_code=400, detail="Invalid target or missing parameters")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Broadcast sent successfully",
            "target": target,
            "message_length": len(message),
            "user_id": user_id,
            "tenant_id": tenant_id,
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
            detail=f"Failed to broadcast message: {str(exc)}"
        ) from exc


@router.post("/v1/admin/streaming/cleanup")
async def cleanup_streaming_system(
    request: Request,
) -> Dict[str, str]:
    """Clean up inactive connections and streams."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))

        # Get stats before cleanup
        before_stats = await websocket_manager.get_stats()

        # Cleanup connections (this is handled by the cleanup loop)
        # For manual cleanup, we can force it here
        cleanup_count = 0

        # Clean up rate limiter states
        rate_limiter = create_rate_limit_manager("memory")
        rate_limiter_cleanup = await rate_limiter.cleanup()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Streaming system cleanup completed",
            "websocket_connections_before": before_stats.get("total_connections", 0),
            "rate_limiter_states_cleaned": rate_limiter_cleanup,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Streaming cleanup failed: {str(exc)}"
        ) from exc
