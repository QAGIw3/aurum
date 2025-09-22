"""WebSocket and Server-Sent Events endpoints for real-time data streaming."""

from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from ..telemetry.context import get_request_id
from .websocket_manager import WebSocketManager, MessageType
from .streaming_manager import get_streaming_manager
from .rate_limiting import create_rate_limit_manager


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time data streaming."""
    start_time = time.perf_counter()

    # Create WebSocket manager if needed
    websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))

    # Accept connection
    connection = await websocket_manager.accept_connection(websocket)
    if not connection:
        await websocket.close(code=1013, reason="Too many connections")
        return

    try:
        # WebSocket connection loop
        await websocket_manager.start()

        while True:
            # Receive message from client
            message_text = await websocket.receive_text()

            # Update activity
            connection.last_activity = time.time()

            # Parse message
            try:
                import json
                message_data = json.loads(message_text)

                message_type = message_data.get("type", "")
                payload = message_data.get("payload", {})

                # Handle different message types
                if message_type == MessageType.AUTH.value:
                    await _handle_auth_message(connection, payload)
                elif message_type == MessageType.SUBSCRIBE.value:
                    await _handle_subscribe_message(connection, payload)
                elif message_type == MessageType.UNSUBSCRIBE.value:
                    await _handle_unsubscribe_message(connection, payload)
                elif message_type == MessageType.PING.value:
                    await connection.handle_ping()
                elif message_type == MessageType.PONG.value:
                    await connection.handle_pong()
                else:
                    await connection.send_error("Unknown message type", message_type)

            except json.JSONDecodeError:
                await connection.send_error("Invalid JSON message")
            except Exception as exc:
                await connection.send_error("Message processing error", str(exc))

    except WebSocketDisconnect:
        # Normal disconnection
        await websocket_manager.remove_connection(connection.connection_id)
    except Exception as exc:
        # Unexpected error
        await websocket_manager.remove_connection(connection.connection_id)
        print(f"WebSocket error: {exc}")


async def _handle_auth_message(connection, payload: dict):
    """Handle authentication message."""
    auth_data = {
        "user_id": payload.get("user_id"),
        "tenant_id": payload.get("tenant_id"),
        "api_key": payload.get("api_key"),
        "token": payload.get("token")
    }

    success = await connection.authenticate(auth_data)

    if success:
        await connection.send_message(MessageType.AUTH, {
            "status": "authenticated",
            "user_id": connection.user_id,
            "tenant_id": connection.tenant_id,
            "connection_id": connection.connection_id
        })
    else:
        await connection.send_error("Authentication failed", "Invalid credentials")


async def _handle_subscribe_message(connection, payload: dict):
    """Handle subscription message."""
    if not connection.authenticated:
        await connection.send_error("Not authenticated")
        return

    subscription_data = {
        "stream_id": payload.get("stream_id"),
        "filters": payload.get("filters", {}),
        "data_types": payload.get("data_types", ["curve_data"])
    }

    success = await connection.subscribe(subscription_data)

    if success:
        # Add to streaming manager
        streaming_manager = get_streaming_manager()
        if streaming_manager.websocket_manager == connection.websocket_manager:
            stream = streaming_manager.get_stream(subscription_data["stream_id"])
            if stream:
                await stream.subscribe(connection.connection_id)


async def _handle_unsubscribe_message(connection, payload: dict):
    """Handle unsubscription message."""
    stream_id = payload.get("stream_id")

    if not stream_id:
        await connection.send_error("Missing stream_id")
        return

    success = await connection.unsubscribe(stream_id)

    if success:
        # Remove from streaming manager
        streaming_manager = get_streaming_manager()
        if streaming_manager.websocket_manager == connection.websocket_manager:
            stream = streaming_manager.get_stream(stream_id)
            if stream:
                await stream.unsubscribe(connection.connection_id)


@router.get("/sse/{stream_id}")
async def sse_endpoint(
    request: Request,
    stream_id: str,
    user_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    """Server-Sent Events endpoint for real-time data streaming."""
    start_time = time.perf_counter()

    # Basic authentication check
    if not user_id or not tenant_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    # Get streaming manager
    streaming_manager = get_streaming_manager()

    # Check if stream exists
    stream = streaming_manager.get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

    async def event_generator():
        """Generate Server-Sent Events."""
        try:
            # Send initial connection event
            yield {
                "event": "connected",
                "data": json.dumps({
                    "stream_id": stream_id,
                    "user_id": user_id,
                    "tenant_id": tenant_id,
                    "timestamp": time.time(),
                    "request_id": get_request_id()
                })
            }

            # Send stream info
            yield {
                "event": "stream_info",
                "data": json.dumps({
                    "stream_id": stream_id,
                    "data_types": stream.config.data_types,
                    "update_interval": stream.config.update_interval,
                    "enable_history": stream.config.enable_history,
                })
            }

            # Subscribe to stream for this request
            request_id = get_request_id()

            # Create a simple subscription mechanism for SSE
            # In a real implementation, this would track SSE connections
            last_message_id = 0

            while True:
                # Check if client is still connected
                if await request.is_disconnected():
                    break

                # Get recent data from stream
                history = await stream.get_history(limit=10)

                for item in history:
                    if item.get("data", {}).get("message_id", 0) > last_message_id:
                        yield {
                            "event": "data",
                            "data": json.dumps({
                                "stream_id": stream_id,
                                "data": item["data"],
                                "timestamp": item["timestamp"].isoformat(),
                                "subscriber_count": stream.subscriber_count,
                                "request_id": request_id
                            }),
                            "id": str(item.get("data", {}).get("message_id", 0))
                        }

                        last_message_id = item.get("data", {}).get("message_id", 0)

                # Wait before next check
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            # Client disconnected
            pass
        except Exception as exc:
            # Send error event
            yield {
                "event": "error",
                "data": json.dumps({
                    "error": str(exc),
                    "stream_id": stream_id,
                    "request_id": request_id
                })
            }

    return EventSourceResponse(
        event_generator(),
        headers={
            "X-Accel-Buffering": "no",  # Disable proxy buffering
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@router.get("/ws/health")
async def websocket_health():
    """Get WebSocket system health status."""
    start_time = time.perf_counter()

    try:
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        stats = await websocket_manager.get_stats()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": "healthy" if stats["running"] else "unhealthy",
            "websocket_manager": stats,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"WebSocket health check failed: {str(exc)}"
        ) from exc


@router.get("/streams")
async def list_streams():
    """List all available data streams."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        streams = streaming_manager.list_streams()

        # Get detailed info for each stream
        stream_info = []
        for stream_id in streams:
            stream = streaming_manager.get_stream(stream_id)
            if stream:
                stats = await stream.get_stats()
                stream_info.append({
                    "stream_id": stream_id,
                    "data_types": stream.config.data_types,
                    "subscriber_count": stream.subscriber_count,
                    "message_count": stream.message_count,
                    "running": stream.running,
                    "enable_history": stream.config.enable_history,
                    "update_interval": stream.config.update_interval,
                })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_streams": len(stream_info),
            },
            "data": stream_info
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list streams: {str(exc)}"
        ) from exc


@router.get("/streams/{stream_id}")
async def get_stream_info(stream_id: str):
    """Get detailed information about a specific stream."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        stream = streaming_manager.get_stream(stream_id)

        if not stream:
            raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

        # Get stream stats and history info
        stats = await stream.get_stats()
        history = await stream.get_history(limit=5)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                **stats,
                "history_preview": history,
                "config": {
                    "stream_id": stream.config.stream_id,
                    "data_types": stream.config.data_types,
                    "update_interval": stream.config.update_interval,
                    "max_subscribers": stream.config.max_subscribers,
                    "enable_history": stream.config.enable_history,
                    "history_duration": stream.config.history_duration,
                    "filters": stream.config.filters,
                }
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


@router.get("/streams/{stream_id}/history")
async def get_stream_history(
    stream_id: str,
    limit: int = 100,
    since: Optional[float] = None,
):
    """Get historical data from a stream."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()
        stream = streaming_manager.get_stream(stream_id)

        if not stream:
            raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

        # Parse since timestamp
        since_dt = None
        if since:
            since_dt = datetime.fromtimestamp(since)

        history = await stream.get_history(limit=limit, since=since_dt)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "stream_id": stream_id,
                "total_items": len(history),
            },
            "data": history
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get stream history: {str(exc)}"
        ) from exc


@router.post("/streams/{stream_id}/publish")
async def publish_to_stream(
    stream_id: str,
    data: dict,
):
    """Publish data to a stream (admin endpoint)."""
    start_time = time.perf_counter()

    try:
        streaming_manager = get_streaming_manager()

        # Check authentication/authorization here
        # For now, just publish the data
        success = await streaming_manager.publish_to_stream(stream_id, data)

        if not success:
            raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Data published to stream {stream_id}",
            "stream_id": stream_id,
            "data_size": len(str(data)),
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
            detail=f"Failed to publish to stream: {str(exc)}"
        ) from exc


@router.get("/streaming/health")
async def streaming_health():
    """Get overall streaming system health."""
    start_time = time.perf_counter()

    try:
        # Get both WebSocket and streaming manager stats
        websocket_manager = WebSocketManager(create_rate_limit_manager("memory"))
        streaming_manager = get_streaming_manager()

        ws_stats = await websocket_manager.get_stats()
        stream_stats = await streaming_manager.get_all_stats()

        # Determine overall health
        health_status = "healthy"
        issues = []

        if not ws_stats.get("running", False):
            health_status = "unhealthy"
            issues.append("WebSocket manager not running")

        if not stream_stats.get("running", False):
            health_status = "unhealthy"
            issues.append("Streaming manager not running")

        if ws_stats.get("total_connections", 0) > ws_stats.get("max_connections", 1000) * 0.9:
            health_status = "warning"
            issues.append("High connection count")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "status": health_status,
            "websocket_manager": ws_stats,
            "streaming_manager": stream_stats,
            "issues": issues,
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


# Import required modules at the end to avoid circular imports
import asyncio
import json
from datetime import datetime
