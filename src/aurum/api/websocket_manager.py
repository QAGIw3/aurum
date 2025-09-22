"""WebSocket connection manager for real-time data streaming."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Awaitable

from fastapi import WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel

from ..telemetry.context import get_request_id
from .rate_limiting import RateLimitManager, QuotaTier


class ConnectionState(Enum):
    """WebSocket connection states."""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    SUBSCRIBED = "subscribed"
    DISCONNECTING = "disconnecting"
    DISCONNECTED = "disconnected"


class MessageType(Enum):
    """WebSocket message types."""
    AUTH = "auth"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    DATA = "data"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"
    STATUS = "status"


class StreamSubscription(BaseModel):
    """Subscription to a data stream."""
    stream_id: str
    filters: Dict[str, Any] = {}
    data_types: List[str] = []
    created_at: datetime = datetime.utcnow()
    last_activity: datetime = datetime.utcnow()


class WebSocketMessage(BaseModel):
    """WebSocket message structure."""
    type: MessageType
    payload: Dict[str, Any]
    timestamp: datetime = datetime.utcnow()
    request_id: str = ""

    def to_json(self) -> str:
        """Convert message to JSON string."""
        return json.dumps({
            "type": self.type.value,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "request_id": self.request_id
        })


class WebSocketConnection:
    """Manages a single WebSocket connection."""

    def __init__(
        self,
        websocket: WebSocket,
        connection_id: str,
        rate_limiter: Optional[RateLimitManager] = None
    ):
        self.websocket = websocket
        self.connection_id = connection_id
        self.state = ConnectionState.CONNECTING
        self.authenticated = False
        self.user_id: Optional[str] = None
        self.tenant_id: Optional[str] = None
        self.subscriptions: Dict[str, StreamSubscription] = {}
        self.created_at = datetime.utcnow()
        self.last_activity = datetime.utcnow()
        self.heartbeat_interval = 30  # seconds
        self.message_count = 0
        self.rate_limiter = rate_limiter

        # Connection metadata
        self.client_info = {
            "user_agent": "",
            "ip_address": "",
            "connection_type": "websocket"
        }

    async def authenticate(self, auth_data: Dict[str, Any]) -> bool:
        """Authenticate the WebSocket connection."""
        try:
            # Extract authentication information
            self.user_id = auth_data.get("user_id")
            self.tenant_id = auth_data.get("tenant_id")

            # In a real implementation, this would validate credentials
            # For now, accept any provided credentials
            if self.user_id and self.tenant_id:
                self.authenticated = True
                self.state = ConnectionState.AUTHENTICATED
                return True
            return False
        except Exception as exc:
            await self.send_error("Authentication failed", str(exc))
            return False

    async def subscribe(self, subscription_data: Dict[str, Any]) -> bool:
        """Subscribe to a data stream."""
        try:
            stream_id = subscription_data.get("stream_id")
            if not stream_id:
                await self.send_error("Subscription failed", "Missing stream_id")
                return False

            subscription = StreamSubscription(
                stream_id=stream_id,
                filters=subscription_data.get("filters", {}),
                data_types=subscription_data.get("data_types", ["curve_data"])
            )

            self.subscriptions[stream_id] = subscription
            self.state = ConnectionState.SUBSCRIBED

            # Send subscription confirmation
            await self.send_message(MessageType.SUBSCRIBE, {
                "stream_id": stream_id,
                "status": "subscribed",
                "subscription": subscription.model_dump()
            })

            return True
        except Exception as exc:
            await self.send_error("Subscription failed", str(exc))
            return False

    async def unsubscribe(self, stream_id: str) -> bool:
        """Unsubscribe from a data stream."""
        try:
            if stream_id in self.subscriptions:
                del self.subscriptions[stream_id]

                # Send unsubscription confirmation
                await self.send_message(MessageType.UNSUBSCRIBE, {
                    "stream_id": stream_id,
                    "status": "unsubscribed"
                })

                # If no subscriptions left, change state
                if not self.subscriptions:
                    self.state = ConnectionState.AUTHENTICATED

                return True
            return False
        except Exception as exc:
            await self.send_error("Unsubscription failed", str(exc))
            return False

    async def send_data(self, stream_id: str, data: Any) -> None:
        """Send data to the client."""
        try:
            await self.send_message(MessageType.DATA, {
                "stream_id": stream_id,
                "data": data
            })
            self.last_activity = datetime.utcnow()
        except Exception as exc:
            # Connection might be closed
            self.state = ConnectionState.DISCONNECTING

    async def send_heartbeat(self) -> None:
        """Send heartbeat to keep connection alive."""
        try:
            await self.send_message(MessageType.HEARTBEAT, {
                "timestamp": datetime.utcnow().isoformat(),
                "connection_id": self.connection_id
            })
        except Exception:
            self.state = ConnectionState.DISCONNECTING

    async def send_message(self, message_type: MessageType, payload: Dict[str, Any]) -> None:
        """Send a message to the client."""
        # Check rate limits if available
        if self.rate_limiter:
            identifier = f"ws:{self.connection_id}"
            result = await self.rate_limiter.check_rate_limit(
                identifier=identifier,
                tier=QuotaTier.FREE,  # WebSocket connections get their own tier
                endpoint="websocket_message"
            )

            if not result.allowed:
                await self.send_error("Rate limit exceeded", "Too many messages")
                return

        message = WebSocketMessage(
            type=message_type,
            payload=payload,
            request_id=get_request_id()
        )

        await self.websocket.send_text(message.to_json())
        self.message_count += 1

    async def send_error(self, error: str, details: str = "") -> None:
        """Send an error message to the client."""
        await self.send_message(MessageType.ERROR, {
            "error": error,
            "details": details
        })

    async def handle_ping(self) -> None:
        """Handle ping from client."""
        await self.send_message(MessageType.PONG, {
            "timestamp": datetime.utcnow().isoformat()
        })

    async def handle_pong(self) -> None:
        """Handle pong from client (update activity)."""
        self.last_activity = datetime.utcnow()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the WebSocket connection."""
        try:
            await self.websocket.close(code=code, reason=reason)
        except Exception:
            pass
        self.state = ConnectionState.DISCONNECTED


class WebSocketManager:
    """Manages multiple WebSocket connections."""

    def __init__(self, rate_limiter: Optional[RateLimitManager] = None):
        self.active_connections: Dict[str, WebSocketConnection] = {}
        self.connection_lock = asyncio.Lock()
        self.max_connections = 1000  # Configurable limit
        self.heartbeat_interval = 30  # seconds
        self.connection_timeout = 300  # seconds

        # Stream data callbacks
        self.stream_callbacks: Dict[str, Callable[[str, Dict[str, Any]], Awaitable[None]]] = {}

        # Background tasks
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        self.running = False

        self.rate_limiter = rate_limiter

    async def start(self) -> None:
        """Start the WebSocket manager."""
        self.running = True
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """Stop the WebSocket manager and close all connections."""
        self.running = False

        # Cancel background tasks
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()

        # Close all connections
        async with self.connection_lock:
            for connection in list(self.active_connections.values()):
                await connection.close(code=1012, reason="Server shutdown")

            self.active_connections.clear()

    async def accept_connection(self, websocket: WebSocket) -> Optional[WebSocketConnection]:
        """Accept a new WebSocket connection."""
        async with self.connection_lock:
            if len(self.active_connections) >= self.max_connections:
                await websocket.close(code=1013, reason="Too many connections")
                return None

            connection_id = str(uuid.uuid4())
            connection = WebSocketConnection(
                websocket=websocket,
                connection_id=connection_id,
                rate_limiter=self.rate_limiter
            )

            self.active_connections[connection_id] = connection
            return connection

    async def remove_connection(self, connection_id: str) -> None:
        """Remove a WebSocket connection."""
        async with self.connection_lock:
            if connection_id in self.active_connections:
                connection = self.active_connections[connection_id]
                await connection.close()
                del self.active_connections[connection_id]

    def get_connection(self, connection_id: str) -> Optional[WebSocketConnection]:
        """Get a WebSocket connection by ID."""
        return self.active_connections.get(connection_id)

    def get_connections_by_user(self, user_id: str) -> List[WebSocketConnection]:
        """Get all connections for a specific user."""
        return [
            conn for conn in self.active_connections.values()
            if conn.user_id == user_id
        ]

    def get_connections_by_tenant(self, tenant_id: str) -> List[WebSocketConnection]:
        """Get all connections for a specific tenant."""
        return [
            conn for conn in self.active_connections.values()
            if conn.tenant_id == tenant_id
        ]

    async def broadcast_to_stream(self, stream_id: str, data: Any) -> None:
        """Broadcast data to all subscribers of a stream."""
        # Find connections subscribed to this stream
        subscribers = []
        for connection in self.active_connections.values():
            if stream_id in connection.subscriptions:
                subscribers.append(connection)

        # Send data to all subscribers
        for connection in subscribers:
            await connection.send_data(stream_id, data)

    async def broadcast_to_user(self, user_id: str, data: Any) -> None:
        """Broadcast data to all connections of a specific user."""
        connections = self.get_connections_by_user(user_id)
        for connection in connections:
            await connection.send_data("user_broadcast", data)

    async def broadcast_to_tenant(self, tenant_id: str, data: Any) -> None:
        """Broadcast data to all connections of a specific tenant."""
        connections = self.get_connections_by_tenant(tenant_id)
        for connection in connections:
            await connection.send_data("tenant_broadcast", data)

    def register_stream_callback(
        self,
        stream_id: str,
        callback: Callable[[str, Dict[str, Any]], Awaitable[None]]
    ) -> None:
        """Register a callback for stream data."""
        self.stream_callbacks[stream_id] = callback

    async def _heartbeat_loop(self) -> None:
        """Send heartbeats to all connections."""
        while self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)

                async with self.connection_lock:
                    current_time = datetime.utcnow()
                    for connection in list(self.active_connections.values()):
                        # Check if connection is stale
                        time_since_activity = (current_time - connection.last_activity).total_seconds()
                        if time_since_activity > self.connection_timeout:
                            await self.remove_connection(connection.connection_id)
                        else:
                            await connection.send_heartbeat()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log error but continue
                print(f"Heartbeat error: {exc}")

    async def _cleanup_loop(self) -> None:
        """Clean up inactive connections."""
        while self.running:
            try:
                await asyncio.sleep(60)  # Cleanup every minute

                async with self.connection_lock:
                    current_time = datetime.utcnow()
                    to_remove = []

                    for connection_id, connection in self.active_connections.items():
                        # Remove connections inactive for too long
                        time_since_activity = (current_time - connection.last_activity).total_seconds()
                        if time_since_activity > self.connection_timeout:
                            to_remove.append(connection_id)

                    for connection_id in to_remove:
                        await self.remove_connection(connection_id)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log error but continue
                print(f"Cleanup error: {exc}")

    async def get_stats(self) -> Dict[str, Any]:
        """Get WebSocket manager statistics."""
        async with self.connection_lock:
            total_connections = len(self.active_connections)
            authenticated_connections = sum(1 for conn in self.active_connections.values() if conn.authenticated)
            subscribed_connections = sum(1 for conn in self.active_connections.values() if conn.subscriptions)

            # Get connection counts by state
            state_counts = {}
            for conn in self.active_connections.values():
                state = conn.state.value
                state_counts[state] = state_counts.get(state, 0) + 1

            # Get subscription counts by stream
            stream_counts = {}
            for conn in self.active_connections.values():
                for subscription in conn.subscriptions.values():
                    stream_id = subscription.stream_id
                    stream_counts[stream_id] = stream_counts.get(stream_id, 0) + 1

        return {
            "total_connections": total_connections,
            "authenticated_connections": authenticated_connections,
            "subscribed_connections": subscribed_connections,
            "connections_by_state": state_counts,
            "subscriptions_by_stream": stream_counts,
            "max_connections": self.max_connections,
            "heartbeat_interval": self.heartbeat_interval,
            "connection_timeout": self.connection_timeout,
            "running": self.running,
            "request_id": get_request_id(),
        }
