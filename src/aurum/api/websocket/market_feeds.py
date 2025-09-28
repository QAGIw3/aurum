"""WebSocket feeds for real-time market data streaming."""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, Dict, Mapping

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..rate_limiting import create_rate_limit_manager
from ..websocket_manager import MessageType, WebSocketConnection, WebSocketManager
from ...streaming import (
    MarketDataEvent,
    MarketDataStreamingConfig,
    MarketDataStreamingService,
)
from ...streaming.service import load_streaming_configs_from_env

LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/ws/market", tags=["Market Data"], include_in_schema=False)


class MarketDataWebSocketCoordinator:
    """Coordinates WebSocket subscriptions with the streaming service."""

    def __init__(
        self,
        service: MarketDataStreamingService,
        manager: WebSocketManager,
    ) -> None:
        self.service = service
        self.websocket_manager = manager
        self._started = False
        self._listener_token: str | None = None
        self._curve_streams: dict[str, set[str]] = defaultdict(set)
        self._connection_streams: dict[str, dict[str, str]] = defaultdict(dict)
        self._start_lock = asyncio.Lock()

    async def ensure_started(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            await self.service.start()
            if not self.websocket_manager.running:
                await self.websocket_manager.start()
            if self._listener_token is None:
                self._listener_token = self.service.engine.register_listener(self._on_report)
            self._started = True

    async def handle_connection(self, websocket: WebSocket) -> None:
        await self.ensure_started()
        connection = await self.websocket_manager.accept_connection(websocket)
        if not connection:
            await websocket.close(code=1013, reason="Too many connections")
            return

        await websocket.accept()

        try:
            while True:
                message_text = await websocket.receive_text()
                await self._handle_message(connection, message_text)
        except WebSocketDisconnect:
            await self.handle_disconnect(connection)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("WebSocket error: %s", exc)
            await self.handle_disconnect(connection)

    async def handle_disconnect(self, connection: WebSocketConnection) -> None:
        for stream_id in list(self._connection_streams.get(connection.connection_id, {})):
            await self._unsubscribe(connection, {"stream_id": stream_id})
        await self.websocket_manager.remove_connection(connection.connection_id)

    async def _handle_message(self, connection: WebSocketConnection, message_text: str) -> None:
        try:
            data = json.loads(message_text)
        except json.JSONDecodeError:
            await connection.send_error("Invalid JSON message")
            return

        message_type = data.get("type")
        payload = data.get("payload", {})

        if message_type == MessageType.AUTH.value:
            await self._handle_auth(connection, payload)
        elif message_type == MessageType.SUBSCRIBE.value:
            await self._handle_subscribe(connection, payload)
        elif message_type == MessageType.UNSUBSCRIBE.value:
            await self._handle_unsubscribe(connection, payload)
        elif message_type == MessageType.PING.value:
            await connection.handle_ping()
        elif message_type == MessageType.PONG.value:
            await connection.handle_pong()
        else:
            await connection.send_error("Unknown message type", str(message_type))

    async def _handle_auth(self, connection: WebSocketConnection, payload: Mapping[str, Any]) -> None:
        success = await connection.authenticate(dict(payload))
        if success:
            await connection.send_message(
                MessageType.AUTH,
                {
                    "status": "authenticated",
                    "connection_id": connection.connection_id,
                    "user_id": connection.user_id,
                    "tenant_id": connection.tenant_id,
                },
            )
        else:
            await connection.send_error("Authentication failed", "Invalid credentials")

    async def _handle_subscribe(self, connection: WebSocketConnection, payload: Mapping[str, Any]) -> None:
        if not connection.authenticated:
            await connection.send_error("Not authenticated")
            return

        success = await self._subscribe(connection, payload)
        if success:
            await connection.send_message(
                MessageType.SUBSCRIBE,
                {
                    "stream_id": payload.get("stream_id"),
                    "status": "subscribed",
                },
            )

    async def _handle_unsubscribe(self, connection: WebSocketConnection, payload: Mapping[str, Any]) -> None:
        success = await self._unsubscribe(connection, payload)
        if success:
            await connection.send_message(
                MessageType.UNSUBSCRIBE,
                {
                    "stream_id": payload.get("stream_id"),
                    "status": "unsubscribed",
                },
            )

    async def _subscribe(self, connection: WebSocketConnection, payload: Mapping[str, Any]) -> bool:
        stream_id = str(payload.get("stream_id", ""))
        filters = dict(payload.get("filters", {}))
        curve_id = filters.get("curve_id")
        if not curve_id and stream_id.startswith("curve::"):
            curve_id = stream_id.split("::", 1)[-1]

        if not curve_id:
            await connection.send_error("Subscription failed", "Missing curve_id filter")
            return False

        subscription_payload = {
            "stream_id": stream_id,
            "filters": filters,
            "data_types": payload.get("data_types", ["curve_data"]),
        }

        success = await connection.subscribe(subscription_payload)
        if not success:
            return False

        self._curve_streams[curve_id].add(stream_id)
        self._connection_streams[connection.connection_id][stream_id] = curve_id
        return True

    async def _unsubscribe(self, connection: WebSocketConnection, payload: Mapping[str, Any]) -> bool:
        stream_id = str(payload.get("stream_id", ""))
        curve_map = self._connection_streams.get(connection.connection_id)
        if not stream_id or not curve_map or stream_id not in curve_map:
            return False

        curve_id = curve_map.pop(stream_id)
        streams = self._curve_streams.get(curve_id)
        if streams:
            streams.discard(stream_id)
            if not streams:
                self._curve_streams.pop(curve_id, None)

        await connection.unsubscribe(stream_id)
        return True

    async def _on_report(self, report) -> None:
        stream_ids = list(self._curve_streams.get(report.event.curve_id, ()))
        if not stream_ids:
            return
        payload_template = {
            "curve_id": report.event.curve_id,
            "payload": report.to_payload(),
        }
        for stream_id in stream_ids:
            payload = dict(payload_template)
            payload["stream_id"] = stream_id
            await self.websocket_manager.broadcast_to_stream(stream_id, payload)

    async def publish_manual_event(self, event: MarketDataEvent) -> None:
        """Utility used in tests to push an event through the pipeline."""
        report = await self.service.ingest_event(event)
        # Best-effort immediate broadcast to any active subscribers for this curve.
        # Subscription messages may still be in-flight, so we probe for a short
        # window and broadcast as soon as a matching subscription is observed.
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 0.5
        default_stream_id = f"curve::{event.curve_id}"

        payload_template = {
            "curve_id": report.event.curve_id,
            "payload": report.to_payload(),
        }

        sent = False
        while loop.time() < deadline and not sent:
            # Prefer the explicitly tracked curve->stream mapping when available
            stream_ids = list(self._curve_streams.get(event.curve_id, ()))

            # If the mapping isn't populated yet, fall back to checking active
            # connections for a subscription matching the conventional stream id.
            if not stream_ids:
                try:
                    # Accessing manager state is safe under best-effort semantics
                    for conn in self.websocket_manager.active_connections.values():
                        if default_stream_id in conn.subscriptions:
                            stream_ids = [default_stream_id]
                            break
                except Exception:
                    stream_ids = []

            # Broadcast once to avoid duplicating data if multiple probes occur
            if stream_ids:
                for stream_id in set(stream_ids):
                    payload = dict(payload_template)
                    payload["stream_id"] = stream_id
                    await self.websocket_manager.broadcast_to_stream(stream_id, payload)
                    sent = True
                    break

            if not sent:
                await asyncio.sleep(0.01)


# Lazily constructed singletons -------------------------------------------------

_rate_limiter = create_rate_limit_manager("memory")
_stream_cfg, _kafka_cfg = load_streaming_configs_from_env()
_service = MarketDataStreamingService(_stream_cfg, kafka_config=_kafka_cfg)
_manager = WebSocketManager(_rate_limiter)
_coordinator = MarketDataWebSocketCoordinator(_service, _manager)


def get_market_data_service() -> MarketDataStreamingService:
    return _service


def get_market_data_coordinator() -> MarketDataWebSocketCoordinator:
    return _coordinator


@router.websocket("/live")
async def live_market_data(websocket: WebSocket) -> None:
    """Main WebSocket endpoint for live market curves."""
    coordinator = get_market_data_coordinator()
    await coordinator.handle_connection(websocket)
