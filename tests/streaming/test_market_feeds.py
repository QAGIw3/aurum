import asyncio
import json
from collections import deque
from datetime import datetime, timezone

import pytest
from fastapi import WebSocketDisconnect

from aurum.api.rate_limiting import create_rate_limit_manager
from aurum.api.websocket.market_feeds import MarketDataWebSocketCoordinator
from aurum.api.websocket_manager import MessageType, WebSocketManager
from aurum.streaming import MarketDataEvent, MarketDataStreamingConfig, MarketDataStreamingService


class DummyWebSocket:
    def __init__(self, messages):
        self._messages = deque(messages)
        self._disconnect_event = asyncio.Event()
        self.sent: list[str] = []
        self.accepted = False
        self.closed = None

    async def accept(self):
        self.accepted = True

    async def close(self, code: int = 1000, reason: str = ""):
        self.closed = (code, reason)

    async def receive_text(self) -> str:
        if self._messages:
            return self._messages.popleft()
        await self._disconnect_event.wait()
        raise WebSocketDisconnect(code=1000)

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    def trigger_disconnect(self) -> None:
        self._disconnect_event.set()


@pytest.mark.asyncio
async def test_market_feed_coordinator_broadcasts_curve_updates():
    service = MarketDataStreamingService(MarketDataStreamingConfig())
    manager = WebSocketManager(create_rate_limit_manager("memory"))
    coordinator = MarketDataWebSocketCoordinator(service, manager)

    auth_message = json.dumps({
        "type": MessageType.AUTH.value,
        "payload": {"user_id": "user", "tenant_id": "tenant"},
    })
    subscribe_message = json.dumps({
        "type": MessageType.SUBSCRIBE.value,
        "payload": {
            "stream_id": "curve::TEST",
            "filters": {"curve_id": "TEST"},
        },
    })
    ping_message = json.dumps({"type": MessageType.PING.value, "payload": {}})

    websocket = DummyWebSocket([auth_message, subscribe_message, ping_message])

    task = asyncio.create_task(coordinator.handle_connection(websocket))

    # Allow handshake and subscription to complete
    await asyncio.sleep(0.1)

    event = MarketDataEvent(
        curve_id="TEST",
        tenor="2024-01",
        price=55.0,
        timestamp=datetime.now(timezone.utc),
        vendor="unit-test",
    )

    await coordinator.publish_manual_event(event)

    await asyncio.sleep(0.1)
    websocket.trigger_disconnect()
    await task

    data_messages = [json.loads(item) for item in websocket.sent if json.loads(item)["type"] == MessageType.DATA.value]
    assert data_messages, "Expected at least one DATA message"
    payload = data_messages[-1]["payload"]
    assert payload["curve_id"] == "TEST"
    assert payload["payload"]["event"]["price"] == pytest.approx(55.0)

    await service.stop()
    await manager.stop()
