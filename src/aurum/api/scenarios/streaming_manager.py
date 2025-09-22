"""Data streaming manager for real-time data distribution."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass

from ..telemetry.context import get_request_id
from .websocket_manager import WebSocketManager
from ..cache.cache import AsyncCache, CacheManager


@dataclass
class StreamConfig:
    """Configuration for a data stream."""
    stream_id: str
    data_types: List[str]
    update_interval: float = 1.0  # seconds
    max_subscribers: int = 1000
    enable_history: bool = True
    history_duration: int = 3600  # seconds
    filters: Dict[str, Any] = None

    def __post_init__(self):
        if self.filters is None:
            self.filters = {}


class DataStream:
    """Manages a single data stream."""

    def __init__(self, config: StreamConfig, websocket_manager: WebSocketManager):
        self.config = config
        self.websocket_manager = websocket_manager
        self.subscribers: Dict[str, datetime] = {}
        self.history: List[Dict[str, Any]] = []
        self.last_update = datetime.utcnow()
        self.running = False
        self.update_task: Optional[asyncio.Task] = None

        # Stream statistics
        self.message_count = 0
        self.subscriber_count = 0
        self.error_count = 0

    async def start(self) -> None:
        """Start the data stream."""
        if self.running:
            return

        self.running = True
        self.update_task = asyncio.create_task(self._update_loop())

    async def stop(self) -> None:
        """Stop the data stream."""
        if not self.running:
            return

        self.running = False
        if self.update_task:
            self.update_task.cancel()

        # Clear subscribers
        self.subscribers.clear()

    async def subscribe(self, connection_id: str) -> bool:
        """Subscribe a connection to this stream."""
        if len(self.subscribers) >= self.config.max_subscribers:
            return False

        self.subscribers[connection_id] = datetime.utcnow()
        self.subscriber_count = len(self.subscribers)
        return True

    async def unsubscribe(self, connection_id: str) -> None:
        """Unsubscribe a connection from this stream."""
        if connection_id in self.subscribers:
            del self.subscribers[connection_id]
            self.subscriber_count = len(self.subscribers)

    async def publish_data(self, data: Any) -> None:
        """Publish data to all subscribers."""
        try:
            # Add to history if enabled
            if self.config.enable_history:
                await self._add_to_history(data)

            # Broadcast to subscribers
            await self.websocket_manager.broadcast_to_stream(self.config.stream_id, data)

            self.message_count += 1
            self.last_update = datetime.utcnow()

        except Exception as exc:
            self.error_count += 1
            # Log error but don't propagate
            print(f"Stream {self.config.stream_id} publish error: {exc}")

    async def get_history(
        self,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get historical data from this stream."""
        if not self.config.enable_history:
            return []

        # Filter by time if specified
        history = self.history
        if since:
            history = [item for item in history if item.get("timestamp") and item["timestamp"] >= since]

        # Return most recent items
        return history[-limit:] if limit > 0 else history

    async def _update_loop(self) -> None:
        """Main update loop for the stream."""
        while self.running:
            try:
                # Generate or fetch data
                data = await self._generate_data()

                if data:
                    await self.publish_data(data)

                # Wait for next update
                await asyncio.sleep(self.config.update_interval)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.error_count += 1
                await asyncio.sleep(1)  # Brief pause on error

    async def _generate_data(self) -> Optional[Dict[str, Any]]:
        """Generate or fetch data for this stream."""
        # This would be implemented based on the specific data type
        # For now, return a placeholder
        return {
            "stream_id": self.config.stream_id,
            "timestamp": datetime.utcnow(),
            "data_type": self.config.data_types[0] if self.config.data_types else "unknown",
            "payload": {"message": f"Data from {self.config.stream_id}"},
            "subscriber_count": self.subscriber_count,
            "message_count": self.message_count
        }

    async def _add_to_history(self, data: Any) -> None:
        """Add data to history."""
        history_item = {
            "timestamp": datetime.utcnow(),
            "data": data,
            "subscriber_count": self.subscriber_count
        }

        self.history.append(history_item)

        # Clean up old history
        cutoff_time = datetime.utcnow() - timedelta(seconds=self.config.history_duration)
        self.history = [
            item for item in self.history
            if item["timestamp"] > cutoff_time
        ]

    async def get_stats(self) -> Dict[str, Any]:
        """Get stream statistics."""
        return {
            "stream_id": self.config.stream_id,
            "running": self.running,
            "subscriber_count": self.subscriber_count,
            "message_count": self.message_count,
            "error_count": self.error_count,
            "last_update": self.last_update.isoformat(),
            "update_interval": self.config.update_interval,
            "max_subscribers": self.config.max_subscribers,
            "enable_history": self.config.enable_history,
            "history_size": len(self.history),
            "data_types": self.config.data_types,
            "request_id": get_request_id(),
        }


class StreamingManager:
    """Manages multiple data streams."""

    def __init__(self, websocket_manager: WebSocketManager):
        self.websocket_manager = websocket_manager
        self.streams: Dict[str, DataStream] = {}
        self.stream_configs: Dict[str, StreamConfig] = {}
        self.running = False
        self.startup_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the streaming manager."""
        if self.running:
            return

        self.running = True

        # Start all configured streams
        for stream_id, config in self.stream_configs.items():
            await self.create_stream(config)

    async def stop(self) -> None:
        """Stop the streaming manager."""
        if not self.running:
            return

        self.running = False

        # Stop all streams
        for stream in self.streams.values():
            await stream.stop()

        self.streams.clear()

    async def create_stream(self, config: StreamConfig) -> Optional[DataStream]:
        """Create a new data stream."""
        if config.stream_id in self.streams:
            return self.streams[config.stream_id]

        stream = DataStream(config, self.websocket_manager)
        self.streams[config.stream_id] = stream

        # Register with WebSocket manager
        self.websocket_manager.register_stream_callback(
            config.stream_id,
            self._handle_stream_data
        )

        # Start the stream
        await stream.start()

        return stream

    async def delete_stream(self, stream_id: str) -> bool:
        """Delete a data stream."""
        if stream_id not in self.streams:
            return False

        stream = self.streams[stream_id]
        await stream.stop()

        del self.streams[stream_id]
        if stream_id in self.stream_configs:
            del self.stream_configs[stream_id]

        return True

    async def register_stream_config(self, config: StreamConfig) -> None:
        """Register a stream configuration."""
        self.stream_configs[config.stream_id] = config

        # Create stream if manager is running
        if self.running:
            await self.create_stream(config)

    def get_stream(self, stream_id: str) -> Optional[DataStream]:
        """Get a data stream by ID."""
        return self.streams.get(stream_id)

    def list_streams(self) -> List[str]:
        """List all active stream IDs."""
        return list(self.streams.keys())

    async def publish_to_stream(self, stream_id: str, data: Any) -> bool:
        """Publish data to a specific stream."""
        stream = self.get_stream(stream_id)
        if not stream:
            return False

        await stream.publish_data(data)
        return True

    async def broadcast_data(self, data: Any, stream_type: str = "broadcast") -> None:
        """Broadcast data to all streams of a type."""
        for stream in self.streams.values():
            if stream_type in stream.config.data_types:
                await stream.publish_data(data)

    async def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all streams."""
        total_subscribers = 0
        total_messages = 0
        total_errors = 0
        stream_stats = {}

        for stream_id, stream in self.streams.items():
            stats = await stream.get_stats()
            stream_stats[stream_id] = stats
            total_subscribers += stats["subscriber_count"]
            total_messages += stats["message_count"]
            total_errors += stats["error_count"]

        return {
            "total_streams": len(self.streams),
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "total_errors": total_errors,
            "running": self.running,
            "streams": stream_stats,
            "request_id": get_request_id(),
        }

    async def _handle_stream_data(self, stream_id: str, data: Dict[str, Any]) -> None:
        """Handle data for a stream."""
        # This is called by the WebSocket manager when data is broadcast
        # Additional processing can be done here if needed
        pass

    def create_default_streams(self) -> None:
        """Create default data streams."""
        default_configs = [
            StreamConfig(
                stream_id="curve_updates",
                data_types=["curve_data", "market_data"],
                update_interval=5.0,
                max_subscribers=500,
                enable_history=True,
                history_duration=1800,  # 30 minutes
            ),
            StreamConfig(
                stream_id="system_status",
                data_types=["status", "health"],
                update_interval=30.0,
                max_subscribers=1000,
                enable_history=False,
            ),
            StreamConfig(
                stream_id="user_notifications",
                data_types=["notification", "alert"],
                update_interval=1.0,
                max_subscribers=1000,
                enable_history=True,
                history_duration=86400,  # 24 hours
            ),
        ]

        for config in default_configs:
            self.stream_configs[config.stream_id] = config


# Global streaming manager instance
_streaming_manager: Optional[StreamingManager] = None


def get_streaming_manager() -> StreamingManager:
    """Get the global streaming manager."""
    global _streaming_manager
    if _streaming_manager is None:
        _streaming_manager = StreamingManager(None)
    return _streaming_manager


async def initialize_streaming(
    websocket_manager: WebSocketManager,
    create_defaults: bool = True
) -> StreamingManager:
    """Initialize the streaming system."""
    global _streaming_manager

    if _streaming_manager is None:
        _streaming_manager = StreamingManager(websocket_manager)

        if create_defaults:
            _streaming_manager.create_default_streams()

        await _streaming_manager.start()

    return _streaming_manager
