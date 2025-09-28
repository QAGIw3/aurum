"""Async testing utilities for the modernized Aurum architecture.

This module provides utilities for testing async services, database operations,
and other async components in the Aurum energy trading platform.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Callable, TypeVar
from unittest.mock import AsyncMock, MagicMock

import pytest


T = TypeVar('T')


def create_async_mock(**kwargs) -> AsyncMock:
    """Create an async mock with sensible defaults for testing."""
    mock = AsyncMock(**kwargs)
    return mock


def create_service_mock(service_class: type) -> MagicMock:
    """Create a mock service instance for testing."""
    mock = MagicMock(spec=service_class)
    # Make all async methods return coroutines that return None
    for attr_name in dir(service_class):
        attr = getattr(service_class, attr_name)
        if callable(attr) and attr_name.startswith(('get_', 'query_', 'create_', 'update_', 'delete_')):
            setattr(mock, attr_name, AsyncMock(return_value=None))
    return mock


@pytest.fixture
async def async_test_context():
    """Provide async test context with timing utilities."""
    start_time = time.time()

    class AsyncTestContext:
        def __init__(self):
            self.start_time = start_time
            self.operations: List[Dict[str, Any]] = []

        async def record_operation(self, operation: str, **context: Any) -> None:
            """Record an async operation for testing."""
            duration = time.time() - self.start_time
            self.operations.append({
                "operation": operation,
                "duration": duration,
                **context
            })

        async def wait_for_condition(
            self,
            condition: Callable[[], bool],
            timeout: float = 5.0,
            interval: float = 0.1
        ) -> bool:
            """Wait for a condition to be true with timeout."""
            start = time.time()
            while time.time() - start < timeout:
                if condition():
                    return True
                await asyncio.sleep(interval)
            return False

    yield AsyncTestContext()


@pytest.fixture
def mock_database_connection():
    """Provide a mock database connection for testing."""
    connection = MagicMock()

    # Configure async behavior
    connection.execute = AsyncMock(return_value=[])
    connection.fetchone = AsyncMock(return_value=None)
    connection.fetchall = AsyncMock(return_value=[])
    connection.close = AsyncMock()

    # Context manager support
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock(return_value=None)

    return connection


@pytest.fixture
def mock_cache_manager():
    """Provide a mock cache manager for testing."""
    cache_manager = AsyncMock()

    cache_manager.get = AsyncMock(return_value=None)
    cache_manager.set = AsyncMock(return_value=True)
    cache_manager.delete = AsyncMock(return_value=True)
    cache_manager.invalidate_pattern = AsyncMock(return_value=0)
    cache_manager.get_stats = AsyncMock(return_value={})

    return cache_manager


@pytest.fixture
def mock_settings():
    """Provide mock settings for testing."""
    from aurum.core import AurumSettings

    settings = MagicMock(spec=AurumSettings)

    # Configure nested attributes
    settings.api = MagicMock()
    settings.api.api_title = "Test API"
    settings.api.version = "1.0.0"
    settings.api.request_timeout_seconds = 30
    settings.api.cors_origins = ["*"]
    settings.api.gzip_min_bytes = 500

    settings.database = MagicMock()
    settings.database.trino_host = "localhost"
    settings.database.trino_port = 8080
    settings.database.trino_catalog = "test"
    settings.database.trino_database_schema = "test"

    settings.cache = MagicMock()
    settings.cache.redis_url = "redis://localhost:6379"
    settings.cache.ttl_seconds = 300

    return settings


@pytest.fixture
async def mock_async_service():
    """Provide a mock async service for testing service interactions."""

    class MockAsyncService:
        def __init__(self):
            self.call_count = 0
            self.call_history: List[Dict[str, Any]] = []

        async def async_operation(self, *args, **kwargs) -> Dict[str, Any]:
            self.call_count += 1
            call_info = {
                "call_number": self.call_count,
                "args": args,
                "kwargs": kwargs,
                "timestamp": time.time()
            }
            self.call_history.append(call_info)

            return {
                "result": "mock_result",
                "call_count": self.call_count,
                **kwargs
            }

        async def failing_operation(self, *args, **kwargs) -> None:
            raise ValueError("Mock error for testing")

    return MockAsyncService()


def assert_async_called(mock: AsyncMock, expected_calls: int = 1) -> None:
    """Assert that an async mock was called the expected number of times."""
    assert mock.call_count == expected_calls, f"Expected {expected_calls} calls, got {mock.call_count}"


def assert_async_called_with(mock: AsyncMock, *args, **kwargs) -> None:
    """Assert that an async mock was called with specific arguments."""
    mock.assert_called_with(*args, **kwargs)


def assert_async_not_called(mock: AsyncMock) -> None:
    """Assert that an async mock was not called."""
    assert mock.call_count == 0, f"Expected no calls, got {mock.call_count}"


@pytest.fixture
async def event_loop():
    """Provide a fresh event loop for async testing."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_tracer():
    """Provide a mock tracer for testing telemetry."""
    tracer = MagicMock()

    # Mock span context manager
    mock_span = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=None)
    mock_span.set_attribute = MagicMock()

    tracer.start_as_current_span = MagicMock(return_value=mock_span)

    return tracer


@pytest.fixture
def mock_logger():
    """Provide a mock logger for testing."""
    logger = MagicMock()

    logger.debug = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.exception = MagicMock()

    return logger


__all__ = [
    "create_async_mock",
    "create_service_mock",
    "assert_async_called",
    "assert_async_called_with",
    "assert_async_not_called",
]
