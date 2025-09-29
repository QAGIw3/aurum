"""Common test fixtures for state management and settings."""

import os
import asyncio
import logging
from typing import Dict, Any, Generator
from unittest.mock import patch

import pytest


@pytest.fixture(scope="function")
def reset_state():
    """Reset global state before each test.

    This fixture clears:
    - LRU caches and global registries
    - Dependency overrides
    - In-memory brokers/queues
    - Logging context
    - Any other global state that could leak between tests
    """
    # Clear any global caches or registries
    # This is a placeholder - specific implementations will be added as needed

    # Clear asyncio task tracking
    current_task = asyncio.current_task()
    if current_task:
        current_task.cancel()

    # Clear dependency overrides (if using FastAPI dependency injection)
    # This is a placeholder - specific implementations will be added

    yield

    # Post-test cleanup
    # Reset any global state that might have been modified


@pytest.fixture(scope="function")
def settings_override(monkeypatch: pytest.MonkeyPatch) -> Generator[Dict[str, str], None, None]:
    """Fixture to override environment variables for a test.

    Usage:
        def test_something(settings_override):
            settings_override["AURUM_API_BACKEND"] = "clickhouse"
            # Test code here
    """
    original_env = {}

    def set_env(key: str, value: str) -> None:
        """Set an environment variable and track for cleanup."""
        if key not in original_env:
            original_env[key] = os.environ.get(key)
        os.environ[key] = value
        monkeypatch.setenv(key, value)

    def del_env(key: str) -> None:
        """Delete an environment variable and track for cleanup."""
        if key not in original_env:
            original_env[key] = os.environ.get(key)
        os.environ.pop(key, None)
        monkeypatch.delenv(key, raising=False)

    # Return the helper functions
    yield {"set": set_env, "del": del_env}

    # Cleanup - restore original environment
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture(scope="function")
def capture_logs(caplog: pytest.LogCaptureFixture) -> Generator[Dict[str, Any], None, None]:
    """Fixture to capture and filter logs during tests.

    Usage:
        def test_something(capture_logs):
            with capture_logs.at_level(logging.WARNING, logger="aurum.api"):
                # Test code that generates logs
                pass
            # Access captured logs
            assert len(capture_logs.records) > 0
    """
    class LogCaptureHelper:
        def __init__(self, caplog_fixture):
            self.caplog = caplog_fixture
            self.records = []

        def at_level(self, level: int, logger: str = ""):
            """Context manager to capture logs at a specific level."""
            return self.caplog.at_level(level, logger=logger)

        def clear(self):
            """Clear captured log records."""
            self.records.clear()

        @property
        def records(self):
            """Get filtered log records."""
            return [record for record in self.caplog.records if not self._should_filter(record)]

        def _should_filter(self, record):
            """Filter out noisy logs that aren't relevant for most tests."""
            # Filter out debug logs from dependencies
            noisy_loggers = [
                "uvicorn",
                "uvicorn.access",
                "uvicorn.error",
                "asyncio",
                "httpx",
            ]
            return any(logger in record.name for logger in noisy_loggers)

    helper = LogCaptureHelper(caplog)
    yield helper


@pytest.fixture(scope="function")
def mock_external_dependencies():
    """Mock external dependencies that tests shouldn't hit."""
    external_services = [
        "requests.Session",
        "httpx.AsyncClient",
        "aiokafka.AIOKafkaProducer",
        "confluent_kafka.Producer",
    ]

    mocks = {}
    for service in external_services:
        try:
            # Split module and class name
            if "." in service:
                module_name, class_name = service.rsplit(".", 1)
                module = __import__(module_name, fromlist=[class_name])
                cls = getattr(module, class_name)
                mock = patch(service)
                mocks[service] = mock.start()
        except (ImportError, AttributeError):
            # Service not available, skip
            continue

    yield mocks

    # Stop all mocks
    for mock in mocks.values():
        mock.stop()


@pytest.fixture(scope="function")
def event_loop():
    """Create a new event loop for each test function.

    This replaces the session-scoped loop and ensures proper isolation.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)
