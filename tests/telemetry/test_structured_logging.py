"""Tests for structured logging and correlation ID functionality."""
from __future__ import annotations

import json
import logging
import time
from unittest.mock import patch, MagicMock
from contextvars import ContextVar

import pytest

from aurum.telemetry.context import (
    set_request_id,
    get_request_id,
    reset_request_id,
    set_correlation_id,
    get_correlation_id,
    reset_correlation_id,
    set_tenant_id,
    get_tenant_id,
    reset_tenant_id,
    set_user_id,
    get_user_id,
    reset_user_id,
    set_session_id,
    get_session_id,
    reset_session_id,
    get_context,
    correlation_context,
    log_structured,
    STRUCTURED_LOGGER,
)


class TestCorrelationContext:
    """Test correlation ID context management."""

    def test_request_id_context(self):
        """Test request ID context management."""
        token = set_request_id("test-request-123")
        assert get_request_id() == "test-request-123"

        reset_request_id(token)
        assert get_request_id() is None

    def test_correlation_id_context(self):
        """Test correlation ID context management."""
        token = set_correlation_id("test-correlation-123")
        assert get_correlation_id() == "test-correlation-123"

        reset_correlation_id(token)
        assert get_correlation_id() is None

    def test_tenant_id_context(self):
        """Test tenant ID context management."""
        token = set_tenant_id("test-tenant-123")
        assert get_tenant_id() == "test-tenant-123"

        reset_tenant_id(token)
        assert get_tenant_id() is None

    def test_user_id_context(self):
        """Test user ID context management."""
        token = set_user_id("test-user-123")
        assert get_user_id() == "test-user-123"

        reset_user_id(token)
        assert get_user_id() is None

    def test_session_id_context(self):
        """Test session ID context management."""
        token = set_session_id("test-session-123")
        assert get_session_id() == "test-session-123"

        reset_session_id(token)
        assert get_session_id() is None

    def test_get_context(self):
        """Test getting all context variables."""
        context = get_context()
        assert isinstance(context, dict)
        assert "request_id" in context
        assert "correlation_id" in context
        assert "tenant_id" in context
        assert "user_id" in context
        assert "session_id" in context

    def test_correlation_context_manager(self):
        """Test correlation context manager."""
        with correlation_context(
            correlation_id="test-corr-123",
            tenant_id="test-tenant-123",
            user_id="test-user-123",
            session_id="test-session-123"
        ) as context:
            assert context["correlation_id"] == "test-corr-123"
            assert context["tenant_id"] == "test-tenant-123"
            assert context["user_id"] == "test-user-123"
            assert context["session_id"] == "test-session-123"

        # Context should be reset after exiting
        assert get_correlation_id() is None
        assert get_tenant_id() is None
        assert get_user_id() is None
        assert get_session_id() is None

    def test_correlation_context_defaults(self):
        """Test correlation context manager with defaults."""
        import uuid
        with correlation_context() as context:
            assert context["correlation_id"] is not None
            assert context["tenant_id"] == "unknown"
            assert context["user_id"] == "unknown"
            assert context["session_id"] == context["correlation_id"]  # Should be same as correlation_id


class TestStructuredLogging:
    """Test structured logging functionality."""

    def test_log_structured_debug(self):
        """Test structured logging with debug level."""
        with patch.object(STRUCTURED_LOGGER, 'debug') as mock_debug:
            log_structured("debug", "test_event", key1="value1", key2="value2")

            mock_debug.assert_called_once()
            call_args = mock_debug.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["level"] == "debug"
            assert log_data["event"] == "test_event"
            assert log_data["key1"] == "value1"
            assert log_data["key2"] == "value2"
            assert "timestamp" in log_data
            assert "request_id" in log_data
            assert "correlation_id" in log_data

    def test_log_structured_info(self):
        """Test structured logging with info level."""
        with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
            log_structured("info", "test_event", key1="value1")

            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["level"] == "info"
            assert log_data["event"] == "test_event"
            assert log_data["key1"] == "value1"

    def test_log_structured_warning(self):
        """Test structured logging with warning level."""
        with patch.object(STRUCTURED_LOGGER, 'warning') as mock_warning:
            log_structured("warning", "test_event", key1="value1")

            mock_warning.assert_called_once()
            call_args = mock_warning.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["level"] == "warning"
            assert log_data["event"] == "test_event"

    def test_log_structured_error(self):
        """Test structured logging with error level."""
        with patch.object(STRUCTURED_LOGGER, 'error') as mock_error:
            log_structured("error", "test_event", key1="value1")

            mock_error.assert_called_once()
            call_args = mock_error.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["level"] == "error"
            assert log_data["event"] == "test_event"

    def test_log_structured_critical(self):
        """Test structured logging with critical level."""
        with patch.object(STRUCTURED_LOGGER, 'critical') as mock_critical:
            log_structured("critical", "test_event", key1="value1")

            mock_critical.assert_called_once()
            call_args = mock_critical.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["level"] == "critical"
            assert log_data["event"] == "test_event"

    def test_log_structured_invalid_level(self):
        """Test structured logging with invalid level."""
        with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
            log_structured("invalid", "test_event", key1="value1")

            mock_info.assert_called_once()  # Should default to info

    def test_log_structured_with_context(self):
        """Test structured logging with correlation context."""
        with correlation_context(
            correlation_id="test-corr-123",
            tenant_id="test-tenant-123",
            user_id="test-user-123"
        ):
            with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
                log_structured("info", "test_event", custom_field="custom_value")

                call_args = mock_info.call_args[0][0]
                log_data = json.loads(call_args)

                assert log_data["correlation_id"] == "test-corr-123"
                assert log_data["tenant_id"] == "test-tenant-123"
                assert log_data["user_id"] == "test-user-123"
                assert log_data["custom_field"] == "custom_value"

    def test_log_structured_filters_none_values(self):
        """Test that structured logging filters out None values."""
        with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
            log_structured("info", "test_event", key1="value1", key2=None, key3="value3")

            call_args = mock_info.call_args[0][0]
            log_data = json.loads(call_args)

            assert "key1" in log_data
            assert "key2" not in log_data  # Should be filtered out
            assert "key3" in log_data
            assert log_data["key1"] == "value1"
            assert log_data["key3"] == "value3"

    def test_log_structured_with_exception(self):
        """Test structured logging with exception information."""
        test_exception = ValueError("Test error message")

        with patch.object(STRUCTURED_LOGGER, 'error') as mock_error:
            try:
                raise test_exception
            except ValueError as exc:
                log_structured("error", "exception_occurred", error=str(exc))

            call_args = mock_error.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["event"] == "exception_occurred"
            assert log_data["error"] == "Test error message"

    def test_log_structured_with_nested_data(self):
        """Test structured logging with nested data structures."""
        nested_data = {
            "user": {"id": 123, "name": "John Doe"},
            "metadata": {"version": "1.0", "tags": ["tag1", "tag2"]}
        }

        with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
            log_structured("info", "complex_event", **nested_data)

            call_args = mock_info.call_args[0][0]
            log_data = json.loads(call_args)

            assert log_data["user"]["id"] == 123
            assert log_data["user"]["name"] == "John Doe"
            assert log_data["metadata"]["version"] == "1.0"
            assert log_data["metadata"]["tags"] == ["tag1", "tag2"]

    def test_log_structured_json_serialization_error(self):
        """Test structured logging handles JSON serialization errors."""
        # Create an object that can't be JSON serialized
        class NonSerializable:
            pass

        non_serializable = NonSerializable()

        with patch.object(STRUCTURED_LOGGER, 'info') as mock_info:
            # Should not raise exception even with non-serializable data
            log_structured("info", "test_event", data=non_serializable)

            mock_info.assert_called_once()

    def test_log_structured_performance(self):
        """Test structured logging performance characteristics."""
        import time

        # Test that logging is reasonably fast (< 1ms per log)
        start_time = time.perf_counter()

        for i in range(100):
            log_structured("info", f"performance_test_{i}", iteration=i)

        end_time = time.perf_counter()
        avg_time_per_log = (end_time - start_time) / 100 * 1000  # Convert to ms

        assert avg_time_per_log < 1.0  # Should be less than 1ms per log on average


class TestContextIsolation:
    """Test that context variables are properly isolated."""

    def test_context_isolation_between_threads(self):
        """Test that context variables are isolated between threads."""
        import threading

        results = []

        def thread_function(thread_id):
            set_request_id(f"request-{thread_id}")
            set_correlation_id(f"correlation-{thread_id}")
            results.append((thread_id, get_request_id(), get_correlation_id()))

        threads = []
        for i in range(5):
            thread = threading.Thread(target=thread_function, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Each thread should have its own context values
        assert len(results) == 5
        for thread_id, request_id, correlation_id in results:
            assert request_id == f"request-{thread_id}"
            assert correlation_id == f"correlation-{thread_id}"

    def test_context_isolation_with_nested_contexts(self):
        """Test context isolation with nested correlation contexts."""
        with correlation_context(correlation_id="outer", tenant_id="outer-tenant") as outer_context:
            assert outer_context["correlation_id"] == "outer"
            assert outer_context["tenant_id"] == "outer-tenant"

            with correlation_context(correlation_id="inner", tenant_id="inner-tenant") as inner_context:
                assert inner_context["correlation_id"] == "inner"
                assert inner_context["tenant_id"] == "inner-tenant"

                # Inner context should override outer
                assert get_correlation_id() == "inner"
                assert get_tenant_id() == "inner-tenant"

            # After inner context exits, should revert to outer
            assert get_correlation_id() == "outer"
            assert get_tenant_id() == "outer-tenant"

        # After all contexts exit, should be None
        assert get_correlation_id() is None
        assert get_tenant_id() is None

    def test_context_reset_with_exceptions(self):
        """Test that context is properly reset even when exceptions occur."""
        try:
            with correlation_context(correlation_id="test", tenant_id="test-tenant"):
                assert get_correlation_id() == "test"
                assert get_tenant_id() == "test-tenant"
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Context should be reset even after exception
        assert get_correlation_id() is None
        assert get_tenant_id() is None


class TestLoggingIntegration:
    """Test integration with actual logging system."""

    def test_structured_logger_configuration(self):
        """Test that structured logger is properly configured."""
        assert STRUCTURED_LOGGER is not None
        assert isinstance(STRUCTURED_LOGGER, logging.Logger)
        assert STRUCTURED_LOGGER.name == "aurum.structured"

    def test_structured_logging_with_real_logger(self, caplog):
        """Test structured logging with real logger that captures output."""
        with caplog.at_level(logging.INFO, logger="aurum.structured"):
            log_structured("info", "integration_test", test_field="test_value")

        # Check that the log was captured
        assert len(caplog.records) == 1
        log_record = caplog.records[0]

        # Parse the JSON log message
        log_data = json.loads(log_record.message)

        assert log_data["level"] == "info"
        assert log_data["event"] == "integration_test"
        assert log_data["test_field"] == "test_value"
        assert "timestamp" in log_data
        assert "request_id" in log_data

    def test_structured_logging_levels(self, caplog):
        """Test that different logging levels work correctly."""
        levels_to_test = ["debug", "info", "warning", "error", "critical"]

        for level in levels_to_test:
            with caplog.at_level(logging.DEBUG, logger="aurum.structured"):
                log_structured(level, f"test_{level}_event")

            # Check that the correct level was logged
            assert len(caplog.records) == 1
            log_data = json.loads(caplog.records[0].message)

            assert log_data["level"] == level
            assert log_data["event"] == f"test_{level}_event"

            caplog.clear()

    def test_structured_logging_context_preservation(self, caplog):
        """Test that context is preserved across log calls."""
        with correlation_context(correlation_id="preservation-test", tenant_id="test-tenant"):
            with caplog.at_level(logging.INFO, logger="aurum.structured"):
                log_structured("info", "context_test_1")
                log_structured("info", "context_test_2")

            # Both log calls should have the same context
            for record in caplog.records:
                log_data = json.loads(record.message)
                assert log_data["correlation_id"] == "preservation-test"
                assert log_data["tenant_id"] == "test-tenant"


class TestLoggingUtilities:
    """Test logging utility functions."""

    def test_context_variables_thread_safety(self):
        """Test that context variables are thread-safe."""
        import threading

        results = []

        def worker(thread_id):
            # Each thread should have independent context
            set_request_id(f"thread-{thread_id}")
            set_correlation_id(f"corr-{thread_id}")

            # Simulate some work
            time.sleep(0.01)

            results.append((
                thread_id,
                get_request_id(),
                get_correlation_id()
            ))

        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Each thread should have its own context values
        assert len(results) == 10
        for thread_id, request_id, correlation_id in results:
            assert request_id == f"thread-{thread_id}"
            assert correlation_id == f"corr-{thread_id}"

    def test_context_cleanup_on_error(self):
        """Test that context is cleaned up even when errors occur."""
        def failing_function():
            set_request_id("test-request")
            set_correlation_id("test-correlation")
            raise ValueError("Test error")

        try:
            failing_function()
        except ValueError:
            pass

        # Context should be cleaned up after the error
        assert get_request_id() is None
        assert get_correlation_id() is None

    def test_multiple_context_variables(self):
        """Test managing multiple context variables simultaneously."""
        # Set multiple context variables
        request_token = set_request_id("req-123")
        correlation_token = set_correlation_id("corr-123")
        tenant_token = set_tenant_id("tenant-123")
        user_token = set_user_id("user-123")
        session_token = set_session_id("session-123")

        # Verify all are set
        assert get_request_id() == "req-123"
        assert get_correlation_id() == "corr-123"
        assert get_tenant_id() == "tenant-123"
        assert get_user_id() == "user-123"
        assert get_session_id() == "session-123"

        # Reset in reverse order
        reset_session_id(session_token)
        reset_user_id(user_token)
        reset_tenant_id(tenant_token)
        reset_correlation_id(correlation_token)
        reset_request_id(request_token)

        # All should be None
        assert get_request_id() is None
        assert get_correlation_id() is None
        assert get_tenant_id() is None
        assert get_user_id() is None
        assert get_session_id() is None
