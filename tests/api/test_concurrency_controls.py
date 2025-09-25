"""Tests for API concurrency controls, timeouts, and Trino client resilience."""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from aurum.api.concurrency_middleware import (
    ConcurrencyController,
    ConcurrencySlot,
    RateLimiter,
    TimeoutController,
    TimeoutHandle,
    ConcurrencyMiddleware,
    RequestLimits,
    create_concurrency_controller,
    create_rate_limiter,
    create_timeout_controller,
    create_concurrency_middleware,
)
from aurum.api.exceptions import ServiceUnavailableException
from aurum.api.database.trino_client import (
    SimpleTrinoClient as TrinoClient,
    HybridTrinoClientManager as TrinoClientManager,
    get_trino_client,
    get_default_trino_client,
)
from aurum.common.circuit_breaker import CircuitBreaker
from aurum.staleness.auto_remediation import CircuitBreakerState
from aurum.performance.connection_pool import ConnectionPool
from aurum.external.collect.base import RetryConfig
from aurum.api.config import TrinoConfig
from aurum.api.database.config import TrinoConfig as DatabaseTrinoConfig


class TestRequestLimits:
    """Test RequestLimits configuration."""

    def test_request_limits_default_values(self):
        """Test default request limits."""
        limits = RequestLimits()
        assert limits.max_concurrent_requests == 100
        assert limits.max_requests_per_second == 10.0
        assert limits.max_request_duration_seconds == 30.0
        assert limits.max_requests_per_tenant == 20
        assert limits.tenant_burst_limit == 50

    def test_request_limits_custom_values(self):
        """Test custom request limits."""
        limits = RequestLimits(
            max_concurrent_requests=50,
            max_requests_per_second=5.0,
            max_request_duration_seconds=60.0,
            max_requests_per_tenant=10,
            tenant_burst_limit=25
        )
        assert limits.max_concurrent_requests == 50
        assert limits.max_requests_per_second == 5.0
        assert limits.max_request_duration_seconds == 60.0
        assert limits.max_requests_per_tenant == 10
        assert limits.tenant_burst_limit == 25


class TestConcurrencyController:
    """Test concurrency controller."""

    @pytest.fixture
    def limits(self):
        return RequestLimits(max_concurrent_requests=5, max_requests_per_tenant=2)

    @pytest.fixture
    def controller(self, limits):
        return ConcurrencyController(limits)

    async def test_acquire_slot_global_limit(self, controller):
        """Test acquiring slots within global limit."""
        slots = []
        for i in range(5):
            slot = await controller.acquire_slot(tenant_id="tenant1", request_id=f"req-{i}")
            slots.append(slot)

        # Should not be able to acquire more than max concurrent
        with pytest.raises(ServiceUnavailableException):
            await controller.acquire_slot(tenant_id="tenant2", request_id="req-6")

        # Clean up
        for slot in slots:
            await slot.release()

    async def test_acquire_slot_tenant_limit(self, controller):
        """Test acquiring slots within tenant limit."""
        # Acquire max per tenant
        slots = []
        for i in range(2):
            slot = await controller.acquire_slot(tenant_id="tenant1", request_id=f"req-{i}")
            slots.append(slot)

        # Should not be able to acquire more for same tenant
        with pytest.raises(ServiceUnavailableException):
            await controller.acquire_slot(tenant_id="tenant1", request_id="req-3")

        # Should be able to acquire for different tenant
        slot = await controller.acquire_slot(tenant_id="tenant2", request_id="req-4")
        slots.append(slot)

        # Clean up
        for slot in slots:
            await slot.release()

    async def test_slot_release(self, controller):
        """Test slot release functionality."""
        slot = await controller.acquire_slot(tenant_id="tenant1", request_id="req-1")
        assert controller._active_requests == 1
        assert controller.limits._tenant_requests["tenant1"] == 1

        await slot.release()
        assert controller._active_requests == 0
        assert controller.limits._tenant_requests["tenant1"] == 0

    async def test_slot_context_manager(self, controller):
        """Test slot as async context manager."""
        async with await controller.acquire_slot(tenant_id="tenant1", request_id="req-1") as slot:
            assert controller._active_requests == 1

        # Should be released automatically
        assert controller._active_requests == 0

    async def test_get_stats(self, controller):
        """Test getting concurrency statistics."""
        stats = await controller.get_stats()
        assert "active_requests" in stats
        assert "global_limit" in stats
        assert "tenants" in stats

    async def test_tenant_request_cleanup(self, controller):
        """Test cleanup of old tenant request times."""
        # Add old request times
        old_time = time.perf_counter() - 120  # 2 minutes ago
        controller.limits._tenant_request_times["tenant1"] = [old_time]

        slot = await controller.acquire_slot(tenant_id="tenant1", request_id="req-1")
        assert len(controller.limits._tenant_request_times["tenant1"]) == 1  # Old entry should be cleaned

        await slot.release()


class TestRateLimiter:
    """Test rate limiter functionality."""

    @pytest.fixture
    def rate_limiter(self):
        return RateLimiter()

    async def test_rate_limit_check(self, rate_limiter):
        """Test rate limit checking."""
        identifier = "test-client"

        # Should allow requests within limit
        for i in range(10):
            allowed = await rate_limiter.check_rate_limit(identifier, max_requests=10, window_seconds=1.0)
            assert allowed

        # Should reject requests over limit
        allowed = await rate_limiter.check_rate_limit(identifier, max_requests=10, window_seconds=1.0)
        assert not allowed

    async def test_rate_limit_cleanup(self, rate_limiter):
        """Test cleanup of old requests."""
        identifier = "test-client"

        # Add old request
        old_time = time.perf_counter() - 120  # 2 minutes ago
        async with rate_limiter._lock:
            rate_limiter._requests[identifier].append(old_time)

        # Check rate limit - should clean old requests
        allowed = await rate_limiter.check_rate_limit(identifier, max_requests=10, window_seconds=60.0)
        assert allowed

        # Should have only 1 request (the new one)
        async with rate_limiter._lock:
            assert len(rate_limiter._requests[identifier]) == 1

    async def test_remaining_requests(self, rate_limiter):
        """Test getting remaining requests."""
        identifier = "test-client"

        # Make some requests
        for i in range(3):
            await rate_limiter.check_rate_limit(identifier, max_requests=5, window_seconds=60.0)

        remaining = await rate_limiter.get_remaining_requests(identifier, max_requests=5, window_seconds=60.0)
        assert remaining == 2


class TestTimeoutController:
    """Test timeout controller."""

    @pytest.fixture
    def timeout_controller(self):
        return TimeoutController(default_timeout=30.0)

    async def test_create_timeout(self, timeout_controller):
        """Test creating timeout handle."""
        request_id = "test-req"
        handle = await timeout_controller.create_timeout(request_id, timeout_seconds=10.0)

        assert handle.request_id == request_id
        assert handle.timeout_seconds == 10.0
        assert not handle.is_expired()

    async def test_timeout_expiry(self, timeout_controller):
        """Test timeout expiry."""
        request_id = "test-req"
        handle = await timeout_controller.create_timeout(request_id, timeout_seconds=0.1)

        # Wait for timeout
        await asyncio.sleep(0.2)

        assert handle.is_expired()
        assert handle.get_remaining_seconds() == 0.0

    async def test_timeout_cancellation(self, timeout_controller):
        """Test timeout cancellation."""
        request_id = "test-req"
        handle = await timeout_controller.create_timeout(request_id, timeout_seconds=10.0)

        await handle.cancel()

        assert not handle.is_expired()
        assert handle.get_remaining_seconds() == 0.0

    def test_retry_after_header(self, timeout_controller):
        """Test Retry-After header generation."""
        headers = timeout_controller.get_retry_after_header(60)
        assert headers == {"Retry-After": "60"}

    def test_timeout_header(self, timeout_controller):
        """Test timeout hint headers."""
        headers = timeout_controller.get_timeout_header(30.0)
        assert "X-Timeout-Seconds" in headers
        assert "X-Retry-After-Timeout" in headers
        assert headers["X-Timeout-Seconds"] == "30"
        assert headers["X-Retry-After-Timeout"] == "60"


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    @pytest.fixture
    def circuit_breaker(self):
        return CircuitBreaker(failure_threshold=3, timeout_seconds=1.0)

    def test_circuit_breaker_initial_state(self, circuit_breaker):
        """Test initial circuit breaker state."""
        assert not circuit_breaker.is_open()
        assert circuit_breaker.get_state() == CircuitBreakerState.CLOSED

    def test_circuit_breaker_failure_threshold(self, circuit_breaker):
        """Test circuit breaker failure threshold."""
        # Record failures up to threshold
        for i in range(3):
            circuit_breaker.record_failure()
            assert not circuit_breaker.is_open()

        # Next failure should open the circuit
        circuit_breaker.record_failure()
        assert circuit_breaker.is_open()
        assert circuit_breaker.get_state() == CircuitBreakerState.OPEN

    def test_circuit_breaker_success_closes(self, circuit_breaker):
        """Test success closes circuit breaker."""
        # Open the circuit
        for i in range(4):
            circuit_breaker.record_failure()
        assert circuit_breaker.is_open()

        # Success should close it
        circuit_breaker.record_success()
        assert not circuit_breaker.is_open()
        assert circuit_breaker.get_state() == CircuitBreakerState.CLOSED

    def test_circuit_breaker_half_open_after_timeout(self, circuit_breaker):
        """Test circuit breaker half-open state after timeout."""
        # Open the circuit
        for i in range(4):
            circuit_breaker.record_failure()
        assert circuit_breaker.is_open()

        # Wait for timeout
        time.sleep(1.1)

        # Should be half-open (not open)
        assert not circuit_breaker.is_open()
        assert circuit_breaker.get_state() == CircuitBreakerState.HALF_OPEN


class TestRetryConfig:
    """Test retry configuration."""

    def test_retry_config_default_values(self):
        """Test default retry config values."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 10.0
        assert config.backoff_factor == 2.0
        assert config.jitter is True

    def test_retry_config_custom_values(self):
        """Test custom retry config values."""
        config = RetryConfig(
            max_retries=5,
            initial_delay=2.0,
            max_delay=20.0,
            backoff_factor=1.5,
            jitter=False
        )
        assert config.max_retries == 5
        assert config.initial_delay == 2.0
        assert config.max_delay == 20.0
        assert config.backoff_factor == 1.5
        assert config.jitter is False

    def test_calculate_delay(self):
        """Test delay calculation."""
        config = RetryConfig(initial_delay=1.0, max_delay=10.0, backoff_factor=2.0)

        # First retry
        delay = config.calculate_delay(0)
        assert 1.0 <= delay <= 1.25  # With jitter

        # Second retry
        delay = config.calculate_delay(1)
        assert 2.0 <= delay <= 2.5

        # Third retry (should be capped at max_delay)
        delay = config.calculate_delay(3)
        assert delay <= 10.0

    def test_calculate_delay_no_jitter(self):
        """Test delay calculation without jitter."""
        config = RetryConfig(jitter=False)

        # Should be exact values without jitter
        delay = config.calculate_delay(0)
        assert delay == 1.0

        delay = config.calculate_delay(1)
        assert delay == 2.0


class TestConnectionPool:
    """Test connection pool functionality."""

    @pytest.fixture
    def pool_config(self):
        return TrinoConfig(host="localhost", port=8080, user="test")

    @pytest.fixture
    def connection_pool(self, pool_config):
        return ConnectionPool(
            config=pool_config,
            min_size=2,
            max_size=5,
            max_idle=3,
            idle_timeout_seconds=60.0,
            wait_timeout_seconds=5.0
        )

    async def test_connection_pool_initialization(self, connection_pool):
        """Test connection pool initialization."""
        # Pool should initialize minimum connections
        assert connection_pool._initialized is False

        # Get first connection (should initialize pool)
        conn = await connection_pool.get_connection()
        assert connection_pool._initialized is True
        assert conn is not None

        # Return connection
        await connection_pool.return_connection(conn)

    async def test_connection_pool_max_size(self, connection_pool):
        """Test connection pool max size limit."""
        connections = []

        # Acquire max connections
        for i in range(5):
            conn = await connection_pool.get_connection()
            connections.append(conn)

        # Should not be able to get more connections
        with pytest.raises(ServiceUnavailableException):
            await connection_pool.get_connection()

        # Clean up
        for conn in connections:
            await connection_pool.return_connection(conn)

    async def test_connection_pool_wait_timeout(self, connection_pool):
        """Test connection pool wait timeout."""
        connections = []

        # Acquire all connections
        for i in range(5):
            conn = await connection_pool.get_connection()
            connections.append(conn)

        # Try to get connection with timeout
        with pytest.raises(ServiceUnavailableException):
            await asyncio.wait_for(
                connection_pool.get_connection(),
                timeout=1.0
            )

        # Clean up
        for conn in connections:
            await connection_pool.return_connection(conn)

    async def test_connection_pool_close(self, connection_pool):
        """Test connection pool close."""
        conn = await connection_pool.get_connection()
        await connection_pool.return_connection(conn)

        await connection_pool.close()

        # Pool should be closed
        assert connection_pool._closed is True

        # Should not be able to get connections from closed pool
        with pytest.raises(ServiceUnavailableException):
            await connection_pool.get_connection()


class TestTrinoClient:
    """Test Trino client with resilience features."""

    @pytest.fixture
    def trino_config(self):
        return TrinoConfig(host="localhost", port=8080, user="test")

    @pytest.fixture
    def concurrency_config(self):
        return {
            "trino_max_retries": 2,
            "trino_retry_delay_seconds": 0.1,
            "trino_max_retry_delay_seconds": 1.0,
            "trino_connection_timeout_seconds": 1.0,
            "trino_query_timeout_seconds": 1.0,
            "trino_circuit_breaker_failure_threshold": 2,
            "trino_circuit_breaker_timeout_seconds": 1.0,
            "trino_connection_pool_min_size": 1,
            "trino_connection_pool_max_size": 3,
            "max_concurrent_trino_connections": 5,
        }

    @pytest.fixture
    def trino_client(self, trino_config, concurrency_config):
        return TrinoClient(trino_config, concurrency_config)

    async def test_trino_client_initialization(self, trino_client):
        """Test Trino client initialization."""
        assert trino_client.config is not None
        assert trino_client.retry_config.max_retries == 2
        assert trino_client.timeout_config.query_timeout == 1.0
        assert trino_client.circuit_breaker.failure_threshold == 2

    async def test_trino_client_circuit_breaker(self, trino_client):
        """Test circuit breaker in Trino client."""
        # Mock the execute method to raise exceptions
        async def failing_execute(*args, **kwargs):
            raise Exception("Connection failed")

        trino_client._execute_query_with_timeout = failing_execute

        # Should not be open initially
        assert not trino_client.circuit_breaker.is_open()

        # First failure
        with pytest.raises(Exception):
            await trino_client.execute_query("SELECT 1")

        # Second failure should open circuit
        with pytest.raises(Exception):
            await trino_client.execute_query("SELECT 1")

        # Circuit should be open
        assert trino_client.circuit_breaker.is_open()

        # Should raise ServiceUnavailableException
        with pytest.raises(ServiceUnavailableException):
            await trino_client.execute_query("SELECT 1")

    async def test_trino_client_retry_logic(self, trino_client):
        """Test retry logic in Trino client."""
        call_count = 0

        async def failing_then_successful_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("First attempt failed")
            return [{"result": "success"}]

        trino_client._execute_query_with_timeout = failing_then_successful_execute

        # Should succeed on retry
        result = await trino_client.execute_query("SELECT 1", tenant_id="test")
        assert result == [{"result": "success"}]
        assert call_count == 2

    async def test_trino_client_health_status(self, trino_client):
        """Test health status reporting."""
        # Mock successful execution
        async def successful_execute(*args, **kwargs):
            return [{"health": "ok"}]

        trino_client._execute_query_with_timeout = successful_execute

        status = await trino_client.get_health_status()

        assert status["healthy"] is True
        assert status["circuit_breaker_state"] == "closed"
        assert "pool_size" in status

    async def test_trino_client_close(self, trino_client):
        """Test Trino client close."""
        await trino_client.close()

        # Should be able to close multiple times
        await trino_client.close()


class TestTrinoClientManager:
    """Test Trino client manager."""

    @pytest.fixture
    def trino_config(self):
        return TrinoConfig(host="localhost", port=8080, user="test")

    async def test_client_manager_singleton(self):
        """Test client manager singleton behavior."""
        manager1 = TrinoClientManager.get_instance()
        manager2 = TrinoClientManager.get_instance()

        assert manager1 is manager2

    async def test_client_manager_get_client(self, trino_config):
        """Test getting clients from manager."""
        manager = TrinoClientManager.get_instance()

        client1 = manager.get_client(trino_config)
        client2 = manager.get_client(trino_config)

        # Should return same client for same config
        assert client1 is client2

    async def test_client_manager_default_client(self, trino_config):
        """Test default client functionality."""
        manager = TrinoClientManager.get_instance()

        client = manager.get_client(trino_config)
        default_client = manager.get_default_client()

        assert default_client is client

    async def test_client_manager_close_all(self, trino_config):
        """Test closing all clients."""
        manager = TrinoClientManager.get_instance()

        client = manager.get_client(trino_config)
        await manager.close_all()

        # Manager should be empty
        assert len(manager._clients) == 0
        assert manager.get_default_client() is None


class TestConcurrencyMiddleware:
    """Test concurrency middleware."""

    @pytest.fixture
    def app(self):
        async def mock_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")]
            })
            await send({
                "type": "http.response.body",
                "body": b'{"message": "success"}'
            })
        return mock_app

    @pytest.fixture
    def middleware(self, app):
        concurrency_controller = create_concurrency_controller(max_concurrent_requests=10)
        rate_limiter = create_rate_limiter()
        timeout_controller = create_timeout_controller()

        return ConcurrencyMiddleware(
            app=app,
            concurrency_controller=concurrency_controller,
            rate_limiter=rate_limiter,
            timeout_controller=timeout_controller,
        )

    async def test_middleware_health_exclusion(self, middleware):
        """Test middleware excludes health endpoints."""
        scope = {
            "type": "http",
            "path": "/health",
            "method": "GET",
            "headers": []
        }

        send_called = False
        async def mock_send(message):
            nonlocal send_called
            send_called = True

        receive = AsyncMock()
        await middleware(scope, receive, mock_send)

        assert send_called

    async def test_middleware_rate_limiting(self, middleware):
        """Test middleware rate limiting."""
        scope = {
            "type": "http",
            "path": "/api/test",
            "method": "GET",
            "headers": [(b"x-aurum-tenant", b"test-tenant")]
        }

        # Exhaust rate limit
        for i in range(100):
            receive = AsyncMock()
            send = AsyncMock()
            await middleware(scope, receive, send)

        # Next request should be rate limited
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        # Check if rate limited response was sent
        rate_limited_call = None
        for call in send.call_args_list:
            if call[0][0]["type"] == "http.response.start":
                if call[0][0]["status"] == 429:
                    rate_limited_call = call
                    break

        assert rate_limited_call is not None

    async def test_middleware_concurrency_control(self, middleware):
        """Test middleware concurrency control."""
        scope = {
            "type": "http",
            "path": "/api/test",
            "method": "GET",
            "headers": [(b"x-aurum-tenant", b"test-tenant")]
        }

        # Acquire max concurrent requests
        tasks = []
        for i in range(10):
            async def make_request():
                receive = AsyncMock()
                send = AsyncMock()
                await middleware(scope, receive, send)
                return send

            tasks.append(make_request())

        # Start all requests
        pending_tasks = [asyncio.create_task(task) for task in tasks]

        # Try one more request - should be rejected
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        # Check for rejection
        rejected_call = None
        for call in send.call_args_list:
            if call[0][0]["type"] == "http.response.start":
                if call[0][0]["status"] == 503:
                    rejected_call = call
                    break

        # Cancel pending tasks
        for task in pending_tasks:
            task.cancel()

        assert rejected_call is not None

    async def test_middleware_timeout_control(self, middleware):
        """Test middleware timeout control."""
        async def slow_app(scope, receive, send):
            # Simulate slow response
            await asyncio.sleep(2.0)
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")]
            })
            await send({
                "type": "http.response.body",
                "body": b'{"message": "slow response"}'
            })

        # Create middleware with short timeout
        timeout_controller = create_timeout_controller(default_timeout=0.5)
        slow_middleware = ConcurrencyMiddleware(
            app=slow_app,
            concurrency_controller=create_concurrency_controller(),
            rate_limiter=create_rate_limiter(),
            timeout_controller=timeout_controller,
        )

        scope = {
            "type": "http",
            "path": "/api/slow",
            "method": "GET",
            "headers": []
        }

        receive = AsyncMock()
        send = AsyncMock()

        await slow_middleware(scope, receive, send)

        # Check for timeout response
        timeout_call = None
        for call in send.call_args_list:
            if call[0][0]["type"] == "http.response.start":
                if call[0][0]["status"] == 504:
                    timeout_call = call
                    break

        assert timeout_call is not None


class TestIntegrationScenarios:
    """Test integration scenarios for concurrency controls."""

    async def test_concurrent_requests_with_different_tenants(self):
        """Test concurrent requests from different tenants."""
        limits = RequestLimits(max_concurrent_requests=5, max_requests_per_tenant=2)
        controller = ConcurrencyController(limits)

        # Acquire slots for different tenants
        slots = []
        for tenant in ["tenant1", "tenant2", "tenant3"]:
            for i in range(2):
                slot = await controller.acquire_slot(
                    tenant_id=tenant,
                    request_id=f"req-{tenant}-{i}"
                )
                slots.append(slot)

        # Should have 6 active requests (2 per tenant)
        assert controller._active_requests == 6

        # Clean up
        for slot in slots:
            await slot.release()

    async def test_circuit_breaker_with_retries(self):
        """Test circuit breaker behavior with retries."""
        config = TrinoConfig(host="localhost", port=8080, user="test")
        concurrency_config = {
            "trino_max_retries": 2,
            "trino_circuit_breaker_failure_threshold": 2,
            "trino_circuit_breaker_timeout_seconds": 0.1,
        }

        client = TrinoClient(config, concurrency_config)

        # Mock failing execution
        call_count = 0
        async def failing_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise Exception(f"Attempt {call_count} failed")

        client._execute_query_with_timeout = failing_execute

        # Should fail and open circuit breaker
        with pytest.raises(Exception):
            await client.execute_query("SELECT 1", tenant_id="test")

        # Circuit should be open
        assert client.circuit_breaker.is_open()

        # Should raise ServiceUnavailableException
        with pytest.raises(ServiceUnavailableException):
            await client.execute_query("SELECT 1", tenant_id="test")

    async def test_rate_limit_with_tenant_isolation(self):
        """Test rate limiting with tenant isolation."""
        rate_limiter = RateLimiter()

        # Make requests for different tenants
        tenant1_identifier = "tenant1"
        tenant2_identifier = "tenant2"

        # Fill rate limit for tenant1
        for i in range(10):
            await rate_limiter.check_rate_limit(tenant1_identifier, max_requests=10, window_seconds=1.0)

        # tenant1 should be rate limited
        allowed = await rate_limiter.check_rate_limit(tenant1_identifier, max_requests=10, window_seconds=1.0)
        assert not allowed

        # tenant2 should still be allowed
        allowed = await rate_limiter.check_rate_limit(tenant2_identifier, max_requests=10, window_seconds=1.0)
        assert allowed

    async def test_timeout_with_early_cancellation(self):
        """Test timeout cancellation."""
        timeout_controller = TimeoutController()

        handle = await timeout_controller.create_timeout("test-req", timeout_seconds=10.0)

        # Should not be expired initially
        assert not handle.is_expired()
        assert handle.get_remaining_seconds() > 9.0

        # Cancel the timeout
        await handle.cancel()

        # Should be considered expired after cancellation
        assert not handle.is_expired()
        assert handle.get_remaining_seconds() == 0.0

    async def test_middleware_error_handling(self):
        """Test middleware error handling."""
        async def failing_app(scope, receive, send):
            raise ValueError("Application error")

        middleware = create_concurrency_middleware(
            app=failing_app,
            concurrency_controller=create_concurrency_controller(),
            rate_limiter=create_rate_limiter(),
            timeout_controller=create_timeout_controller(),
        )

        scope = {
            "type": "http",
            "path": "/api/error",
            "method": "GET",
            "headers": []
        }

        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        # Should send error response
        error_call = None
        for call in send.call_args_list:
            if call[0][0]["type"] == "http.response.start":
                if call[0][0]["status"] == 500:
                    error_call = call
                    break

        assert error_call is not None

    async def test_middleware_metrics_integration(self, middleware):
        """Test metrics integration in middleware."""
        # Make a successful request
        scope = {
            "type": "http",
            "path": "/api/test",
            "method": "GET",
            "headers": []
        }

        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        # Check that metrics were recorded (if prometheus available)
        if hasattr(middleware, 'concurrency_requests'):
            # Metrics should be available
            assert middleware.concurrency_requests is not None

        if hasattr(middleware, 'active_requests'):
            # Active requests should be updated
            assert middleware.active_requests is not None

    async def test_end_to_end_concurrency_flow(self):
        """Test end-to-end concurrency flow."""
        async def simple_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")]
            })
            await send({
                "type": "http.response.body",
                "body": b'{"status": "ok"}'
            })

        middleware = create_concurrency_middleware(
            app=simple_app,
            concurrency_controller=create_concurrency_controller(max_concurrent_requests=3),
            rate_limiter=create_rate_limiter(),
            timeout_controller=create_timeout_controller(),
        )

        # Make concurrent requests
        tasks = []
        for i in range(3):
            async def make_request():
                scope = {
                    "type": "http",
                    "path": f"/api/test/{i}",
                    "method": "GET",
                    "headers": []
                }
                receive = AsyncMock()
                send = AsyncMock()
                await middleware(scope, receive, send)
                return send.call_args_list[-1][0][0]["status"]

            tasks.append(asyncio.create_task(make_request()))

        # Wait for all requests
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(status == 200 for status in results)

    async def test_trino_client_manager_integration(self):
        """Test Trino client manager integration."""
        config = TrinoConfig(host="localhost", port=8080, user="test")
        concurrency_config = {"trino_max_retries": 1}

        # Get client from manager
        client = get_trino_client(config, concurrency_config)

        assert client is not None
        assert client.retry_config.max_retries == 1

        # Get default client
        default_client = await get_default_trino_client()
        assert default_client is client

        # Close all clients
        await TrinoClientManager.get_instance().close_all()
