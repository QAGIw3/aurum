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
    ConcurrencyRejected,
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
from aurum.api.database import trino_client as trino_module
from aurum.api.database.trino_client import (
    SimpleTrinoClient as TrinoClient,
    HybridTrinoClientManager as TrinoClientManager,
    get_trino_client,
    get_default_trino_client,
)
from aurum.common.circuit_breaker import CircuitBreaker
from aurum.staleness.auto_remediation import CircuitBreakerState
from aurum.api.performance import ConnectionPool
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
        assert limits.tenant_queue_limit == 64
        assert limits.queue_timeout_seconds == 2.0
        assert limits.burst_refill_per_second == 0.5
        assert limits.slow_start_initial_limit == 2
        assert limits.slow_start_step_seconds == 3.0
        assert limits.slow_start_step_size == 1
        assert limits.slow_start_cooldown_seconds == 30.0

    def test_request_limits_custom_values(self):
        """Test custom request limits."""
        limits = RequestLimits(
            max_concurrent_requests=50,
            max_requests_per_second=5.0,
            max_request_duration_seconds=60.0,
            max_requests_per_tenant=10,
            tenant_burst_limit=25,
            tenant_queue_limit=10,
            queue_timeout_seconds=0.5,
            burst_refill_per_second=1.5,
            slow_start_initial_limit=3,
            slow_start_step_seconds=1.0,
            slow_start_step_size=2,
            slow_start_cooldown_seconds=10.0,
        )
        assert limits.max_concurrent_requests == 50
        assert limits.max_requests_per_second == 5.0
        assert limits.max_request_duration_seconds == 60.0
        assert limits.max_requests_per_tenant == 10
        assert limits.tenant_burst_limit == 25
        assert limits.tenant_queue_limit == 10
        assert limits.queue_timeout_seconds == 0.5
        assert limits.burst_refill_per_second == 1.5
        assert limits.slow_start_initial_limit == 3
        assert limits.slow_start_step_seconds == 1.0
        assert limits.slow_start_step_size == 2
        assert limits.slow_start_cooldown_seconds == 10.0


class TestConcurrencyController:
    """Test concurrency controller."""

    @pytest.fixture
    def limits(self):
        return RequestLimits(
            max_concurrent_requests=2,
            max_requests_per_tenant=2,
            tenant_queue_limit=4,
            queue_timeout_seconds=0.4,
        )

    @pytest.fixture
    def controller(self, limits):
        return ConcurrencyController(limits)

    async def test_acquire_slot_allows_waiting_tenants(self, controller):
        """Ensure queued tenants eventually receive capacity without starvation."""

        first = await controller.acquire_slot(tenant_id="tenantA", request_id="req-a1")
        second = await controller.acquire_slot(tenant_id="tenantA", request_id="req-a2")

        waiter_a = asyncio.create_task(
            controller.acquire_slot(tenant_id="tenantA", request_id="req-a3")
        )
        waiter_b = asyncio.create_task(
            controller.acquire_slot(tenant_id="tenantB", request_id="req-b1")
        )

        await asyncio.sleep(0)  # allow tasks to queue

        assert not waiter_a.done()
        assert not waiter_b.done()

        await first.release()
        await second.release()

        slot_a3 = await asyncio.wait_for(waiter_a, timeout=1.0)
        slot_b1 = await asyncio.wait_for(waiter_b, timeout=1.0)

        assert slot_a3.tenant_id == "tenantA"
        assert slot_b1.tenant_id == "tenantB"
        assert slot_b1.queue_wait_seconds >= 0.0

        await slot_a3.release()
        await slot_b1.release()

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
        old_time = time.perf_counter() - 120  # 2 minutes ago
        controller.limits._tenant_request_times["tenant1"] = [old_time]

        slot = await controller.acquire_slot(tenant_id="tenant1", request_id="req-1")
        assert len(controller.limits._tenant_request_times["tenant1"]) == 1

        await slot.release()

    async def test_tenant_queue_limit_enforced(self):
        """Tenant queue limits should reject excessive bursts with 429."""
        limits = RequestLimits(
            max_concurrent_requests=1,
            max_requests_per_tenant=1,
            tenant_queue_limit=1,
            queue_timeout_seconds=0.5,
        )
        controller = ConcurrencyController(limits)

        first = await controller.acquire_slot(tenant_id="tenantA", request_id="req-1")
        waiter = asyncio.create_task(
            controller.acquire_slot(tenant_id="tenantA", request_id="req-2")
        )
        await asyncio.sleep(0)

        with pytest.raises(ConcurrencyRejected) as exc_info:
            await controller.acquire_slot(tenant_id="tenantA", request_id="req-3")

        assert exc_info.value.reason == "tenant_queue_full"
        assert exc_info.value.status_code == 429

        await first.release()
        slot_two = await waiter
        assert slot_two.queue_wait_seconds >= 0.0
        await slot_two.release()

    async def test_queue_timeout_returns_503(self):
        """Requests should time out if capacity never frees."""
        limits = RequestLimits(
            max_concurrent_requests=1,
            max_requests_per_tenant=1,
            tenant_queue_limit=4,
            queue_timeout_seconds=0.05,
        )
        controller = ConcurrencyController(limits)

        slot = await controller.acquire_slot(tenant_id="tenantA", request_id="req-1")

        waiter = asyncio.create_task(
            controller.acquire_slot(tenant_id="tenantB", request_id="req-2")
        )

        await asyncio.sleep(0.2)

        with pytest.raises(ConcurrencyRejected) as exc_info:
            await waiter

        assert exc_info.value.reason == "queue_timeout"
        assert exc_info.value.status_code == 503

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
        return CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)

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

    @pytest.fixture
    def patch_trino_metrics(self, monkeypatch):
        """Patch asynchronous metric helpers with no-op coroutines."""

        async def _noop(*args, **kwargs):  # pragma: no cover - helper
            return None

        for name in [
            "increment_trino_queries",
            "observe_trino_query_duration",
            "increment_trino_queue_rejections",
            "observe_trino_connection_acquire_time",
            "set_trino_connection_pool_active",
            "set_trino_connection_pool_idle",
            "set_trino_connection_pool_utilization",
            "set_trino_pool_saturation",
            "set_trino_pool_saturation_status",
            "set_trino_request_queue_depth",
            "set_trino_circuit_breaker_state",
        ]:
            monkeypatch.setattr(trino_module, name, _noop, raising=False)

        return _noop

    @pytest.mark.asyncio
    async def test_trino_client_acquire_connection_retries(self, trino_client, patch_trino_metrics, monkeypatch):
        """Connection acquisition should retry on timeouts and report metrics."""

        class FlakyPool:
            def __init__(self):
                self._connections = []
                self._active_connections = 0
                self.max_size = 2
                self.calls = 0

            async def get_connection(self):
                self.calls += 1
                if self.calls == 1:
                    raise asyncio.TimeoutError
                self._active_connections += 1
                return {"id": "conn"}

            async def return_connection(self, conn):  # pragma: no cover - safety
                self._active_connections = max(0, self._active_connections - 1)

        flaky_pool = FlakyPool()
        trino_client._pool = flaky_pool
        monkeypatch.setattr(trino_client, "_record_pool_metrics", AsyncMock())
        monkeypatch.setattr(trino_module.asyncio, "sleep", AsyncMock())

        connection = await trino_client._acquire_connection()
        assert connection["id"] == "conn"
        assert flaky_pool.calls == 2

        await trino_client._release_connection(connection)

    @pytest.mark.asyncio
    async def test_trino_client_acquire_connection_exhausted(self, trino_client, patch_trino_metrics, monkeypatch):
        """Exhausted pool should raise ServiceUnavailableException after retries."""

        class ExhaustedPool:
            def __init__(self):
                self._connections = []
                self._active_connections = 0
                self.max_size = 1

            async def get_connection(self):
                raise ServiceUnavailableException("trino", detail="Connection pool exhausted")

            async def return_connection(self, conn):
                pass

        trino_client._pool = ExhaustedPool()
        trino_client.acquire_attempts = 1
        monkeypatch.setattr(trino_module.asyncio, "sleep", AsyncMock())

        with pytest.raises(ServiceUnavailableException):
            await trino_client._acquire_connection()

    @pytest.mark.asyncio
    async def test_trino_client_run_query_transforms_rows(self, trino_client, patch_trino_metrics):
        """Row fetching should marshal column names and values."""

        class DummyCursor:
            def __init__(self):
                self.description = [("value", None, None, None, None, None, None)]

            def execute(self, sql, params=None):
                self.sql = sql

            def fetchall(self):
                return [("ok",)]

            def close(self):
                pass

        class DummyConnection:
            def cursor(self):
                return DummyCursor()

            def commit(self):
                self.committed = True

        result = await trino_client._run_query(DummyConnection(), "SELECT 'ok'", None)
        assert result == [{"value": "ok"}]

    @pytest.mark.asyncio
    async def test_trino_client_run_query_without_results_commits(self, trino_client, patch_trino_metrics):
        """When a query returns no rows we should commit the transaction."""

        class DummyCursor:
            description = None

            def execute(self, sql, params=None):
                pass

            def fetchall(self):
                return []

            def close(self):
                pass

        class DummyConnection:
            def __init__(self):
                self.committed = False

            def cursor(self):
                return DummyCursor()

            def commit(self):
                self.committed = True

        conn = DummyConnection()
        rows = await trino_client._run_query(conn, "DELETE FROM t", None)
        assert rows == []
        assert conn.committed is True

    def test_trino_client_record_breaker_failure_opens_circuit(self, trino_client, patch_trino_metrics):
        """Recording enough failures should transition breaker to open state."""

        threshold = trino_client.circuit_breaker.failure_threshold
        for _ in range(threshold + 1):
            trino_client._record_breaker_failure()

        assert trino_client.circuit_breaker.is_open()

    def test_trino_client_cache_helpers(self, trino_client):
        """Cache helper utilities should produce stable keys and classifications."""

        key_one = trino_client._cache_key("SELECT 1", {"foo": "bar"})
        key_two = trino_client._cache_key("SELECT 1", {"foo": "bar"})
        key_three = trino_client._cache_key("SELECT 2", {"foo": "bar"})

        assert key_one == key_two
        assert key_one != key_three
        assert trino_client._classify_query("SELECT 1") == "read"
        assert trino_client._classify_query("INSERT INTO t VALUES (1)") == "write"
        assert trino_client._classify_query("MERGE INTO t USING s") == "write"
        assert trino_client._classify_query("") == "unknown"

    def test_trino_client_extract_cache_policy(self, trino_client):
        """Cache policy extraction should respect explicit and implicit hints."""

        use_cache, ttl = trino_client._extract_cache_policy("SELECT 1", {"_cache_ttl": "2"}, explicit_ttl=None)
        assert use_cache and ttl == 2.0

        use_cache, ttl = trino_client._extract_cache_policy("SELECT 1", {"_cache_control": "max-age=3"}, explicit_ttl=None)
        assert use_cache and ttl == 3.0

        use_cache, ttl = trino_client._extract_cache_policy("SELECT 1", {}, explicit_ttl=0)
        assert use_cache is False and ttl == 0.0

        params = {"_cache_control": "max-age=bogus"}
        use_cache, ttl = trino_client._extract_cache_policy("SELECT 1", params, explicit_ttl=None)
        assert use_cache and ttl == trino_client._cache.default_ttl

    @pytest.mark.asyncio
    async def test_trino_client_ttl_cache_operations(self, monkeypatch):
        """TTL cache should honour set, get, invalidate, and clear semantics."""

        cache = trino_module.TTLCache(maxsize=2, default_ttl=1.0)
        await cache.set("one", {"value": 1})
        assert await cache.get("one") == {"value": 1}

        await cache.invalidate("one")
        assert await cache.get("one") is None

        await cache.set("skip", {"value": 0}, ttl=0)  # ttl <= 0 short-circuits

        base_time = time.time()
        monkeypatch.setattr(trino_module.time, "time", lambda: base_time)
        await cache.set("expires", {"value": 2}, ttl=0.5)
        monkeypatch.setattr(trino_module.time, "time", lambda: base_time + 1.0)
        assert await cache.get("expires") is None

        await cache.set("two", {"value": 2})
        await cache.set("two", {"value": 22})  # existing key moves to end
        await cache.set("three", {"value": 3})  # triggers eviction of oldest when needed
        await cache.clear()
        assert await cache.get("two") is None

    @pytest.mark.asyncio
    async def test_trino_client_execute_query_with_timeout_uses_wait_for(
        self,
        trino_client,
        patch_trino_metrics,
        monkeypatch,
    ):
        """Execute query path should honour wait_for when a timeout is provided."""

        conn = object()
        monkeypatch.setattr(trino_client, "_acquire_connection", AsyncMock(return_value=conn))
        monkeypatch.setattr(trino_client, "_release_connection", AsyncMock())
        run_query = AsyncMock(return_value=[{"value": 1}])
        monkeypatch.setattr(trino_client, "_run_query", run_query)
        call_args = {}

        async def fake_wait_for(coro, timeout):
            call_args["timeout"] = timeout
            return await coro

        monkeypatch.setattr(trino_module.asyncio, "wait_for", fake_wait_for)

        result = await trino_client._execute_query_with_timeout("SELECT 1", None, 0.5)

        assert result == [{"value": 1}]
        assert call_args["timeout"] == 0.5
        trino_client._release_connection.assert_awaited_with(conn)

    @pytest.mark.asyncio
    async def test_trino_client_execute_query_with_timeout_releases_on_error(
        self,
        trino_client,
        patch_trino_metrics,
        monkeypatch,
    ):
        """Connections should always be released when the query fails."""

        conn = object()
        monkeypatch.setattr(trino_client, "_acquire_connection", AsyncMock(return_value=conn))
        monkeypatch.setattr(trino_client, "_release_connection", AsyncMock())
        monkeypatch.setattr(trino_client, "_run_query", AsyncMock(return_value=[]))
        async def failing_wait_for(coro, timeout):
            await coro
            raise RuntimeError("boom")

        monkeypatch.setattr(trino_module.asyncio, "wait_for", failing_wait_for)

        with pytest.raises(RuntimeError):
            await trino_client._execute_query_with_timeout("SELECT 1", None, 0.1)

        trino_client._release_connection.assert_awaited_with(conn)

    @pytest.mark.asyncio
    async def test_trino_client_run_query_rolls_back_on_error(self, trino_client, patch_trino_metrics):
        """Failed queries should invoke rollback when available."""

        class ErrorCursor:
            description = [("value", None, None, None, None, None, None)]

            def execute(self, sql, params=None):
                raise RuntimeError("boom")

            def close(self):
                self.closed = True

        class ErrorConnection:
            def __init__(self):
                self.rolled_back = False

            def cursor(self):
                return ErrorCursor()

            def rollback(self):
                self.rolled_back = True

        conn = ErrorConnection()
        with pytest.raises(RuntimeError):
            await trino_client._run_query(conn, "SELECT 1", None)

        assert conn.rolled_back is True


class TestTrinoClientManager:
    """Test Trino client manager."""

    @pytest.fixture
    def trino_config(self):
        return TrinoConfig(host="localhost", port=8080, user="test")

    @pytest.fixture(autouse=True)
    async def reset_manager(self):
        """Ensure the shared manager is cleaned up around every test."""

        manager = TrinoClientManager.get_instance()
        await manager.close_all()
        try:
            yield
        finally:
            await manager.close_all()

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

        default_client = manager.get_default_client()
        assert default_client is manager.get_client(None)
        assert default_client is manager.get_client(TrinoConfig())
        # Different configurations should yield distinct client instances
        assert default_client is not manager.get_client(trino_config)

    async def test_client_manager_close_all(self, trino_config):
        """Test closing all clients."""
        manager = TrinoClientManager.get_instance()

        client = manager.get_client(trino_config)
        await manager.close_all()

        # Manager should release cached clients
        assert len(manager._clients) == 0

        # New acquisitions should produce fresh client instances
        replacement = manager.get_client(trino_config)
        assert replacement is not client

        # Synchronous helper should schedule shutdown without raising
        pending = manager.close_all_sync()
        if pending is not None:
            await pending

    async def test_client_manager_switch_modes(self, trino_config):
        """Switch between simplified and legacy clients."""

        manager = TrinoClientManager.get_instance()
        await manager.close_all()

        legacy_stub = MagicMock()
        legacy_stub.get_client.return_value = TrinoClient(trino_config)
        legacy_stub.close_all = AsyncMock(return_value=None)

        manager._legacy_manager = legacy_stub
        manager._use_simple = False
        manager._migration_phase = "legacy"

        legacy_client = manager.get_client(trino_config)
        assert legacy_client is legacy_stub.get_client.return_value
        assert manager.switch_to_legacy() is True

        # Seed simplified clients and switch back
        key = manager._build_config_key(trino_config)
        manager._simple_clients[key] = legacy_client
        assert manager.switch_to_simplified() is True

        await manager.close_all()
        legacy_stub.close_all.assert_awaited()


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
        gate = asyncio.Event()

        async def blocking_app(scope, receive, send):
            await gate.wait()
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            })
            await send({
                "type": "http.response.body",
                "body": b"{}",
            })

        controller = create_concurrency_controller(
            max_concurrent_requests=1,
            max_requests_per_tenant=1,
            tenant_queue_limit=1,
            queue_timeout_seconds=0.05,
        )
        constrained_middleware = ConcurrencyMiddleware(
            app=blocking_app,
            concurrency_controller=controller,
            rate_limiter=create_rate_limiter(),
            timeout_controller=create_timeout_controller(),
        )

        scope = {
            "type": "http",
            "path": "/api/test",
            "method": "GET",
            "headers": [(b"x-aurum-tenant", b"tenant-a")],
        }

        first_send = AsyncMock()
        first_task = asyncio.create_task(
            constrained_middleware(scope, AsyncMock(), first_send)
        )

        await asyncio.sleep(0.01)

        second_send = AsyncMock()
        await constrained_middleware(scope, AsyncMock(), second_send)

        rejected_statuses = [
            call[0][0]["status"]
            for call in second_send.call_args_list
            if call[0][0]["type"] == "http.response.start"
        ]

        assert 503 in rejected_statuses

        gate.set()
        await first_task

    async def test_middleware_queue_overflow_returns_429(self):
        """Queue overflow should surface as HTTP 429."""

        gate = asyncio.Event()

        async def blocking_app(scope, receive, send):
            await gate.wait()
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            })
            await send({
                "type": "http.response.body",
                "body": b"{}",
            })

        controller = create_concurrency_controller(
            max_concurrent_requests=1,
            max_requests_per_tenant=1,
            tenant_queue_limit=1,
            queue_timeout_seconds=0.5,
        )
        overflow_middleware = ConcurrencyMiddleware(
            app=blocking_app,
            concurrency_controller=controller,
            rate_limiter=create_rate_limiter(),
            timeout_controller=create_timeout_controller(),
        )

        scope = {
            "type": "http",
            "path": "/api/test",
            "method": "GET",
            "headers": [(b"x-aurum-tenant", b"tenant-a")],
        }

        first_task = asyncio.create_task(
            overflow_middleware(scope, AsyncMock(), AsyncMock())
        )
        await asyncio.sleep(0.01)

        second_task_send = AsyncMock()
        second_task = asyncio.create_task(
            overflow_middleware(scope, AsyncMock(), second_task_send)
        )
        await asyncio.sleep(0.01)

        third_send = AsyncMock()
        await overflow_middleware(scope, AsyncMock(), third_send)

        statuses = [
            call[0][0]["status"]
            for call in third_send.call_args_list
            if call[0][0]["type"] == "http.response.start"
        ]

        assert 429 in statuses

        gate.set()
        await first_task
        await second_task

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

    async def test_concurrent_requests_with_different_tenants(self):
        """Test concurrent requests from different tenants."""
        limits = RequestLimits(max_concurrent_requests=6, max_requests_per_tenant=2)
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
                for call in send.call_args_list:
                    message = call[0][0]
                    if message.get("type") == "http.response.start":
                        return message.get("status")
                return None

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

        # Default client should correspond to default configuration cache
        default_client = get_default_trino_client()
        assert default_client is get_trino_client(TrinoConfig())

        # Close all clients
        await TrinoClientManager.get_instance().close_all()


class TestDatabaseMigrationMetrics:
    """Exercise migration metrics helper to improve coverage."""

    def test_database_migration_metrics_recording(self, tmp_path, monkeypatch):
        monkeypatch.setenv(trino_module.DB_FEATURE_FLAGS["ENABLE_DB_MIGRATION_MONITORING"], "true")
        monkeypatch.setenv("HOME", str(tmp_path))

        metrics = trino_module.DatabaseMigrationMetrics()
        metrics.record_db_call("simplified", 12.5)
        metrics.record_db_call("legacy", 5.0, error=True)
        metrics.set_migration_phase("simplified")

        data = metrics._metrics["db_migration"]
        assert data["simplified_calls"] == 1
        assert data["legacy_calls"] == 1
        assert data["errors"] == 1
        assert metrics.is_monitoring_enabled() is True
