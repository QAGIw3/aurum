"""End-to-end tests for concurrency middleware headers."""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from aurum.api.rate_limiting.concurrency_middleware import (
    ConcurrencyMiddleware,
    create_concurrency_controller,
    create_timeout_controller,
)
from aurum.api.rate_limiting.redis_concurrency import (
    RedisConcurrencyBackend,
    RedisConcurrencyConfig,
)

try:  # pragma: no cover - optional dependency for distributed tests
    import fakeredis.aioredis as fakeredis_aioredis  # type: ignore
except ImportError:  # pragma: no cover
    fakeredis_aioredis = None  # type: ignore


def _make_client(app) -> AsyncClient:
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://testserver")


def _blocking_app(first_started: asyncio.Event, gate: asyncio.Event):
    async def _app(scope, receive, send):
        if not first_started.is_set():
            first_started.set()
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

    return _app


def _assert_rejection_headers(response):
    headers = response.headers
    assert "Retry-After" in headers
    assert headers["Retry-After"].isdigit()
    assert "X-Queue-Depth" in headers
    assert headers["X-Queue-Depth"].isdigit()
    assert "X-Backpressure" in headers
    assert headers["X-Backpressure"]


@pytest.mark.asyncio
async def test_local_concurrency_queue_overflow_headers() -> None:
    first_started = asyncio.Event()
    gate = asyncio.Event()

    controller = create_concurrency_controller(
        max_concurrent_requests=1,
        max_requests_per_tenant=1,
        tenant_queue_limit=1,
        queue_timeout_seconds=2.0,
    )
    assert controller.limits.tenant_queue_limit == 1

    middleware = ConcurrencyMiddleware(
        app=_blocking_app(first_started, gate),
        concurrency_controller=controller,
        timeout_controller=create_timeout_controller(),
        backpressure_threshold=0.0,
    )

    async with _make_client(middleware) as client:
        task_one = asyncio.create_task(client.get("/resource"))
        await asyncio.wait_for(first_started.wait(), timeout=1.0)

        queued_task = asyncio.create_task(client.get("/resource"))
        await asyncio.sleep(0.05)
        overflow_response = await client.get("/resource")

        assert overflow_response.status_code == 429
        _assert_rejection_headers(overflow_response)

        gate.set()
        await asyncio.gather(task_one, queued_task)


@pytest.mark.asyncio
async def test_local_concurrency_timeout_headers() -> None:
    first_started = asyncio.Event()
    gate = asyncio.Event()

    controller = create_concurrency_controller(
        max_concurrent_requests=1,
        max_requests_per_tenant=1,
        tenant_queue_limit=1,
        queue_timeout_seconds=0.1,
    )

    middleware = ConcurrencyMiddleware(
        app=_blocking_app(first_started, gate),
        concurrency_controller=controller,
        timeout_controller=create_timeout_controller(),
        backpressure_threshold=0.0,
    )

    async with _make_client(middleware) as client:
        task_one = asyncio.create_task(client.get("/resource"))
        await asyncio.wait_for(first_started.wait(), timeout=1.0)

        timeout_response = await client.get("/resource")

        assert timeout_response.status_code == 503
        _assert_rejection_headers(timeout_response)

        gate.set()
        await task_one


@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis_aioredis is None, reason="fakeredis is required for distributed tests")
async def test_distributed_concurrency_queue_overflow_headers() -> None:
    first_started = asyncio.Event()
    gate = asyncio.Event()

    redis_client = fakeredis_aioredis.FakeRedis()
    backend = RedisConcurrencyBackend(
        redis_client,
        config=RedisConcurrencyConfig(url="redis://fake"),
    )

    controller = create_concurrency_controller(
        backend=backend,
        max_concurrent_requests=1,
        max_requests_per_tenant=1,
        tenant_queue_limit=1,
        queue_timeout_seconds=2.0,
    )

    middleware = ConcurrencyMiddleware(
        app=_blocking_app(first_started, gate),
        concurrency_controller=controller,
        timeout_controller=create_timeout_controller(),
        backpressure_threshold=0.0,
    )

    async with _make_client(middleware) as client:
        task_one = asyncio.create_task(client.get("/resource"))
        await asyncio.wait_for(first_started.wait(), timeout=1.0)

        queued_task = asyncio.create_task(client.get("/resource"))
        await asyncio.sleep(0.05)
        overflow_response = await client.get("/resource")

        assert overflow_response.status_code == 429
        _assert_rejection_headers(overflow_response)

        gate.set()
        await asyncio.gather(task_one, queued_task)


@pytest.mark.asyncio
@pytest.mark.skipif(fakeredis_aioredis is None, reason="fakeredis is required for distributed tests")
async def test_distributed_concurrency_timeout_headers() -> None:
    first_started = asyncio.Event()
    gate = asyncio.Event()

    redis_client = fakeredis_aioredis.FakeRedis()
    backend = RedisConcurrencyBackend(
        redis_client,
        config=RedisConcurrencyConfig(url="redis://fake"),
    )

    controller = create_concurrency_controller(
        backend=backend,
        max_concurrent_requests=1,
        max_requests_per_tenant=1,
        tenant_queue_limit=1,
        queue_timeout_seconds=0.1,
    )

    middleware = ConcurrencyMiddleware(
        app=_blocking_app(first_started, gate),
        concurrency_controller=controller,
        timeout_controller=create_timeout_controller(),
        backpressure_threshold=0.0,
    )

    async with _make_client(middleware) as client:
        task_one = asyncio.create_task(client.get("/resource"))
        await asyncio.wait_for(first_started.wait(), timeout=1.0)

        timeout_response = await client.get("/resource")

        assert timeout_response.status_code == 503
        _assert_rejection_headers(timeout_response)

        gate.set()
        await task_one
