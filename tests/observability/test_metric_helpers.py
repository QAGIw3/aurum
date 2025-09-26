"""Regression tests for shared Prometheus metric helpers."""

from __future__ import annotations

import asyncio

import pytest

from aurum.observability.metric_helpers import (
    clear_metric_cache,
    get_counter,
)
from aurum.api.rate_limiting.concurrency_middleware import (
    ConcurrencyMiddleware,
    create_concurrency_controller,
    create_timeout_controller,
)


@pytest.fixture(autouse=True)
def _clear_metric_cache():
    """Provide isolation across tests that mutate the metric cache."""

    clear_metric_cache()
    yield
    clear_metric_cache()


def test_metric_helpers_reuse_same_collector():
    """Requesting the same metric multiple times should reuse the collector."""

    counter_one = get_counter(
        "aurum_metric_helper_test_total",
        "Ensures helper reuses counters",
        ["outcome"],
    )
    counter_two = get_counter(
        "aurum_metric_helper_test_total",
        "Ensures helper reuses counters",
        ["outcome"],
    )

    assert counter_one is counter_two


def test_metric_cache_reset_creates_new_collectors():
    """Clearing the cache should drop existing collectors."""

    counter_one = get_counter(
        "aurum_metric_helper_cache_test_total",
        "Ensures cache clears",
        ["outcome"],
    )
    clear_metric_cache()
    counter_two = get_counter(
        "aurum_metric_helper_cache_test_total",
        "Ensures cache clears",
        ["outcome"],
    )

    assert counter_one is not counter_two


@pytest.mark.asyncio
async def test_concurrency_middleware_reuses_metric_collectors():
    """Multiple middleware instances should not register duplicate collectors."""

    async def noop_app(scope, receive, send):  # pragma: no cover - simple test shim
        return None

    middleware_one = ConcurrencyMiddleware(
        noop_app,
        create_concurrency_controller(),
        create_timeout_controller(),
    )
    middleware_two = ConcurrencyMiddleware(
        noop_app,
        create_concurrency_controller(),
        create_timeout_controller(),
    )

    assert middleware_one.concurrency_requests is middleware_two.concurrency_requests
    assert middleware_one.tenant_queue_depth is middleware_two.tenant_queue_depth

    # ensure metrics still usable via labels
    middleware_one.concurrency_requests.labels(result="success").inc()
    middleware_two.rejections.labels(reason="test", tenant="demo").inc()

    handles = await asyncio.gather(
        middleware_one.timeout_controller.create_timeout("test-1"),
        middleware_two.timeout_controller.create_timeout("test-2"),
    )
    await asyncio.gather(*(handle.cancel() for handle in handles))
