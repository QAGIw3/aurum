"""Tests for in-process concurrency diagnostics endpoints."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from aurum.api.rate_limiting.concurrency_middleware import (
    ConcurrencyMiddleware,
    create_concurrency_controller,
    create_timeout_controller,
    local_diagnostics_router,
)


@pytest.mark.asyncio
async def test_local_concurrency_diagnostics_endpoints() -> None:
    app = FastAPI()
    app.include_router(local_diagnostics_router)

    controller = create_concurrency_controller(max_concurrent_requests=2)
    app.state.concurrency_controller = controller
    app.state.concurrency_limits = controller.limits

    wrapped = ConcurrencyMiddleware(
        app=app,
        concurrency_controller=controller,
        timeout_controller=create_timeout_controller(),
    )

    transport = ASGITransport(app=wrapped)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        stats_response = await client.get("/v1/admin/concurrency/local/stats")
        assert stats_response.status_code == 200
        stats_payload = stats_response.json()["data"]
        assert stats_payload["active_total"] >= 0
        assert "tenant_active" in stats_payload
        assert "tenant_queue" in stats_payload

        health_response = await client.get("/v1/admin/concurrency/local/health")
        assert health_response.status_code == 200
        assert health_response.json()["status"] == "ok"
