"""Tests for the sliding-window rate limit middleware."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from aurum.api.config import CacheConfig
from aurum.api.rate_limiting.sliding_window import RateLimitConfig, RateLimitMiddleware


def _build_client(rl_cfg: RateLimitConfig) -> AsyncClient:
    cache_cfg = CacheConfig()
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        api=SimpleNamespace(metrics=SimpleNamespace(path="/metrics"))
    )

    @app.get("/resource")
    async def _resource():
        return {}

    middleware = RateLimitMiddleware(app, cache_cfg, rl_cfg)
    return AsyncClient(transport=ASGITransport(app=middleware), base_url="http://testserver")


@pytest.mark.asyncio
async def test_rate_limit_headers_on_rps_exceeded() -> None:
    rl_cfg = RateLimitConfig(rps=1, burst=0, daily_cap=100)

    async with _build_client(rl_cfg) as client:
        tenant_headers = {"X-Aurum-Tenant": "tenant-a"}
        first = await client.get("/resource", headers=tenant_headers)
        assert first.status_code == 200

        second = await client.get("/resource", headers=tenant_headers)
        assert second.status_code == 429

        headers = second.headers
        assert headers["Retry-After"].isdigit()
        assert headers["X-RateLimit-Limit"] == "1"
        assert headers["X-RateLimit-Remaining"] == "0"
        assert headers["X-RateLimit-Reset"].isdigit()


@pytest.mark.asyncio
async def test_rate_limit_whitelist_bypass() -> None:
    rl_cfg = RateLimitConfig(
        rps=1,
        burst=0,
        daily_cap=100,
        identifier_header="X-Test-Id",
        whitelist=("trusted",),
    )

    async with _build_client(rl_cfg) as client:
        tenant_headers = {"X-Aurum-Tenant": "tenant-a"}
        await client.get("/resource", headers=tenant_headers)
        limited = await client.get("/resource", headers=tenant_headers)
        assert limited.status_code == 429

        whitelisted_headers = {
            "X-Aurum-Tenant": "tenant-a",
            "X-Test-Id": "trusted",
        }
        allowed = await client.get("/resource", headers=whitelisted_headers)
        assert allowed.status_code == 200

        # Ensure cooldown to avoid leaking between tests
        await asyncio.sleep(1.1)
