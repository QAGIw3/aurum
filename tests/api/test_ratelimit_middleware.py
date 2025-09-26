from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pytest

PROJECT_SRC = Path(__file__).resolve().parents[2] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

API_PACKAGE = "aurum.api"
api_path = PROJECT_SRC / "aurum" / "api"
if API_PACKAGE not in sys.modules:
    api_stub = types.ModuleType(API_PACKAGE)
    api_stub.__path__ = [str(api_path)]  # type: ignore[attr-defined]
    sys.modules[API_PACKAGE] = api_stub


@dataclass
class DummyCacheConfig:
    redis_url: Optional[str] = None
    ttl_seconds: int = 0
    namespace: str = "test"
    mode: str = "standalone"
    sentinel_endpoints: Tuple[Tuple[str, int], ...] = ()
    sentinel_master: Optional[str] = None
    cluster_nodes: Tuple[str, ...] = ()
    username: Optional[str] = None
    password: Optional[str] = None
    socket_timeout: float = 1.5
    connect_timeout: float = 1.5


@dataclass
class DummyRateLimitConfig:
    rps: int
    burst: int
    overrides: Dict[str, Tuple[int, int]]
    identifier_header: Optional[str]
    whitelist: Tuple[str, ...]
    daily_cap: int = 100_000

    async def limits_for_path(self, path: str, tenant: Optional[str] = None) -> Tuple[int, int]:
        for prefix, limits in self.overrides.items():
            if path.startswith(prefix):
                return limits
        return self.rps, self.burst

    def is_whitelisted(self, *identifiers: Optional[str]) -> bool:
        if not self.whitelist:
            return False
        candidates = {value for value in identifiers if value}
        if not candidates:
            return False
        whitelist_set = set(self.whitelist)
        return any(candidate in whitelist_set for candidate in candidates)


def _load_rate_limit_module():
    """Return the canonical rate limiting module for tests."""

    return importlib.import_module("aurum.api.rate_limiting.sliding_window")


def _build_app(rl_cfg: DummyRateLimitConfig):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    RateLimitMiddleware = _load_rate_limit_module().RateLimitMiddleware

    async def handler(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/ping", handler)])
    app.add_middleware(RateLimitMiddleware, cache_cfg=DummyCacheConfig(), rl_cfg=rl_cfg)
    return app


def test_rate_limit_returns_429(monkeypatch: pytest.MonkeyPatch):
    from fastapi.testclient import TestClient

    rl_cfg = DummyRateLimitConfig(rps=1, burst=0, overrides={}, identifier_header=None, whitelist=())
    app = _build_app(rl_cfg)
    rl_module = _load_rate_limit_module()
    monkeypatch.setattr(rl_module.time, "time", lambda: 1_000_000)

    client = TestClient(app)

    assert client.get("/ping").status_code == 200
    resp = client.get("/ping")
    assert resp.status_code == 429
    assert resp.headers.get("Retry-After") == "1"
    assert resp.headers.get("X-RateLimit-Limit") == "1"
    assert resp.headers.get("X-RateLimit-Remaining") == "0"


def test_rate_limit_counts_per_client_ip(monkeypatch: pytest.MonkeyPatch):
    from fastapi.testclient import TestClient

    rl_cfg = DummyRateLimitConfig(rps=1, burst=0, overrides={}, identifier_header=None, whitelist=())
    app = _build_app(rl_cfg)
    rl_module = _load_rate_limit_module()
    monkeypatch.setattr(rl_module.time, "time", lambda: 2_000_000)

    client = TestClient(app)

    assert client.get("/ping", headers={"x-forwarded-for": "1.1.1.1"}).status_code == 200
    assert client.get("/ping", headers={"x-forwarded-for": "2.2.2.2"}).status_code == 200


def test_rate_limit_whitelist(monkeypatch: pytest.MonkeyPatch):
    from fastapi.testclient import TestClient

    rl_cfg = DummyRateLimitConfig(
        rps=1,
        burst=0,
        overrides={},
        identifier_header="X-Client-Id",
        whitelist=("vip-client",),
    )
    app = _build_app(rl_cfg)
    rl_module = _load_rate_limit_module()
    monkeypatch.setattr(rl_module.time, "time", lambda: 4_000_000)

    client = TestClient(app)

    headers = {"X-Client-Id": "vip-client"}
    assert client.get("/ping", headers=headers).status_code == 200
    assert client.get("/ping", headers=headers).status_code == 200


def test_rate_limit_combines_tenant_and_header(monkeypatch: pytest.MonkeyPatch):
    from fastapi.testclient import TestClient

    rl_cfg = DummyRateLimitConfig(
        rps=1,
        burst=0,
        overrides={},
        identifier_header="X-Client-Id",
        whitelist=(),
    )
    app = _build_app(rl_cfg)
    rl_module = _load_rate_limit_module()
    monkeypatch.setattr(rl_module.time, "time", lambda: 5_000_000)

    client = TestClient(app)

    headers_a = {"X-Client-Id": "client-1", "X-Aurum-Tenant": "tenant-a"}
    headers_b = {"X-Client-Id": "client-1", "X-Aurum-Tenant": "tenant-b"}

    assert client.get("/ping", headers=headers_a).status_code == 200
    assert client.get("/ping", headers=headers_b).status_code == 200
    assert client.get("/ping", headers=headers_a).status_code == 429
