import importlib

import pytest


def _reload_app(monkeypatch: pytest.MonkeyPatch, rps: int, burst: int):
    monkeypatch.setenv("AURUM_API_RATE_LIMIT_RPS", str(rps))
    monkeypatch.setenv("AURUM_API_RATE_LIMIT_BURST", str(burst))
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
    # Ensure no Redis URL to exercise in-memory path
    monkeypatch.delenv("AURUM_API_REDIS_URL", raising=False)
    # Reload module to reconstruct app with new middleware config
    import sys

    if "aurum.api.app" in sys.modules:
        importlib.reload(sys.modules["aurum.api.app"])  # type: ignore[arg-type]
    else:
        import aurum.api.app  # noqa: F401
    mod = importlib.import_module("aurum.api.app")
    return mod


def test_rate_limit_returns_429(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    import importlib
    from fastapi.testclient import TestClient

    # Freeze time so both requests fall into the same 1-second window
    monkeypatch.setattr("aurum.api.ratelimit.time.time", lambda: 1000000)
    mod = _reload_app(monkeypatch, rps=1, burst=0)
    app = getattr(mod, "app")

    client = TestClient(app)
    r1 = client.get("/v1/metadata/units")
    assert r1.status_code == 200
    r2 = client.get("/v1/metadata/units")
    assert r2.status_code == 429
    # Response should include Retry-After and rate limit headers
    assert r2.headers.get("retry-after") == "1"
    assert r2.headers.get("x-ratelimit-limit") == "1"
    assert r2.headers.get("x-ratelimit-remaining") == "0"
    # restore default app for other tests
    monkeypatch.delenv("AURUM_API_RATE_LIMIT_RPS", raising=False)
    monkeypatch.delenv("AURUM_API_RATE_LIMIT_BURST", raising=False)
    if "aurum.api.app" in __import__("sys").modules:
        importlib.reload(__import__("sys").modules["aurum.api.app"])  # type: ignore[arg-type]


def test_rate_limit_counts_per_client_ip(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient

    monkeypatch.setattr("aurum.api.ratelimit.time.time", lambda: 2000000)
    mod = _reload_app(monkeypatch, rps=1, burst=0)
    app = getattr(mod, "app")
    client = TestClient(app)

    h1 = {"x-forwarded-for": "1.1.1.1"}
    h2 = {"x-forwarded-for": "2.2.2.2"}
    r1 = client.get("/v1/metadata/units", headers=h1)
    r2 = client.get("/v1/metadata/units", headers=h2)
    assert r1.status_code == 200
    assert r2.status_code == 200
    # restore default app for other tests
    monkeypatch.delenv("AURUM_API_RATE_LIMIT_RPS", raising=False)
    monkeypatch.delenv("AURUM_API_RATE_LIMIT_BURST", raising=False)
    if "aurum.api.app" in __import__("sys").modules:
        importlib.reload(__import__("sys").modules["aurum.api.app"])  # type: ignore[arg-type]
