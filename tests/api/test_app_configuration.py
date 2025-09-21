from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Dict

import pytest


@pytest.fixture()
def reload_api_app(monkeypatch: pytest.MonkeyPatch):
    """Reload `aurum.api.app` with a clean environment for each invocation."""

    keys = [
        "AURUM_API_ADMIN_GROUP",
        "AURUM_API_AUTH_DISABLED",
        "AURUM_API_CORS_ORIGINS",
        "AURUM_API_GZIP_MIN_BYTES",
        "AURUM_API_INMEMORY_TTL",
    ]

    def _reload(env: Dict[str, str] | None = None) -> ModuleType:
        for key in keys:
            monkeypatch.delenv(key, raising=False)
        if env:
            for key, value in env.items():
                monkeypatch.setenv(key, value)
        if "aurum.api.app" in sys.modules:
            return importlib.reload(sys.modules["aurum.api.app"])
        return importlib.import_module("aurum.api.app")

    yield _reload

    for key in keys:
        monkeypatch.delenv(key, raising=False)
    if "aurum.api.app" in sys.modules:
        importlib.reload(sys.modules["aurum.api.app"])


def test_admin_groups_from_env_lowercase_and_trim(reload_api_app) -> None:
    module = reload_api_app({"AURUM_API_ADMIN_GROUP": "TeamA, TeamB  "})
    assert module.ADMIN_GROUPS == {"teama", "teamb"}


def test_admin_groups_cleared_when_auth_disabled(reload_api_app) -> None:
    module = reload_api_app(
        {
            "AURUM_API_ADMIN_GROUP": "TeamA",
            "AURUM_API_AUTH_DISABLED": "1",
        }
    )
    assert module.ADMIN_GROUPS == set()


def test_is_admin_respects_membership_and_empty_guard(reload_api_app) -> None:
    module = reload_api_app({"AURUM_API_ADMIN_GROUP": "TeamA"})
    assert module._is_admin({"groups": ["TeamA"]}) is True
    assert module._is_admin({"groups": ["Other"]}) is False
    module.ADMIN_GROUPS = set()
    assert module._is_admin({}) is True


def test_cors_and_gzip_configuration_are_env_driven(reload_api_app) -> None:
    module = reload_api_app(
        {
            "AURUM_API_CORS_ORIGINS": "https://a.example.com, https://b.example.com",
            "AURUM_API_GZIP_MIN_BYTES": "2048",
        }
    )

    cors_middleware = next(
        mw for mw in module.app.user_middleware if mw.cls.__name__ == "CORSMiddleware"
    )
    allow_origins = cors_middleware.kwargs.get("allow_origins", [])
    assert allow_origins == [
        "https://a.example.com",
        "https://b.example.com",
    ]

    gzip_middleware = next(
        mw for mw in module.app.user_middleware if mw.cls.__name__ == "GZipMiddleware"
    )
    assert gzip_middleware.kwargs.get("minimum_size") == 2048


def test_etag_cache_returns_304_on_match(reload_api_app) -> None:
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient

    module = reload_api_app({})
    client = TestClient(module.app)

    first = client.get("/v1/metadata/units")
    assert first.status_code == 200
    etag = first.headers.get("ETag")
    assert etag
    assert "Cache-Control" in first.headers
    payload_first = first.json()

    cached = client.get("/v1/metadata/units", headers={"If-None-Match": etag})
    assert cached.status_code in (200, 304)
    if cached.status_code == 200:
        payload_cached = cached.json()
        assert payload_cached["data"] == payload_first["data"]
