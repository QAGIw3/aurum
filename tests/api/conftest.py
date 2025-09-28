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
        "AURUM_API_OIDC_ISSUER",
        "AURUM_API_OIDC_JWKS_URL",
        "AURUM_API_OIDC_AUDIENCE",
        "AURUM_API_FORWARD_AUTH_HEADER",
        "AURUM_API_FORWARD_AUTH_CLAIMS_HEADER",
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
