from __future__ import annotations

import importlib
import os

import pytest


@pytest.mark.parametrize("path,params", [
    ("/v2/admin/mappings", {"tenant_id": "t1", "limit": 1}),
    ("/v2/admin/config/versions", {}),
])
def test_admin_guard_blocks_when_enabled(path, params, monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient

    # Enable admin guard and reload the app module to pick it up
    monkeypatch.setenv("AURUM_API_ADMIN_GUARD_ENABLED", "1")
    if "aurum.api.app" in importlib.sys.modules:
        importlib.reload(importlib.import_module("aurum.api.app"))
    module = importlib.import_module("aurum.api.app")
    app = getattr(module, "app", None)
    assert app is not None

    client = TestClient(app)
    resp = client.get(path, params=params)
    assert resp.status_code == 403
