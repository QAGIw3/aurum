"""Basic tests for v2 admin config endpoints using dependency stubs.

These tests validate that the /v2/admin/config endpoints are reachable and
return well-formed responses when RBAC and backing services are stubbed.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi import Depends, Request
from fastapi.testclient import TestClient
from fastapi import FastAPI


class _StubPrincipal:
    def __init__(self, tenant_id: str = "tenant-test"):
        self.tenant_id = tenant_id
        self.roles = ("admin",)
        self.permissions = {"admin:config"}
        self.subject = "user-1"

    def has_permission(self, perm: Any, _tenant: str | None = None) -> bool:
        value = getattr(perm, "value", str(perm))
        return value in self.permissions


@pytest.fixture
def client(monkeypatch):
    # Stub the RBAC dependency factory before importing the router
    def no_op_require_permissions(*_args, **_kwargs):
        async def _dep(request: Request):
            # inject a stub principal on state
            request.state.principal = _StubPrincipal()
            return request.state.principal

        return _dep

    monkeypatch.setattr("aurum.security.rbac.require_permissions", no_op_require_permissions)

    # Stub DynamicConfigService
    class _StubSnap:
        def __init__(self):
            self.version = 1
            self.timestamp = 1234567890.0
            self.content_hash = "hash-abc"

    class _StubConfigSvc:
        def __init__(self, environment: str | None = None):
            self.environment = environment or "development"

        def get(self) -> Dict[str, Any]:
            return {"api": {"title": "Aurum API"}, "environment": self.environment}

        def get_snapshot(self):
            return _StubSnap()

        def set_ephemeral_override(self, key: str, value: Dict[str, Any], ttl_seconds: int | None):
            return None

        def remove_ephemeral_override(self, key: str):
            return None

    monkeypatch.setattr("aurum.api.v2.admin_config.DynamicConfigService", _StubConfigSvc)

    # Stub change tracker
    class _Version:
        def __init__(self, v: int):
            self.version = v
            self.timestamp = 1234567890.0 + v
            self.content_hash = f"hash-{v}"
            self.change_id = f"chg-{v}"
            self.compressed_size = 100 + v
            self.metadata = {"note": f"v{v}"}

    class _Tracker:
        def list_versions(self, limit: int = 50) -> List[_Version]:
            return [_Version(1), _Version(2)][:limit]

        def get_version(self, version: int):
            if version in (1, 2):
                return _Version(version)
            return None

        async def backup_current_config(self, config: Dict[str, Any], reason: str) -> str:
            return "chg-backup"

        async def restore_version(self, version: int, actor: str, reason: str) -> str:
            return f"chg-restore-{version}"

        def get_latest_version(self):
            return _Version(2)

        def get_change_history(self, limit=100, namespace=None, actor=None, since_timestamp=None):
            return []

        def compare_versions(self, from_version: int, to_version: int) -> Dict[str, Any]:
            return {"diff": ["key"]}

    monkeypatch.setattr("aurum.api.v2.admin_config.get_change_tracker", lambda: _Tracker())

    # Now import the router after monkeypatches and mount a minimal app
    from aurum.api.v2.admin_config import router as admin_router

    app = FastAPI()
    app.include_router(admin_router)
    return TestClient(app)


def test_get_effective_config(client):
    r = client.get("/v2/admin/config/effective", params={"environment": "development"})
    assert r.status_code == 200
    body = r.json()
    assert body["version"] >= 1
    assert "config" in body and isinstance(body["config"], dict)


def test_list_config_versions(client):
    r = client.get("/v2/admin/config/versions", params={"limit": 2})
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and len(body) == 2
    assert body[0]["version"] == 1 and body[1]["version"] == 2
