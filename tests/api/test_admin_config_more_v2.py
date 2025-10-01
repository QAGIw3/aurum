"""Additional tests for v2 admin config endpoints (diff, backup, restore)."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _bypass_rbac(monkeypatch):
    def no_op_require_permissions(*_args, **_kwargs):
        async def _dep(request):
            request.state.principal = {"roles": ["admin"], "permissions": ["admin", "admin:config"]}
            return request.state.principal

        return _dep

    monkeypatch.setattr("aurum.security.rbac.require_permissions", no_op_require_permissions)


@pytest.fixture
def client(monkeypatch):
    _bypass_rbac(monkeypatch)

    # Stubs for config service and tracker
    class _Version:
        def __init__(self, v: int):
            self.version = v
            self.timestamp = 1000.0 + v
            self.content_hash = f"hash-{v}"
            self.change_id = f"chg-{v}"
            self.compressed_size = 100 + v
            self.metadata = {"note": f"v{v}"}
            self.config = {"k": v}

    class _Tracker:
        def list_versions(self, limit: int = 50) -> List[_Version]:
            return [_Version(1), _Version(2)][:limit]

        def get_version(self, version: int):
            return _Version(version) if version in (1, 2) else None

        def compare_versions(self, from_version: int, to_version: int) -> Dict[str, Any]:
            return {"changes": ["k"]}

        async def backup_current_config(self, config: Dict[str, Any], reason: str) -> str:
            return "chg-backup"

        async def restore_version(self, version: int, actor: str, reason: str) -> str:
            return f"chg-restore-{version}"

        def get_latest_version(self):
            return _Version(2)

        def get_change_history(self, **kwargs):
            class _EnumVal:
                def __init__(self, v: str):
                    self.value = v

            class _Change:
                def __init__(self, i: int):
                    self.change_id = f"c{i}"
                    self.timestamp = 1000.0 + i
                    self.change_type = _EnumVal("update")
                    self.source = _EnumVal("admin_api")
                    self.actor = "tester"
                    self.namespace = None
                    self.reason = "test"
                    self.correlation_id = None
                    self.metadata = {}

            return [_Change(1), _Change(2)]

    class _Svc:
        def __init__(self, environment: str | None = None):
            self.environment = environment or "development"

        def get(self) -> Dict[str, Any]:
            return {"api": {"title": "Aurum API"}}

        def get_snapshot(self):
            class S:
                version = 2
                timestamp = 2000.0
                content_hash = "hash-2"

            return S()

        def set_ephemeral_override(self, key: str, value: Dict[str, Any], ttl_seconds: int | None):
            return None

        def remove_ephemeral_override(self, key: str):
            return None

    monkeypatch.setattr("aurum.api.v2.admin_config.DynamicConfigService", _Svc)
    monkeypatch.setattr("aurum.api.v2.admin_config.get_change_tracker", lambda: _Tracker())

    from aurum.api.v2.admin_config import router as admin_config_router
    app = FastAPI()
    app.include_router(admin_config_router)
    return TestClient(app)


def test_config_diff(client):
    r = client.get("/v2/admin/config/diff", params={"from_version": 1, "to_version": 2})
    assert r.status_code == 200
    body = r.json()
    assert body["from_version"] == 1 and body["to_version"] == 2
    assert "diff" in body


def test_config_backup_and_restore(client):
    b = client.post("/v2/admin/config/backup", params={"reason": "test"})
    assert b.status_code == 200
    bp = b.json()
    assert bp["message"].startswith("Configuration backed up")

    r = client.post("/v2/admin/config/restore", params={"version": 1, "reason": "test"})
    assert r.status_code == 200
    rp = r.json()
    assert rp["restored_version"] == 1


def test_list_config_changes(client):
    r = client.get("/v2/admin/config/changes", params={"limit": 2})
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and len(body) == 2
    assert all("change_id" in c for c in body)


def test_get_specific_config_version(client):
    r = client.get("/v2/admin/config/versions/2")
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 2


def test_set_and_remove_ephemeral_override(client, monkeypatch):
    # Remove and set calls are side-effect stubs; validate 200s
    s = client.post(
        "/v2/admin/config/overrides",
        params={"key": "feature_x", "ttl_seconds": 60},
        json={"enabled": True},
    )
    assert s.status_code == 200
    d = client.delete("/v2/admin/config/overrides/feature_x")
    assert d.status_code == 200


def test_export_schemas(client, monkeypatch, tmp_path):
    # Monkeypatch export_all_schemas to write a test file into provided directory
    def fake_export(outdir: str):
        import json, os
        with open(os.path.join(outdir, "schema.sample.json"), "w") as f:
            json.dump({"title": "sample"}, f)

    monkeypatch.setattr("aurum.api.v2.admin_config.export_all_schemas", fake_export)

    r = client.get("/v2/admin/config/schemas", params={"output_format": "json"})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] >= 1
    assert any(name.endswith(".json") for name in body["schemas"].keys())
