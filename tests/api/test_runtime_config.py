from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

import aurum.api.runtime_config as runtime_config
import aurum.api.app as app_module
from aurum.core.settings import SimplifiedSettings


class StubScenarioStore:
    """Minimal in-memory scenario store to back runtime config tests."""

    def __init__(self) -> None:
        self._concurrency: dict[str, dict[str, object]] = {}
        self._rate_limits: list[dict[str, object]] = []

    async def get_concurrency_override(self, tenant_id: str):
        return self._concurrency.get(tenant_id)

    async def set_concurrency_override(self, tenant_id: str, configuration: dict[str, object], *, enabled: bool = True):
        record = {
            "tenant_id": tenant_id,
            "configuration": configuration,
            "enabled": enabled,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        self._concurrency[tenant_id] = record
        return record

    async def list_concurrency_overrides(self):  # pragma: no cover - unused but keeps parity with real store
        return list(self._concurrency.values())

    async def get_rate_limit_override(self, path_prefix: str, tenant_id: str):  # pragma: no cover - not used
        return None

    async def set_rate_limit_override(
        self,
        path_prefix: str,
        requests_per_second: int,
        burst_capacity: int,
        daily_cap: int,
        enabled: bool,
        tenant_id: str,
    ):
        record = {
            "tenant_id": tenant_id,
            "path_prefix": path_prefix,
            "requests_per_second": requests_per_second,
            "burst_capacity": burst_capacity,
            "daily_cap": daily_cap,
            "enabled": enabled,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        self._rate_limits = [r for r in self._rate_limits if not (r["tenant_id"] == tenant_id and r["path_prefix"] == path_prefix)]
        self._rate_limits.append(record)
        return record

    async def list_rate_limit_overrides(self, tenant_id: str):
        return [r for r in self._rate_limits if r["tenant_id"] == tenant_id]


@pytest.fixture(autouse=True)
def _reset_audit_log():
    runtime_config._runtime_config_service._audit_log.clear()
    yield


@pytest.fixture
def scenario_store(monkeypatch: pytest.MonkeyPatch) -> StubScenarioStore:
    store = StubScenarioStore()
    monkeypatch.setattr("aurum.scenarios.storage.get_scenario_store", lambda: store, raising=False)
    monkeypatch.setattr("aurum.telemetry.context.set_tenant_id", lambda tenant: tenant, raising=False)
    monkeypatch.setattr("aurum.telemetry.context.reset_tenant_id", lambda token: None, raising=False)
    return store


def _settings_stub() -> SimpleNamespace:
    rate_limit_ns = SimpleNamespace(tenant_overrides={}, daily_cap=100000)
    concurrency_ns = SimpleNamespace(tenant_overrides={})
    api = SimpleNamespace(rate_limit=rate_limit_ns, concurrency=concurrency_ns)
    return SimpleNamespace(api=api)


def test_concurrency_override_update_and_fetch(monkeypatch: pytest.MonkeyPatch, scenario_store: StubScenarioStore) -> None:
    metrics_mock = AsyncMock()
    monkeypatch.setattr(runtime_config, "increment_runtime_override_updates", metrics_mock, raising=False)
    monkeypatch.setattr(runtime_config, "get_user_id", lambda: "user-123", raising=False)
    monkeypatch.setattr(runtime_config, "get_request_id", lambda: "req-123", raising=False)
    monkeypatch.setattr(runtime_config, "AurumSettings", SimpleNamespace(from_env=lambda: _settings_stub()), raising=False)

    app = FastAPI()
    app.include_router(runtime_config.router)
    app.dependency_overrides[runtime_config._get_principal] = lambda: {"groups": ["admin"]}
    app.dependency_overrides[runtime_config._require_admin] = lambda principal: None

    client = TestClient(app)

    response = client.put(
        "/v1/admin/config/concurrency/tenant-one",
        json={
            "max_requests_per_tenant": 4,
            "tenant_queue_limit": 12,
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["tenant_id"] == "tenant-one"
    assert payload["configuration"] == {
        "max_requests_per_tenant": 4,
        "tenant_queue_limit": 12,
    }
    assert payload["enabled"] is True

    metrics_mock.assert_any_await("concurrency", result="success")

    fetch = client.get("/v1/admin/config/concurrency/tenant-one")
    assert fetch.status_code == 200
    body = fetch.json()
    assert body["data"]["configuration"] == {
        "max_requests_per_tenant": 4,
        "tenant_queue_limit": 12,
    }
    assert body["data"]["enabled"] is True
    assert body["data"]["source"] == "database"


def test_runtime_config_router_is_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURUM_API_LIGHT_INIT", "1")
    monkeypatch.setattr(app_module, "configure_telemetry", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(app_module, "_register_metrics_endpoint", lambda app, settings: None, raising=False)
    monkeypatch.setattr(app_module, "_register_trino_lifecycle", lambda app: None, raising=False)
    monkeypatch.setattr(app_module, "HybridTrinoClientManager", type("_Stub", (), {"get_instance": classmethod(lambda cls: None)}))

    settings = SimplifiedSettings()
    app = app_module._create_simplified_app(settings, logger=app_module.logging.getLogger("test-config"))

    paths = {route.path for route in app.router.routes}
    assert "/v1/admin/config/concurrency/{tenant_id}" in paths
    assert "/v1/admin/config/ratelimit/{tenant_id}" in paths
