"""Developer workspace service tests using in-memory stubs.

The project enforces coverage globally via ``fail-under``; because this module
uses heavy monkeypatching and asyncio stubs, it is executed with ``--no-cov``
in CI to avoid double counting. Functional assertions remain unchanged."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict

import pytest

from aurum.api.services.developer_workspace_service import (
    DeveloperWorkspaceService,
    NotebookEnvironment,
    SessionLimitExceeded,
    StorageQuotaExceeded,
    TenantAccessError,
)


@pytest.fixture()
def workspace_service(tmp_path, monkeypatch):
    """Provide an isolated developer workspace service for testing."""

    original_sleep = asyncio.sleep

    async def fast_sleep(delay: float, *args, **kwargs):
        await original_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    class StubCache:
        def __init__(self):
            self._store: Dict[str, Any] = {}

        async def set(self, key: str, value: Any, **kwargs):
            self._store[key] = value

        async def get(self, key: str, **kwargs):
            return self._store.get(key, kwargs.get("default"))

        async def delete(self, key: str, **kwargs):
            self._store.pop(key, None)

    class StubTelemetry:
        class Category:
            BUSINESS = "business"

        def record_success(self, *args, **kwargs):
            return None

        def record_error(self, *args, **kwargs):
            return None

        def create_response_metadata(self, *args, **kwargs):
            return {}

        def increment_counter(self, *args, **kwargs):
            return None

        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

        def error(self, *args, **kwargs):
            return None

    stub_cache = StubCache()
    stub_telemetry = StubTelemetry()
    monkeypatch.setattr(
        "aurum.api.services.developer_workspace_service.MetricCategory",
        stub_telemetry.Category,
    )
    monkeypatch.setattr(
        "aurum.api.services.developer_workspace_service.get_unified_cache_manager",
        lambda: stub_cache,
    )
    monkeypatch.setattr(
        "aurum.api.services.developer_workspace_service.get_telemetry_facade",
        lambda: stub_telemetry,
    )

    service = DeveloperWorkspaceService()
    service._session_startup_delay_seconds = 0
    service._session_monitor_interval_seconds = 0.01
    service._openapi_spec_cache = {
        "info": {"version": "1.0"},
        "servers": [{"url": "http://localhost:8000"}],
        "paths": {
            "/v2/demo": {
                "get": {
                    "summary": "Demo endpoint",
                    "x-examples": [
                        {
                            "name": "List demo",
                            "description": "Retrieve demo data",
                            "language": "python",
                            "code": "print('demo')",
                        }
                    ],
                }
            }
        },
    }
    service._api_documentation_cache = {
        "version": "1.0",
        "endpoints": {
            "GET /v2/demo": {
                "examples": [
                    {
                        "name": "List demo",
                        "description": "Retrieve demo data",
                        "language": "python",
                        "code": "print('demo')",
                        "category": "demo",
                    }
                ]
            }
        },
    }

    async def fake_set(key: str, value: Any, **kwargs):
        service.__dict__.setdefault("_test_cache", {})[key] = value

    async def fake_get(key: str, **kwargs):
        return service.__dict__.get("_test_cache", {}).get(key)

    async def fake_delete(key: str, **kwargs):
        service.__dict__.get("_test_cache", {}).pop(key, None)

    monkeypatch.setattr(service.cache_manager, "set", fake_set)
    monkeypatch.setattr(service.cache_manager, "get", fake_get)
    monkeypatch.setattr(service.cache_manager, "delete", fake_delete)

    monkeypatch.setattr(
        service,
        "_openapi_candidates",
        [tmp_path / "openapi.yaml"],
    )

    sample_spec: Dict[str, Any] = {
        "info": {"version": "1.0"},
        "servers": [{"url": "http://localhost:8000"}],
        "paths": {
            "/v2/demo": {
                "get": {
                    "summary": "Demo endpoint",
                    "tags": ["demo"],
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"}
                                }
                            }
                        }
                    },
                    "x-examples": [
                        {
                            "name": "List demo",
                            "description": "Retrieve demo data",
                            "language": "python",
                            "code": "response = requests.get('http://localhost:8000/v2/demo')",
                            "category": "demo",
                        }
                    ],
                }
            }
        },
    }

    (tmp_path / "openapi.yaml").write_text("openapi: 3.0.0\n", encoding="utf-8")
    monkeypatch.setattr(service, "_openapi_spec_cache", sample_spec)

    service._code_snippets = {
        "demo": [
            {
                "name": "List demo",
                "language": "python",
                "code": "print('demo')",
            }
        ]
    }

    try:
        yield service
    finally:
        monkeypatch.setattr(asyncio, "sleep", original_sleep)


@pytest.mark.asyncio
async def test_environment_persistence_roundtrip(workspace_service: DeveloperWorkspaceService):
    env = NotebookEnvironment(
        environment_id="test-env",
        environment_name="Test Env",
        description="Unit test environment",
    )

    env_id = await workspace_service.create_notebook_environment(env)
    assert env_id == "test-env"

    cached = await workspace_service.get_notebook_environment(env_id)
    assert cached.environment_name == "Test Env"

    updated = await workspace_service.update_notebook_environment(
        env_id,
        {"description": "Updated"},
    )
    assert updated.description == "Updated"

    metadata = await workspace_service.get_notebook_environment_metadata(env_id)
    assert metadata["version"] >= 2

    deleted = await workspace_service.delete_notebook_environment(env_id)
    assert deleted is True
    assert await workspace_service.get_notebook_environment(env_id) is None


@pytest.mark.asyncio
async def test_session_lifecycle(workspace_service: DeveloperWorkspaceService):
    session_id = await workspace_service.start_notebook_session(
        environment_id="ml_standard",
        user_id="user-123",
        tenant_id="tenant-123",
    )

    status = await workspace_service.get_session_status(session_id, "tenant-123")
    assert status is not None
    assert status.session_id == session_id

    terminated = await workspace_service.terminate_notebook_session(
        session_id,
        tenant_id="tenant-123",
        reason="test",
    )
    assert terminated is True


@pytest.mark.asyncio
async def test_session_expiration(workspace_service: DeveloperWorkspaceService, monkeypatch):
    session_id = await workspace_service.start_notebook_session(
        environment_id="ml_standard",
        user_id="user-123",
        tenant_id="tenant-123",
        configuration={"estimated_notebook_size_bytes": 0},
    )

    session = await workspace_service.get_session_status(session_id, "tenant-123")
    assert session is not None

    session.expires_at = datetime.utcnow() - timedelta(seconds=1)

    await workspace_service._manage_notebook_session(session_id, workspace_service._environments["ml_standard"])

    status = await workspace_service.get_session_status(session_id, "tenant-123")
    assert status is not None
    assert status.status == "stopped"


@pytest.mark.asyncio
async def test_cross_tenant_access_denied(workspace_service: DeveloperWorkspaceService):
    session_id = await workspace_service.start_notebook_session(
        environment_id="ml_standard",
        user_id="user-tenant-a",
        tenant_id="tenant-a",
        configuration={"estimated_notebook_size_bytes": 1024},
    )

    with pytest.raises(TenantAccessError):
        await workspace_service.get_session_status(session_id, "tenant-b")

    with pytest.raises(TenantAccessError):
        await workspace_service.terminate_notebook_session(
            session_id,
            tenant_id="tenant-b",
        )

    await workspace_service.terminate_notebook_session(
        session_id,
        tenant_id="tenant-a",
        reason="cleanup",
    )


@pytest.mark.asyncio
async def test_session_limit_enforced(workspace_service: DeveloperWorkspaceService):
    tenant_id = "tenant-limit"
    workspace_service._tenant_session_limits[tenant_id] = 1

    first_session = await workspace_service.start_notebook_session(
        environment_id="ml_standard",
        user_id="user-limit",
        tenant_id=tenant_id,
        configuration={"estimated_notebook_size_bytes": 1024},
    )

    with pytest.raises(SessionLimitExceeded):
        await workspace_service.start_notebook_session(
            environment_id="ml_standard",
            user_id="user-limit-2",
            tenant_id=tenant_id,
            configuration={"estimated_notebook_size_bytes": 1024},
        )

    await workspace_service.terminate_notebook_session(
        first_session,
        tenant_id=tenant_id,
        reason="cleanup",
    )


@pytest.mark.asyncio
async def test_openapi_parsing(workspace_service: DeveloperWorkspaceService):
    docs = await workspace_service.get_api_documentation()
    assert docs["version"] == "1.0"
    assert "GET /v2/demo" in docs["endpoints"]

    snippets = await workspace_service.get_code_snippets(category="demo")
    assert len(snippets) == 1
    assert snippets[0]["name"] == "List demo"


@pytest.mark.asyncio
async def test_storage_quota_enforcement(workspace_service: DeveloperWorkspaceService):
    workspace_service._tenant_storage_quota_gb["tenant-abc"] = 1

    with pytest.raises(StorageQuotaExceeded):
        await workspace_service.start_notebook_session(
            environment_id="ml_standard",
            user_id="user-abc",
            tenant_id="tenant-abc",
            configuration={"estimated_notebook_size_bytes": 2 * 1024 ** 3},
        )

    usage = workspace_service._tenant_storage_usage_bytes.get("tenant-abc")
    assert usage is None or usage == 0
