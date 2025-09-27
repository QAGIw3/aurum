import asyncio
from typing import Any, Dict

import pytest

from aurum.api.services.developer_workspace_service import (
    DeveloperWorkspaceService,
    NotebookEnvironment,
)


@pytest.fixture()
def workspace_service(tmp_path, monkeypatch):
    """

    original_sleep = asyncio.sleep

    async def fast_sleep(delay: float, *args, **kwargs):
        await original_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    service = DeveloperWorkspaceService()
    service._session_startup_delay_seconds = 0
    service._session_monitor_interval_seconds = 0.01

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

    yield service

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

    status = await workspace_service.get_session_status(session_id)
    assert status is not None
    assert status.session_id == session_id

    terminated = await workspace_service.terminate_notebook_session(
        session_id,
        reason="test",
    )
    assert terminated is True


@pytest.mark.asyncio
async def test_openapi_parsing(workspace_service: DeveloperWorkspaceService):
    docs = await workspace_service.get_api_documentation()
    assert docs["version"] == "1.0"
    assert "GET /v2/demo" in docs["endpoints"]

    snippets = await workspace_service.get_code_snippets(category="demo")
    assert len(snippets) == 1
    assert snippets[0]["name"] == "List demo"
