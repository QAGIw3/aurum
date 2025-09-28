from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Tuple

import pytest

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("AURUM_API_V2_LIGHT_INIT", "1")

from aurum.api.services.developer_workspace_service import (
    NotebookEnvironment,
    NotebookSession,
    TenantAccessError,
)
from aurum.api.v2 import developer_workspace as dw_module


class StubTelemetry:
    def record_success(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
        return None

    def record_error(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
        return None

    def create_response_metadata(self, *args, **kwargs) -> Dict[str, Any]:
        return {"operation": kwargs.get("operation"), "duration_ms": kwargs.get("query_time_ms", 0)}


class StubWorkspaceService:
    def __init__(self) -> None:
        self._environments: Dict[Tuple[str, str], NotebookEnvironment] = {}
        self._metadata: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._sessions: Dict[str, NotebookSession] = {}
        self._session_sequence: int = 0

    async def create_notebook_environment(self, tenant_id: str, environment: NotebookEnvironment) -> str:
        key = (tenant_id, environment.environment_id)
        if key in self._environments:
            raise ValueError("environment already exists")

        env_copy = environment.model_copy(deep=True)
        timestamp = datetime.utcnow().isoformat()
        self._environments[key] = env_copy
        self._metadata[key] = {
            "environment_id": environment.environment_id,
            "tenant_id": tenant_id,
            "created_at": timestamp,
            "updated_at": timestamp,
            "version": 1,
        }
        return environment.environment_id

    async def get_notebook_environment(self, tenant_id: str, environment_id: str) -> NotebookEnvironment | None:
        env = self._environments.get((tenant_id, environment_id))
        return env.model_copy(deep=True) if env else None

    async def get_notebook_environment_metadata(self, tenant_id: str, environment_id: str) -> Dict[str, Any] | None:
        metadata = self._metadata.get((tenant_id, environment_id))
        return dict(metadata) if metadata else None

    async def list_notebook_environments(self, tenant_id: str) -> list[NotebookEnvironment]:
        return [env.model_copy(deep=True) for (tenant, _), env in self._environments.items() if tenant == tenant_id]

    async def get_service_health(self) -> Dict[str, Any]:
        active_sessions = sum(1 for session in self._sessions.values() if session.status != "stopped")
        return {
            "environments_available": len(self._environments),
            "sessions_active": active_sessions,
        }

    async def start_notebook_session(
        self,
        environment_id: str,
        user_id: str,
        tenant_id: str,
        configuration: Dict[str, Any] | None = None,
    ) -> str:
        key = (tenant_id, environment_id)
        if key not in self._environments:
            raise ValueError("Environment not found")

        self._session_sequence += 1
        session_id = f"session-{self._session_sequence}"

        now = datetime.utcnow()
        session = NotebookSession(
            session_id=session_id,
            environment_id=environment_id,
            user_id=user_id,
            tenant_id=tenant_id,
            status="running",
            start_time=now,
            last_activity=now,
            resource_usage={"cpu": "100m", "memory": "512Mi"},
            notebook_url=f"http://sandbox/{session_id}",
        )

        self._sessions[session_id] = session
        return session_id

    async def get_session_status(self, session_id: str, tenant_id: str) -> NotebookSession | None:
        session = self._sessions.get(session_id)
        if not session:
            return None
        if session.tenant_id != tenant_id:
            raise TenantAccessError(tenant_id, session.tenant_id, session_id)
        return session.model_copy(deep=True)

    async def list_user_sessions(self, user_id: str) -> list[NotebookSession]:
        return [session.model_copy(deep=True) for session in self._sessions.values() if session.user_id == user_id]

    async def terminate_notebook_session(self, session_id: str, tenant_id: str, reason: str = "user_requested") -> bool:
        session = self._sessions.get(session_id)
        if not session:
            return False
        if session.tenant_id != tenant_id:
            raise TenantAccessError(tenant_id, session.tenant_id, session_id)
        if session.status == "stopped":
            return False
        session.status = "stopped"
        session.last_activity = datetime.utcnow()
        self._sessions[session_id] = session
        return True


@pytest.fixture()
def developer_workspace_client(monkeypatch) -> Tuple[TestClient, StubWorkspaceService]:
    app = FastAPI()
    service = StubWorkspaceService()

    monkeypatch.setattr(dw_module, "get_developer_workspace_service", lambda: service)
    monkeypatch.setattr(dw_module, "get_telemetry_facade", lambda: StubTelemetry())

    app.dependency_overrides[dw_module.get_principal] = lambda: {
        "groups": ["admin"],
        "tenant": "tenant-test",
        "email": "admin@example.com",
    }

    app.include_router(dw_module.router)

    return TestClient(app), service


def test_create_environment_endpoint(developer_workspace_client) -> None:
    client, service = developer_workspace_client

    payload = {
        "environment_id": "sandbox",
        "environment_name": "Sandbox",
        "description": "Tenant sandbox environment",
    }

    response = client.post(
        "/v2/developer-workspace/environments",
        json=payload,
        headers={"X-Aurum-Tenant": "tenant-test"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["environment_id"] == "sandbox"
    assert ("tenant-test", "sandbox") in service._environments


def test_create_environment_requires_tenant(developer_workspace_client) -> None:
    client, _ = developer_workspace_client

    payload = {
        "environment_id": "missing",
        "environment_name": "Missing",
        "description": "Should fail",
    }

    response = client.post("/v2/developer-workspace/environments", json=payload)

    assert response.status_code == 400


def test_list_environments_returns_tenant_scoped_data(developer_workspace_client) -> None:
    client, _ = developer_workspace_client

    payload = {
        "environment_id": "lab",
        "environment_name": "Lab",
        "description": "Research lab environment",
    }

    create_response = client.post(
        "/v2/developer-workspace/environments",
        json=payload,
        headers={"X-Aurum-Tenant": "tenant-test"},
    )
    assert create_response.status_code == 201

    response = client.get(
        "/v2/developer-workspace/environments",
        headers={"X-Aurum-Tenant": "tenant-test"},
    )

    assert response.status_code == 200
    body = response.json()
    environments = body["data"]["environments"]
    assert any(env["environment_id"] == "lab" for env in environments)
    assert body["data"]["tenant_id"] == "tenant-test"
