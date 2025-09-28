"""End-to-end tests for the v1 Model Registry API."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Dict, Any

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app
from aurum.core import AurumSettings


pytestmark = pytest.mark.usefixtures("enable_test_default_settings")


def _make_settings() -> AurumSettings:
    """Factory returning relaxed settings for test clients."""

    settings = AurumSettings()
    settings.api.rate_limit.enabled = False
    return settings


@pytest.fixture
def make_client() -> Callable[[Dict[str, Any]], TestClient]:
    """Return a factory producing TestClient instances with injected principals."""

    def _factory(principal: Dict[str, Any]) -> TestClient:
        app = create_app(_make_settings())

        @app.middleware("http")
        async def _inject_principal(request, call_next):  # type: ignore[override]
            request.state.principal = principal
            tenant = principal.get("tenant")
            if tenant:
                request.state.tenant = tenant
                request.state.tenant_id = tenant
            return await call_next(request)

        return TestClient(app)

    return _factory


def test_model_registry_crud_flow(make_client: Callable[[Dict[str, Any]], TestClient]) -> None:
    """Exercise CRUD, versioning, audit, and documentation endpoints."""

    admin_principal = {"groups": ["aurum-admins"], "tenant": "tenant-alpha", "email": "admin@example.com"}
    client = make_client(admin_principal)

    # Documentation endpoint
    docs_resp = client.get("/v1/model-registry/docs")
    assert docs_resp.status_code == 200
    docs = docs_resp.json()
    assert docs["version"] == "1.0"
    assert any(ep["path"] == "/v1/model-registry/models" for ep in docs["endpoints"])

    # Create model
    model_payload = {
        "model_name": "load-forecast",
        "model_type": "xgboost",
        "description": "Day-ahead load forecasting",
        "owners": ["alice"],
        "tags": {"asset": "load"},
        "metadata": {"region": "ERCOT"},
        "audit": {"requested_by": "alice"},
    }
    create_resp = client.post("/v1/model-registry/models", json=model_payload)
    assert create_resp.status_code == 201, create_resp.text
    created = create_resp.json()["data"]
    assert created["model_name"] == "load-forecast"
    assert created["status"] == "active"

    # Update metadata
    update_resp = client.patch(
        "/v1/model-registry/models/load-forecast",
        json={"description": "Updated", "tags": {"team": "ml"}, "audit": {"requested_by": "bob"}},
    )
    assert update_resp.status_code == 200, update_resp.text
    updated = update_resp.json()["data"]
    assert updated["description"] == "Updated"
    assert updated["tags"]["team"] == "ml"

    # Register version
    training_start = datetime.utcnow() - timedelta(days=7)
    training_end = datetime.utcnow()
    version_payload = {
        "description": "Baseline champion",
        "config": {
            "model_type": "xgboost",
            "hyperparameters": {"max_depth": 6},
            "feature_selection": ["temperature", "load"],
            "target_variable": "load_mw",
        },
        "training_start_date": training_start.isoformat(),
        "training_end_date": training_end.isoformat(),
        "model_size_bytes": 1024,
        "performance_metrics": {"accuracy": 0.9},
        "audit": {"requested_by": "alice"},
    }
    version_resp = client.post(
        "/v1/model-registry/models/load-forecast/versions",
        json=version_payload,
    )
    assert version_resp.status_code == 201, version_resp.text
    version = version_resp.json()
    assert version["model_name"] == "load-forecast"
    version_number = version["version_number"]

    # Duplicate version should fail with 409
    duplicate_payload = {**version_payload, "version_number": version_number}
    dup_resp = client.post(
        "/v1/model-registry/models/load-forecast/versions",
        json=duplicate_payload,
    )
    assert dup_resp.status_code == 409

    # List models
    list_resp = client.get("/v1/model-registry/models")
    assert list_resp.status_code == 200
    listed = list_resp.json()["data"]
    assert any(model["model_name"] == "load-forecast" for model in listed)

    # Fetch model detail
    detail_resp = client.get("/v1/model-registry/models/load-forecast")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()["data"]
    assert detail["latest_version"] == version_number

    # List versions
    versions_resp = client.get("/v1/model-registry/models/load-forecast/versions")
    assert versions_resp.status_code == 200
    versions = versions_resp.json()["data"]
    assert len(versions) == 1

    # Audit log should contain entries
    audit_resp = client.get("/v1/model-registry/audit")
    assert audit_resp.status_code == 200
    audit_events = audit_resp.json()["data"]
    assert any(event["action"] == "register_model_version" for event in audit_events)

    # Archive model
    archive_resp = client.delete(
        "/v1/model-registry/models/load-forecast",
        params={"reason": "superseded"},
    )
    assert archive_resp.status_code == 200
    assert archive_resp.json()["message"] == "model archived"

    archived_resp = client.get("/v1/model-registry/models/load-forecast")
    assert archived_resp.status_code == 200
    assert archived_resp.json()["data"]["status"] == "archived"


def test_model_registry_rbac_enforced(make_client: Callable[[Dict[str, Any]], TestClient]) -> None:
    """Ensure write actions require elevated permissions while reads remain accessible."""

    analyst_principal = {"groups": ["analyst"], "tenant": "tenant-alpha", "email": "analyst@example.com"}
    client = make_client(analyst_principal)

    # Read endpoints allowed
    docs_resp = client.get("/v1/model-registry/docs")
    assert docs_resp.status_code == 200

    # Write endpoint should be forbidden
    model_payload = {
        "model_name": "rbac-test",
        "model_type": "xgboost",
        "description": "RBAC test",
    }
    create_resp = client.post("/v1/model-registry/models", json=model_payload)
    assert create_resp.status_code == 403

    # Attempt version creation explicitly verifies permission denial
    now = datetime.utcnow()
    version_payload = {
        "description": "rbac",
        "config": {
            "model_type": "xgboost",
            "hyperparameters": {"n_estimators": 10},
            "feature_selection": ["load"],
            "target_variable": "load_mw",
        },
        "training_start_date": (now - timedelta(days=1)).isoformat(),
        "training_end_date": now.isoformat(),
        "model_size_bytes": 1,
    }
    version_resp = client.post("/v1/model-registry/models/rbac-test/versions", json=version_payload)
    assert version_resp.status_code == 403
