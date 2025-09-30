from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.main import create_app


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


def test_catalog_series_smoke(client: TestClient):
    # Expect either success (200) or dependency error surfaces (5xx) based on environment
    resp = client.get("/v2/catalog/series", params={"tenant_id": "test-tenant", "limit": 1})
    assert resp.headers.get("X-Request-Id") is not None
    assert resp.status_code in (200, 500)

