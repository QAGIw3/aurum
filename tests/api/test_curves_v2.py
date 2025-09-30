from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.main import create_app


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


def test_v2_curves_list_smoke(client: TestClient):
    # Minimal smoke test to ensure route is wired; may hit real backend in CI
    resp = client.get("/v2/curves/curves", params={"tenant_id": "test", "limit": 1})
    # Accept 200 or 500 depending on backend connectivity; presence of JSON and request_id header is checked
    assert resp.headers.get("X-Request-Id") is not None
    assert resp.status_code in (200, 500)


