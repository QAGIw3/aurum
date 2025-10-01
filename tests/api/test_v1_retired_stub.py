"""Smoke test for the v1 retirement 410 stub.

This test validates that the catch-all v1 router returns a 410 Gone Problem
Detail payload and includes the expected deprecation headers.
"""

from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_v1_retired_stub_returns_410_with_headers(monkeypatch):
    # Ensure stable header values for the test
    monkeypatch.setenv("AURUM_API_V1_SUNSET", "Thu, 30 Oct 2025 23:59:59 GMT")
    monkeypatch.setenv("AURUM_API_V1_MIGRATION_GUIDE", "https://docs.aurum.dev/api/migration-v1-to-v2")

    # Import the retired router and mount it on a minimal FastAPI app
    from aurum.api.v1_retired import router as retired_router

    app = FastAPI()
    app.include_router(retired_router)
    client = TestClient(app)

    resp = client.get("/v1/any/legacy/endpoint")

    assert resp.status_code == 410
    body = resp.json()

    # Verify Problem Details shape
    assert body["type"] == "https://docs.aurum.dev/problems/api-version-retired"
    assert body["title"].lower().startswith("api version retired")
    assert body["status"] == 410
    assert "detail" in body and "migrate to /v2" in body["detail"].lower()

    # Verify deprecation headers
    assert resp.headers.get("Deprecation") == "true"
    assert resp.headers.get("Sunset") == os.getenv("AURUM_API_V1_SUNSET")
    assert resp.headers.get("X-API-Version") == "v1"
    assert resp.headers.get("X-API-Lifecycle") == "retired"
    assert resp.headers.get("X-API-Migration-Guide") == os.getenv("AURUM_API_V1_MIGRATION_GUIDE")

