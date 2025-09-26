from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app
from aurum.core.settings import AurumSettings


@pytest.fixture
def metrics_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("AURUM_API_METRICS_ENABLED", "true")
    settings = AurumSettings.from_env()
    app = create_app(settings)
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()


def test_metrics_endpoint_returns_prometheus_text(metrics_client: TestClient) -> None:
    response = metrics_client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    assert "aurum_api_concurrency_requests_total" in body
    assert "aurum_api_active_requests" in body


def test_metrics_endpoint_supports_multiprocess(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("AURUM_API_METRICS_ENABLED", "true")
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    settings = AurumSettings.from_env()
    app = create_app(settings)
    with TestClient(app) as client:
        response = client.get("/metrics")
        assert response.status_code == 200
        assert response.text.strip() != ""
        assert response.headers["content-type"].startswith("text/plain")
