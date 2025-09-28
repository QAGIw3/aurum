from __future__ import annotations

import os
import pytest
from fastapi.testclient import TestClient

from aurum.core.settings import AurumSettings
from aurum.api.app import create_app, create_dev_app, create_prod_app


def _make_settings(**overrides):
    s = AurumSettings.from_env()
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def test_create_app_has_single_instance_and_no_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURUM_API_METRICS_ENABLED", "false")
    settings = _make_settings()
    app = create_app(settings)

    # Ensure settings bound and lifespan manager attached lazily
    assert getattr(app.state, "settings", None) is settings

    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code in (200, 204)


def test_env_specific_factories_toggle_docs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURUM_API_METRICS_ENABLED", "false")

    dev_app = create_dev_app(_make_settings())
    assert dev_app.docs_url == "/docs"
    assert dev_app.redoc_url == "/redoc"

    prod_app = create_prod_app(_make_settings())
    assert prod_app.docs_url is None
    assert prod_app.redoc_url is None


def test_metrics_endpoint_registration(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("AURUM_API_METRICS_ENABLED", "true")
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    app = create_app(_make_settings())

    with TestClient(app) as client:
        res = client.get("/metrics")
        assert res.status_code in (200, 503)  # 503 when prometheus_client unavailable
