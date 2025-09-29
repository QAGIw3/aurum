from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from aurum.core import AurumSettings
from aurum.api.middleware.manager import MiddlewareManager


def test_default_order_and_activation():
    settings = AurumSettings.from_env()
    app = FastAPI()
    app.state.settings = settings

    manager = MiddlewareManager()
    manager.add_defaults(settings)
    order = manager.describe_order()

    # Ensure some core middleware are present in the chain
    assert "logging_context" in order
    assert "rfc7807" in order
    assert "cors" in order
    assert "gzip" in order
    assert "response_headers" in order


def test_disable_enable_via_manager():
    settings = AurumSettings.from_env()
    app = FastAPI()
    app.state.settings = settings

    manager = MiddlewareManager()
    manager.add_defaults(settings)
    manager.set_enabled("cors", False)
    manager.set_enabled("gzip", False)
    order = manager.describe_order()
    assert "cors" not in order
    assert "gzip" not in order


def test_application_runs_with_manager():
    settings = AurumSettings.from_env()
    app = FastAPI()
    app.state.settings = settings

    manager = MiddlewareManager()
    manager.add_defaults(settings)
    manager.apply(app, settings)

    @app.get("/ping")
    def ping():
        return {"ok": True}

    client = TestClient(app)
    r = client.get("/ping")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


