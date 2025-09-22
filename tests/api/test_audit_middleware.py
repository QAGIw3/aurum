from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aurum.api.external_audit_logging import audit_middleware


def create_app() -> FastAPI:
    app = FastAPI()

    # Register audit middleware directly for this isolated test
    app.middleware("http")(audit_middleware)

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    return app


def test_correlation_id_auto_injected_when_missing():
    app = create_app()
    client = TestClient(app)

    resp = client.get("/ping")
    assert resp.status_code == 200
    corr = resp.headers.get("x-correlation-id")
    assert corr
    assert isinstance(corr, str) and len(corr) > 0


def test_correlation_id_preserved_when_provided():
    app = create_app()
    client = TestClient(app)

    provided = "test-corr-id-123"
    resp = client.get("/ping", headers={"x-correlation-id": provided})
    assert resp.status_code == 200
    assert resp.headers.get("x-correlation-id") == provided

