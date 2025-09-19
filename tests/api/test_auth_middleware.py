import pytest


def test_auth_middleware_missing_token_unauthorized():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from aurum.api.auth import AuthMiddleware, OIDCConfig

    # Build a minimal app with auth enforced and no JWKS configured
    app = FastAPI()

    @app.get("/secure")
    def secure():
        return {"ok": True}

    cfg = OIDCConfig(issuer=None, audience=None, jwks_url=None, disabled=False, leeway=60)
    app.add_middleware(AuthMiddleware, config=cfg)

    client = TestClient(app)
    resp = client.get("/secure")
    assert resp.status_code == 401


def test_auth_middleware_disabled_allows():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from aurum.api.auth import AuthMiddleware, OIDCConfig

    app = FastAPI()

    @app.get("/open")
    def open_route():
        return {"ok": True}

    cfg = OIDCConfig(issuer=None, audience=None, jwks_url=None, disabled=True, leeway=60)
    app.add_middleware(AuthMiddleware, config=cfg)

    client = TestClient(app)
    resp = client.get("/open")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

