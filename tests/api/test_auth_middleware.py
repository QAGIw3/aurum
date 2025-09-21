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


def test_oidc_config_defaults_to_disabled_without_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.auth import OIDCConfig

    for key in [
        "AURUM_API_OIDC_ISSUER",
        "AURUM_API_OIDC_JWKS_URL",
        "AURUM_API_OIDC_AUDIENCE",
        "AURUM_API_AUTH_DISABLED",
    ]:
        monkeypatch.delenv(key, raising=False)

    cfg = OIDCConfig.from_env()
    assert cfg.disabled is True


def test_oidc_config_respects_explicit_disable_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.auth import OIDCConfig

    monkeypatch.setenv("AURUM_API_OIDC_ISSUER", "https://issuer")
    monkeypatch.setenv("AURUM_API_OIDC_JWKS_URL", "https://issuer/jwks")
    monkeypatch.setenv("AURUM_API_OIDC_AUDIENCE", "aurum")
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")

    cfg = OIDCConfig.from_env()
    assert cfg.disabled is True


def test_oidc_config_enables_when_all_values_present(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.auth import OIDCConfig

    monkeypatch.setenv("AURUM_API_OIDC_ISSUER", "https://issuer")
    monkeypatch.setenv("AURUM_API_OIDC_JWKS_URL", "https://issuer/jwks")
    monkeypatch.setenv("AURUM_API_OIDC_AUDIENCE", "aurum")
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "0")

    cfg = OIDCConfig.from_env()
    assert cfg.disabled is False
    assert cfg.issuer == "https://issuer"
    assert cfg.audience == "aurum"
