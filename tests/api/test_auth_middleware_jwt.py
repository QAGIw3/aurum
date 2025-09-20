from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest


def _rsa_keypair():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    numbers = public_key.public_numbers()
    n_int = numbers.n
    e_int = numbers.e
    # base64url without padding
    import base64

    def b64url(x: int) -> str:
        raw = x.to_bytes((x.bit_length() + 7) // 8, "big")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    jwk = {
        "kty": "RSA",
        "kid": "kid1",
        "alg": "RS256",
        "n": b64url(n_int),
        "e": b64url(e_int),
    }
    return private_pem, jwk


def _reload_main_app_with_auth(monkeypatch: pytest.MonkeyPatch, issuer: str, audience: str, jwks_url: str):
    import importlib, sys
    # Enable auth and set OIDC env
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "0")
    monkeypatch.setenv("AURUM_API_OIDC_ISSUER", issuer)
    monkeypatch.setenv("AURUM_API_OIDC_AUDIENCE", audience)
    monkeypatch.setenv("AURUM_API_OIDC_JWKS_URL", jwks_url)
    # Generous rate limit to avoid interference
    monkeypatch.setenv("AURUM_API_RATE_LIMIT_RPS", "1000")
    monkeypatch.setenv("AURUM_API_RATE_LIMIT_BURST", "1000")
    # Reload app module to apply middleware with new config
    if "aurum.api.app" in sys.modules:
        importlib.reload(sys.modules["aurum.api.app"])  # type: ignore[arg-type]
    else:
        import aurum.api.app  # noqa: F401
    return importlib.import_module("aurum.api.app")


def test_auth_middleware_valid_jwt_allows_and_sets_principal(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from jose import jwt

    priv, jwk = _rsa_keypair()

    # Monkeypatch JWKSCache.get_key to return our key
    def fake_get_key(self, kid):  # type: ignore[no-redef]
        return jwk if kid == jwk["kid"] else None

    monkeypatch.setattr("aurum.api.auth.JWKSCache.get_key", fake_get_key)

    issuer = "https://issuer.test"
    audience = "aurum"
    mod = _reload_main_app_with_auth(monkeypatch, issuer, audience, jwks_url="https://jwks.test/keys")
    client = TestClient(mod.app)

    claims = {
        "sub": "user123",
        "iss": issuer,
        "aud": audience,
        "exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
    }
    token = jwt.encode(claims, priv, algorithm="RS256", headers={"kid": jwk["kid"]})

    r = client.get("/v1/metadata/units", headers={"authorization": f"Bearer {token}"})
    assert r.status_code == 200
    # Restore default app state (auth disabled)
    for key in [
        "AURUM_API_AUTH_DISABLED",
        "AURUM_API_OIDC_ISSUER",
        "AURUM_API_OIDC_AUDIENCE",
        "AURUM_API_OIDC_JWKS_URL",
        "AURUM_API_RATE_LIMIT_RPS",
        "AURUM_API_RATE_LIMIT_BURST",
    ]:
        monkeypatch.delenv(key, raising=False)
    import importlib, sys
    if "aurum.api.app" in sys.modules:
        importlib.reload(sys.modules["aurum.api.app"])  # type: ignore[arg-type]


def test_auth_middleware_unknown_kid_rejected(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from jose import jwt

    priv, jwk = _rsa_keypair()

    def fake_get_key(self, kid):  # type: ignore[no-redef]
        return jwk if kid == jwk["kid"] else None

    monkeypatch.setattr("aurum.api.auth.JWKSCache.get_key", fake_get_key)

    issuer = "https://issuer.test"
    audience = "aurum"
    mod = _reload_main_app_with_auth(monkeypatch, issuer, audience, jwks_url="https://jwks.test/keys")
    client = TestClient(mod.app)

    claims = {
        "sub": "user123",
        "iss": issuer,
        "aud": audience,
        "exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
    }
    # mismatched kid
    token = jwt.encode(claims, priv, algorithm="RS256", headers={"kid": "unknown-kid"})

    r = client.get("/v1/metadata/units", headers={"authorization": f"Bearer {token}"})
    assert r.status_code == 401
    # Restore default app state (auth disabled)
    for key in [
        "AURUM_API_AUTH_DISABLED",
        "AURUM_API_OIDC_ISSUER",
        "AURUM_API_OIDC_AUDIENCE",
        "AURUM_API_OIDC_JWKS_URL",
        "AURUM_API_RATE_LIMIT_RPS",
        "AURUM_API_RATE_LIMIT_BURST",
    ]:
        monkeypatch.delenv(key, raising=False)
    import importlib, sys
    if "aurum.api.app" in sys.modules:
        importlib.reload(sys.modules["aurum.api.app"])  # type: ignore[arg-type]
    payload = r.json()
    assert payload.get("error") == "unauthorized"
