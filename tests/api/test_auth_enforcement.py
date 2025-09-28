from __future__ import annotations

import json

import pytest


pytest.importorskip("fastapi", reason="fastapi not installed")
from fastapi.testclient import TestClient  # type: ignore  # noqa: E402


@pytest.mark.parametrize(
    "extra_env",
    [
        {
            "AURUM_API_AUTH_DISABLED": "0",
            "AURUM_API_FORWARD_AUTH_HEADER": "X-Forwarded-User",
            "AURUM_API_FORWARD_AUTH_CLAIMS_HEADER": "X-Forwarded-Claims",
            "AURUM_API_OIDC_ISSUER": "https://issuer.test",
            "AURUM_API_OIDC_JWKS_URL": "https://issuer.test/jwks",
            "AURUM_API_OIDC_AUDIENCE": "aurum-api",
            "AURUM_API_ADMIN_GROUP": "admins",
        }
    ],
)
def test_requests_without_identity_are_rejected(reload_api_app, extra_env):
    module = reload_api_app(extra_env)
    client = TestClient(module.app)

    response = client.get("/v1/metadata/units")
    assert response.status_code == 401


def test_requests_with_forward_auth_identity_succeed(reload_api_app):
    module = reload_api_app(
        {
            "AURUM_API_AUTH_DISABLED": "0",
            "AURUM_API_FORWARD_AUTH_HEADER": "X-Forwarded-User",
            "AURUM_API_FORWARD_AUTH_CLAIMS_HEADER": "X-Forwarded-Claims",
            "AURUM_API_OIDC_ISSUER": "https://issuer.test",
            "AURUM_API_OIDC_JWKS_URL": "https://issuer.test/jwks",
            "AURUM_API_OIDC_AUDIENCE": "aurum-api",
            "AURUM_API_ADMIN_GROUP": "admins",
        }
    )
    client = TestClient(module.app)

    claims = {
        "email": "user@example.com",
        "tenant": "example",
        "groups": ["admins"],
    }

    response = client.get(
        "/v1/metadata/units",
        headers={
            "X-Forwarded-User": "user@example.com",
            "X-Forwarded-Claims": json.dumps(claims),
        },
    )
    assert response.status_code in (200, 304)
