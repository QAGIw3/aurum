"""Authentication enforcement tests."""

from __future__ import annotations

import json

import pytest

from tests.common import TestAppConfig


@pytest.mark.unit
@pytest.mark.parametrize(
    "auth_config",
    [
        {
            "enable_auth": True,
            "admin_group": "admins",
        }
    ],
)
def test_requests_without_identity_are_rejected(
    app_settings: TestAppConfig,
    api_app,
    api_client_with_auth,
    auth_config
):
    """Test requests without identity are rejected when auth is enabled."""
    # Configure settings for auth
    for key, value in auth_config.items():
        setattr(app_settings, key, value)

    client = api_client_with_auth

    response = client.get("/v1/metadata/units")
    assert response.status_code == 401


@pytest.mark.unit
def test_requests_with_forward_auth_identity_succeed(
    app_settings: TestAppConfig,
    api_app,
    api_client_with_auth
):
    """Test requests with forward auth identity succeed."""
    app_settings.enable_auth = True
    app_settings.admin_group = "admins"

    client = api_client_with_auth

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


@pytest.mark.unit
def test_requests_succeed_when_auth_disabled(
    app_settings: TestAppConfig,
    api_app,
    api_client
):
    """Test requests succeed when auth is disabled."""
    app_settings.enable_auth = False

    client = api_client

    response = client.get("/v1/metadata/units")
    assert response.status_code in (200, 304)
