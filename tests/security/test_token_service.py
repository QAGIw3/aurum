from __future__ import annotations

import pytest
from jose import jwt

from aurum.security.token_service import (
    InvalidRefreshTokenError,
    TokenService,
    TokenServiceConfig,
)


@pytest.fixture()
def token_service() -> TokenService:
    config = TokenServiceConfig(
        issuer="urn:aurum:test",
        audiences=("aurum-api",),
        access_token_ttl=60,
        refresh_token_ttl=300,
    )
    return TokenService(config=config)


def test_issue_token_pair(token_service: TokenService) -> None:
    pair = token_service.issue_token_pair(
        subject="client-123",
        tenant_id="tenant-1",
        scopes=["read", "write"],
        base_claims={"role": "admin"},
    )

    assert "access_token" in pair
    assert pair["tenant"] == "tenant-1"

    claims = jwt.get_unverified_claims(pair["access_token"])
    assert claims["iss"] == "urn:aurum:test"
    assert claims["tenant"] == "tenant-1"
    assert set(claims["scope"].split()) == {"read", "write"}
    assert claims["role"] == "admin"


def test_refresh_rotates_tokens(token_service: TokenService) -> None:
    pair = token_service.issue_token_pair(subject="client-456", tenant_id=None, scopes=["read"])
    refreshed = token_service.refresh(pair["refresh_token"])

    assert refreshed["session_id"] == pair["session_id"]
    assert refreshed["refresh_token"] != pair["refresh_token"]


def test_refresh_invalid_token(token_service: TokenService) -> None:
    with pytest.raises(InvalidRefreshTokenError):
        token_service.refresh("invalid-token")


def test_logout_revokes_session(token_service: TokenService) -> None:
    pair = token_service.issue_token_pair(subject="client-789", tenant_id=None)
    token_service.revoke_refresh_token(pair["refresh_token"])
    assert token_service.is_session_revoked(pair["session_id"]) is True
    with pytest.raises(InvalidRefreshTokenError):
        token_service.refresh(pair["refresh_token"])


