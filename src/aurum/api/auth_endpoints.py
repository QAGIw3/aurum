"""Internal authentication endpoints backed by the token service."""

from __future__ import annotations

import secrets
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from aurum.core import AurumSettings
from aurum.security.audit import security_audit
from aurum.security.token_service import (
    InvalidRefreshTokenError,
    TokenService,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


class ClientCredentialsRequest(BaseModel):
    client_id: str = Field(..., description="Registered client identifier")
    client_secret: str = Field(..., description="Client secret")
    scope: Optional[str] = Field(None, description="Requested scopes, space delimited")
    tenant_id: Optional[str] = Field(None, description="Target tenant override")


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=32, description="Refresh token issued by the API")


class LogoutRequest(BaseModel):
    refresh_token: Optional[str] = Field(None, description="Refresh token to revoke")
    session_id: Optional[str] = Field(None, description="Session identifier to revoke")


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str]
    token_type: str = "Bearer"
    expires_in: int
    refresh_expires_in: int
    scope: str
    session_id: str
    tenant: Optional[str] = None


def _get_settings(request: Request) -> AurumSettings:
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="settings_unavailable")
    return settings


def _get_token_service(request: Request) -> TokenService:
    token_service = getattr(request.app.state, "token_service", None)
    if token_service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="token_service_unavailable")
    return token_service


def _ensure_token_issuer_enabled(settings: AurumSettings) -> None:
    if not getattr(settings.auth, "token_issuer_enabled", False):
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="token_issuer_disabled")


def _resolve_client(
    settings: AurumSettings,
    client_id: str,
    client_secret: str,
    request: Request,
) -> Dict[str, Any]:
    registry = getattr(settings.auth, "clients", {}) or {}
    client = registry.get(client_id)
    if not client:
        _log_auth_failure(client_id, settings, request, reason="unknown_client")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid_client")
    stored_secret = str(client.get("client_secret", ""))
    if not stored_secret or not secrets.compare_digest(stored_secret, client_secret):
        _log_auth_failure(client_id, settings, request, reason="invalid_secret")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid_client")
    return client


def _parse_scopes(request_value: Optional[str], client_default: Dict[str, Any]) -> list[str]:
    requested = []
    if request_value:
        requested = [token.strip() for token in request_value.split() if token.strip()]
    allowed = list(client_default.get("scopes", ()))
    if not allowed:
        return requested or []
    if not requested:
        return allowed
    intersection = [scope for scope in requested if scope in allowed]
    return intersection


def _maybe_set_access_cookie(settings: AurumSettings, response: Response, token_payload: Dict[str, Any]) -> None:
    cookie_cfg = getattr(settings.auth, "cookie", None)
    if not cookie_cfg or not getattr(cookie_cfg, "enabled", False):
        return
    max_age = getattr(settings.auth, "access_token_ttl_seconds", 0)
    value = token_payload["access_token"]
    same_site = str(getattr(cookie_cfg, "same_site", "lax") or "lax").capitalize()
    response.set_cookie(
        key=cookie_cfg.name,
        value=value,
        max_age=max_age or None,
        secure=getattr(cookie_cfg, "secure", True),
        httponly=getattr(cookie_cfg, "http_only", True),
        samesite=same_site,
        domain=getattr(cookie_cfg, "domain", None),
        path=getattr(cookie_cfg, "path", "/"),
    )


@router.post("/token", response_model=TokenResponse, status_code=status.HTTP_200_OK)
def create_token(
    payload: ClientCredentialsRequest,
    response: Response,
    request: Request,
    settings: AurumSettings = Depends(_get_settings),
    token_service: TokenService = Depends(_get_token_service),
) -> TokenResponse:
    _ensure_token_issuer_enabled(settings)
    client = _resolve_client(settings, payload.client_id, payload.client_secret, request)
    scopes = _parse_scopes(payload.scope, client)
    tenant = payload.tenant_id or client.get("tenant")
    claims = dict(client.get("claims") or {})
    token_payload = token_service.issue_token_pair(
        subject=payload.client_id,
        tenant_id=tenant,
        scopes=scopes,
        base_claims=claims,
    )
    _log_auth_success(payload.client_id, tenant, request, method="client_credentials")
    _maybe_set_access_cookie(settings, response, token_payload)
    return TokenResponse(**token_payload)


@router.post("/refresh", response_model=TokenResponse, status_code=status.HTTP_200_OK)
def refresh_token(
    payload: RefreshRequest,
    response: Response,
    settings: AurumSettings = Depends(_get_settings),
    token_service: TokenService = Depends(_get_token_service),
) -> TokenResponse:
    _ensure_token_issuer_enabled(settings)
    try:
        token_payload = token_service.refresh(payload.refresh_token)
    except InvalidRefreshTokenError as exc:
        _log_auth_failure("anonymous", settings, request, reason="invalid_refresh_token")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    _log_auth_success("anonymous", None, request, method="refresh_token")
    _maybe_set_access_cookie(settings, response, token_payload)
    return TokenResponse(**token_payload)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    payload: LogoutRequest,
    request: Request,
    settings: AurumSettings = Depends(_get_settings),
    token_service: TokenService = Depends(_get_token_service),
) -> Response:
    _ensure_token_issuer_enabled(settings)
    if payload.refresh_token:
        token_service.revoke_refresh_token(payload.refresh_token)
    if payload.session_id:
        token_service.revoke_session(payload.session_id)
    security_audit.log_security_event(
        event_type="logout",
        user_id="anonymous",
        tenant_id=None,
        resource="/auth/logout",
        action="POST",
        ip_address=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        details={"session_id": payload.session_id},
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/keys", status_code=status.HTTP_200_OK)
def jwks(
    settings: AurumSettings = Depends(_get_settings),
    token_service: TokenService = Depends(_get_token_service),
) -> Dict[str, Any]:
    _ensure_token_issuer_enabled(settings)
    return token_service.jwks()


def _log_auth_success(user_id: str, tenant_id: Optional[str], request: Request, *, method: str) -> None:
    try:
        security_audit.log_auth_success(
            user_id=user_id,
            tenant_id=tenant_id,
            ip_address=_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            auth_method=method,
        )
    except Exception:
        pass


def _log_auth_failure(user_id: str, settings: AurumSettings, request: Request, *, reason: str) -> None:
    try:
        security_audit.log_auth_failure(
            user_id=user_id,
            tenant_id=None,
            ip_address=_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            reason=reason,
        )
    except Exception:
        pass


def _client_ip(request: Request) -> Optional[str]:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip
    if request.client:
        return request.client.host
    return None


__all__ = ["router"]
