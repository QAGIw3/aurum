from __future__ import annotations

"""Optional OIDC/JWT auth middleware for the Aurum API.

Enables Bearer token verification against a JWKS endpoint when configured.
Controlled via AURUM_API_AUTH_DISABLED (default: 0 / enabled when OIDC config present).
"""

import json
import os
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Optional

import httpx
from fastapi import HTTPException
from jose import jwt
from jose.utils import base64url_decode
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


@dataclass(frozen=True)
class OIDCConfig:
    issuer: str | None
    audience: str | None
    jwks_url: str | None
    disabled: bool
    leeway: int

    @classmethod
    def from_env(cls) -> "OIDCConfig":
        issuer = os.getenv("AURUM_API_OIDC_ISSUER")
        audience = os.getenv("AURUM_API_OIDC_AUDIENCE")
        jwks_url = os.getenv("AURUM_API_OIDC_JWKS_URL")
        disabled_flag = os.getenv("AURUM_API_AUTH_DISABLED")
        disabled = False if disabled_flag is None else disabled_flag.lower() in {"1", "true", "yes"}
        if not issuer or not jwks_url:
            disabled = True
        leeway = int(os.getenv("AURUM_API_JWT_LEEWAY", "60") or 60)
        return cls(issuer=issuer, audience=audience, jwks_url=jwks_url, disabled=disabled, leeway=leeway)


class JWKSCache:
    def __init__(self, url: str, ttl_seconds: int = 300) -> None:
        self._url = url
        self._ttl = ttl_seconds
        self._cached: dict[str, Any] | None = None
        self._expires_at: float = 0.0
        self._lock = Lock()
        self._cache_by_kid: dict[str, dict[str, Any]] = {}

    def _refresh_locked(self) -> None:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(self._url)
            resp.raise_for_status()
            data = resp.json()
        self._cached = data
        keys = data.get("keys", []) if isinstance(data, dict) else []
        self._cache_by_kid = {}
        for entry in keys:
            kid = entry.get("kid")
            if kid:
                self._cache_by_kid[kid] = entry
        self._expires_at = time.time() + self._ttl

    def get(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._cached is None or now >= self._expires_at:
                self._refresh_locked()
            return self._cached or {"keys": []}

    def get_key(self, kid: str) -> Optional[dict[str, Any]]:
        with self._lock:
            now = time.time()
            if self._cached is None or now >= self._expires_at:
                self._refresh_locked()
            key = self._cache_by_kid.get(kid)
            if key is not None:
                return key
            # force single refresh in case of rotation
            self._refresh_locked()
            return self._cache_by_kid.get(kid)


def _unauthorized(detail: str) -> JSONResponse:
    return JSONResponse({"error": "unauthorized", "message": detail}, status_code=401)


class AuthMiddleware:
    def __init__(self, app: ASGIApp, config: OIDCConfig) -> None:
        self.app = app
        self.config = config
        self._jwks = JWKSCache(config.jwks_url, ttl_seconds=300) if config.jwks_url else None
        self._exempt = {"/health", "/metrics", "/docs", "/openapi.json", "/ready"}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if self.config.disabled or path in self._exempt:
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.lower().startswith("bearer "):
            response = _unauthorized("Missing bearer token")
            await response(scope, receive, send)
            return
        token = auth_header.split(" ", 1)[1]

        try:
            claims = self._verify_jwt(token)
        except HTTPException as exc:
            response = _unauthorized(exc.detail)
            await response(scope, receive, send)
            return
        # Attach principal to request.state for downstream use
        scope.setdefault("state", {})
        scope["state"]["principal"] = {
            "sub": claims.get("sub"),
            "email": claims.get("email") or claims.get("preferred_username"),
            "groups": claims.get("groups") or claims.get("roles") or [],
            "tenant": claims.get("tenant") or claims.get("org") or None,
            "claims": claims,
        }
        await self.app(scope, receive, send)

    def _verify_jwt(self, token: str) -> dict[str, Any]:
        if not (self.config.issuer and self.config.jwks_url):
            raise HTTPException(status_code=401, detail="OIDC not configured")
        try:
            unverified = jwt.get_unverified_header(token)
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token header")
        kid = unverified.get("kid")
        if not kid:
            raise HTTPException(status_code=401, detail="Missing key id")
        try:
            public_key = self._jwks.get_key(kid) if self._jwks else None
        except Exception as exc:  # pragma: no cover - network failure path
            raise HTTPException(status_code=503, detail="Unable to fetch JWKS") from exc
        if public_key is None:
            raise HTTPException(status_code=401, detail="Signing key not found")

        try:
            claims = jwt.decode(
                token,
                public_key,
                algorithms=[public_key.get("alg") or "RS256", "RS256", "ES256"],
                audience=self.config.audience,
                issuer=self.config.issuer,
                options={"leeway": self.config.leeway},
            )
        except Exception:
            raise HTTPException(status_code=401, detail="Token verification failed")
        return claims


__all__ = ["AuthMiddleware", "OIDCConfig"]
