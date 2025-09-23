from __future__ import annotations

"""Optional OIDC/JWT auth middleware for the Aurum API.

Enables Bearer token verification against a JWKS endpoint when configured.
Controlled via AURUM_API_AUTH_DISABLED (default: 0 / enabled when OIDC config present).
"""

import json
import os
import time
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import Any, Dict, List, Optional, Set

from aurum.core import AurumSettings
from fastapi import HTTPException
from jose import jwt
from jose.utils import base64url_decode
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from .http.clients import request as http_request


class Permission(str, Enum):
    """Authorization permissions."""
    # Data access
    CURVES_READ = "curves:read"
    CURVES_WRITE = "curves:write"

    # Scenario management
    SCENARIOS_READ = "scenarios:read"
    SCENARIOS_WRITE = "scenarios:write"
    SCENARIOS_RUN = "scenarios:run"
    SCENARIOS_DELETE = "scenarios:delete"

    # Administration
    ADMIN_READ = "admin:read"
    ADMIN_WRITE = "admin:write"

    # Specific admin features
    FEATURE_FLAGS_MANAGE = "admin:feature_flags"
    RATE_LIMIT_MANAGE = "admin:rate_limits"
    TRINO_ADMIN = "admin:trino"

    # Tenant management
    TENANT_MANAGE = "tenant:manage"


class Role(str, Enum):
    """User roles with associated permissions."""
    USER = "user"  # Basic user access
    ANALYST = "analyst"  # Data analysis access
    TRADER = "trader"  # Trading operations
    ADMIN = "admin"  # Full administrative access
    SUPER_ADMIN = "super_admin"  # System administration


@dataclass(frozen=True)
class AuthorizationConfig:
    """Authorization configuration."""
    role_permissions: Dict[Role, Set[Permission]]
    default_role: Role = Role.USER
    admin_groups: Set[str] = frozenset()

    @classmethod
    def default(cls) -> "AuthorizationConfig":
        """Default authorization configuration."""
        return cls(
            role_permissions={
                Role.USER: {
                    Permission.CURVES_READ,
                    Permission.SCENARIOS_READ,
                },
                Role.ANALYST: {
                    Permission.CURVES_READ,
                    Permission.SCENARIOS_READ,
                    Permission.SCENARIOS_RUN,
                },
                Role.TRADER: {
                    Permission.CURVES_READ,
                    Permission.CURVES_WRITE,
                    Permission.SCENARIOS_READ,
                    Permission.SCENARIOS_WRITE,
                    Permission.SCENARIOS_RUN,
                    Permission.SCENARIOS_DELETE,
                },
                Role.ADMIN: {
                    Permission.CURVES_READ,
                    Permission.CURVES_WRITE,
                    Permission.SCENARIOS_READ,
                    Permission.SCENARIOS_WRITE,
                    Permission.SCENARIOS_RUN,
                    Permission.SCENARIOS_DELETE,
                    Permission.ADMIN_READ,
                    Permission.FEATURE_FLAGS_MANAGE,
                    Permission.RATE_LIMIT_MANAGE,
                    Permission.TRINO_ADMIN,
                },
                Role.SUPER_ADMIN: {
                    Permission.CURVES_READ,
                    Permission.CURVES_WRITE,
                    Permission.SCENARIOS_READ,
                    Permission.SCENARIOS_WRITE,
                    Permission.SCENARIOS_RUN,
                    Permission.SCENARIOS_DELETE,
                    Permission.ADMIN_READ,
                    Permission.ADMIN_WRITE,
                    Permission.FEATURE_FLAGS_MANAGE,
                    Permission.RATE_LIMIT_MANAGE,
                    Permission.TRINO_ADMIN,
                    Permission.TENANT_MANAGE,
                },
            },
            admin_groups={"admin", "administrator", "superuser"},
        )


def get_user_role(principal: Dict[str, Any]) -> Role:
    """Determine user role from principal information."""
    if not principal:
        return Role.USER

    # Check groups for role indicators
    groups = principal.get("groups", [])
    if isinstance(groups, list):
        groups_lower = [str(g).lower() for g in groups]

        if any("super" in g and "admin" in g for g in groups_lower):
            return Role.SUPER_ADMIN
        elif any("admin" in g for g in groups_lower):
            return Role.ADMIN
        elif any("trader" in g for g in groups_lower):
            return Role.TRADER
        elif any("analyst" in g for g in groups_lower):
            return Role.ANALYST

    # Check email domain for role hints
    email = principal.get("email", "")
    if "@" in email:
        domain = email.split("@")[1].lower()
        if "admin" in domain or "ops" in domain:
            return Role.ADMIN

    return Role.USER


def has_permission(principal: Dict[str, Any], permission: Permission, tenant_id: Optional[str] = None) -> bool:
    """Check if user has permission for tenant."""
    if not principal:
        return False

    user_role = get_user_role(principal)
    config = AuthorizationConfig.default()

    # Get permissions for role
    role_permissions = config.role_permissions.get(user_role, set())

    # Check if user has the required permission
    if permission not in role_permissions:
        return False

    # For tenant-specific operations, ensure user belongs to tenant
    if tenant_id:
        user_tenant = principal.get("tenant")
        if user_tenant and user_tenant != tenant_id:
            return False

    return True


def require_permission(principal: Dict[str, Any], permission: Permission, tenant_id: Optional[str] = None) -> None:
    """Require permission, raising exception if not authorized."""
    if not has_permission(principal, permission, tenant_id):
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {permission.value}",
            headers={"X-Required-Permission": permission.value}
        )


@dataclass(frozen=True)
class OIDCConfig:
    issuer: str | None
    audience: str | None
    jwks_url: str | None
    disabled: bool
    leeway: int
    forward_auth_header: str | None
    forward_auth_claims_header: str | None

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

        # Traefik forward-auth support
        forward_auth_header = os.getenv("AURUM_API_FORWARD_AUTH_HEADER")
        forward_auth_claims_header = os.getenv("AURUM_API_FORWARD_AUTH_CLAIMS_HEADER")

        return cls(
            issuer=issuer,
            audience=audience,
            jwks_url=jwks_url,
            disabled=disabled,
            leeway=leeway,
            forward_auth_header=forward_auth_header,
            forward_auth_claims_header=forward_auth_claims_header,
        )

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "OIDCConfig":
        issuer = settings.auth.oidc_issuer
        audience = settings.auth.oidc_audience
        jwks_url = settings.auth.oidc_jwks_url
        disabled = settings.auth.disabled or not issuer or not jwks_url
        leeway = settings.auth.jwt_leeway_seconds

        # Traefik forward-auth support
        forward_auth_header = getattr(settings.auth, 'forward_auth_header', None)
        forward_auth_claims_header = getattr(settings.auth, 'forward_auth_claims_header', None)

        return cls(
            issuer=issuer,
            audience=audience,
            jwks_url=jwks_url,
            disabled=disabled,
            leeway=leeway,
            forward_auth_header=forward_auth_header,
            forward_auth_claims_header=forward_auth_claims_header,
        )


class JWKSCache:
    def __init__(self, url: str, ttl_seconds: int = 300) -> None:
        self._url = url
        self._ttl = ttl_seconds
        self._cached: dict[str, Any] | None = None
        self._expires_at: float = 0.0
        self._lock = Lock()
        self._cache_by_kid: dict[str, dict[str, Any]] = {}

    def _refresh_locked(self) -> None:
        resp = http_request("GET", self._url, timeout=5.0)
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
            # Set default principal for disabled auth or exempt paths
            scope.setdefault("state", {})
            scope["state"]["principal"] = {
                "sub": "anonymous",
                "email": None,
                "groups": [],
                "tenant": None,
                "claims": {},
            }
            await self.app(scope, receive, send)
            return

        request = Request(scope)

        # Try Traefik forward-auth first
        if self.config.forward_auth_header:
            principal = self._extract_forward_auth_principal(request)
            if principal:
                # Attach principal to request.state for downstream use
                scope.setdefault("state", {})
                scope["state"]["principal"] = principal

                # Also set tenant_id in request state for easy access
                if principal.get("tenant"):
                    scope["state"]["tenant"] = principal["tenant"]

                await self.app(scope, receive, send)
                return
            else:
                # Forward-auth was configured but failed, return 401 immediately
                response = _unauthorized("Forward-auth failed")
                await response(scope, receive, send)
                return

        # Fall back to OIDC JWT verification
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
        # Extract tenant from claims
        tenant = claims.get("tenant") or claims.get("org") or None

        # If no direct tenant claim, try to extract from email domain
        if not tenant:
            email = claims.get("email") or claims.get("preferred_username")
            if email and "@" in email:
                domain = email.split("@")[1]
                if "." in domain:
                    tenant = domain.split(".")[0]

        # Attach principal to request.state for downstream use
        scope.setdefault("state", {})
        scope["state"]["principal"] = {
            "sub": claims.get("sub"),
            "email": claims.get("email") or claims.get("preferred_username"),
            "groups": claims.get("groups") or claims.get("roles") or [],
            "tenant": tenant,
            "claims": claims,
        }

        # Also set tenant_id in request state for easy access
        if tenant:
            scope["state"]["tenant"] = tenant
        await self.app(scope, receive, send)

    def _extract_forward_auth_principal(self, request: Request) -> Dict[str, Any] | None:
        """Extract principal from Traefik forward-auth headers with tenant_id verification."""
        if not self.config.forward_auth_header:
            return None

        # Check for user identity header
        user_header = request.headers.get(self.config.forward_auth_header)
        if not user_header:
            return None

        principal = {
            "sub": user_header,
            "email": None,
            "groups": [],
            "tenant": None,
            "claims": {},
        }

        # Extract additional claims if configured
        if self.config.forward_auth_claims_header:
            claims_json = request.headers.get(self.config.forward_auth_claims_header)
            if claims_json:
                try:
                    claims = json.loads(claims_json)

                    # Map common claim names
                    principal["email"] = claims.get("email") or claims.get("preferred_username")
                    principal["groups"] = claims.get("groups") or claims.get("roles") or []
                    principal["claims"] = claims

                    # Extract tenant from various possible sources
                    # Priority: direct tenant claim -> org -> organization -> domain extraction -> groups
                    principal["tenant"] = (
                        claims.get("tenant") or
                        claims.get("org") or
                        claims.get("organization")
                    )

                    # If no direct tenant claim, try to extract from domain/email
                    if not principal["tenant"]:
                        if principal["email"] and "@" in principal["email"]:
                            domain = principal["email"].split("@")[1]
                            if "." in domain:
                                principal["tenant"] = domain.split(".")[0]

                    # If still no tenant, try to extract from groups
                    if not principal["tenant"]:
                        if principal["groups"]:
                            for group in principal["groups"]:
                                group_str = str(group).lower()
                                if "tenant:" in group_str:
                                    tenant_part = group_str.split("tenant:")[-1].strip()
                                    if tenant_part:
                                        principal["tenant"] = tenant_part
                                        break
                                elif "org:" in group_str:
                                    org_part = group_str.split("org:")[-1].strip()
                                    if org_part:
                                        principal["tenant"] = org_part
                                        break

                    # Validate tenant_id is present and not empty
                    if not principal["tenant"]:
                        raise HTTPException(
                            status_code=401,
                            detail="Missing tenant_id claim in forward-auth headers"
                        )

                except (json.JSONDecodeError, KeyError, HTTPException):
                    # If claims parsing fails or tenant validation fails, return None to trigger 401
                    return None
            else:
                # If forward auth claims header is configured but not present, fail
                return None

        return principal

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
