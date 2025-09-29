from __future__ import annotations

"""OIDC and internal token authentication middleware for the Aurum API."""

import json
import os
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from fastapi import HTTPException, status
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from aurum.core import AurumSettings
from aurum.security.audit import security_audit
from aurum.security.rbac import (
    Permission,
    Principal,
    Role,
    current_principal,
    merge_permissions,
    require_permissions,
    require_role,
)
from aurum.security.token_service import TokenService
from aurum.telemetry.context import TenantIdValidationError, get_request_id, normalize_tenant_id

try:  # pragma: no cover - import guard for optional dependency
    from jose import jwt  # type: ignore[import]
except Exception as _jwt_exc:  # pragma: no cover - import guard
    jwt = None  # type: ignore[assignment]
    _JWT_IMPORT_ERROR = _jwt_exc
else:
    _JWT_IMPORT_ERROR = None

from .http.clients import request as http_request


@dataclass(frozen=True)
class CookieConfig:
    """Cookie-based token transport configuration."""

    enabled: bool
    name: str
    secure: bool
    http_only: bool
    same_site: str
    domain: str | None
    path: str


@dataclass(frozen=True)
class OIDCConfig:
    """OIDC verification configuration."""

    issuer: str | None
    audience: str | None
    audiences: Tuple[str, ...]
    jwks_url: str | None
    disabled: bool
    leeway: int
    forward_auth_header: str | None
    forward_auth_claims_header: str | None
    required_scopes: Tuple[str, ...]
    admin_groups: Tuple[str, ...]
    cookie: CookieConfig

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "OIDCConfig":
        auth_cfg = settings.auth
        cookie_cfg = getattr(auth_cfg, "cookie", None)
        cookie = CookieConfig(
            enabled=bool(getattr(cookie_cfg, "enabled", False)),
            name=getattr(cookie_cfg, "name", "aurum_access_token"),
            secure=bool(getattr(cookie_cfg, "secure", True)),
            http_only=bool(getattr(cookie_cfg, "http_only", True)),
            same_site=str(getattr(cookie_cfg, "same_site", "lax") or "lax").lower(),
            domain=getattr(cookie_cfg, "domain", None),
            path=getattr(cookie_cfg, "path", "/"),
        )

        audiences: Tuple[str, ...]
        raw_audiences = tuple(getattr(auth_cfg, "audiences", ()) or ())
        if raw_audiences:
            audiences = tuple(str(value) for value in raw_audiences if value)
        elif getattr(auth_cfg, "oidc_audience", None):
            audiences = (str(auth_cfg.oidc_audience),)
        else:
            audiences = tuple()

        required_scopes = tuple(getattr(auth_cfg, "required_scopes", ()) or ())

        return cls(
            issuer=getattr(auth_cfg, "oidc_issuer", None),
            audience=getattr(auth_cfg, "oidc_audience", None),
            audiences=audiences,
            jwks_url=getattr(auth_cfg, "oidc_jwks_url", None),
            disabled=bool(getattr(auth_cfg, "disabled", False)),
            leeway=int(getattr(auth_cfg, "jwt_leeway_seconds", 60)),
            forward_auth_header=getattr(auth_cfg, "forward_auth_header", None),
            forward_auth_claims_header=getattr(auth_cfg, "forward_auth_claims_header", None),
            required_scopes=required_scopes,
            admin_groups=tuple(getattr(auth_cfg, "admin_groups", ()) or ()),
            cookie=cookie,
        )

    @classmethod
    def from_env(cls) -> "OIDCConfig":  # pragma: no cover - legacy convenience
        issuer = os.getenv("AURUM_API_OIDC_ISSUER")
        audience = os.getenv("AURUM_API_OIDC_AUDIENCE")
        jwks_url = os.getenv("AURUM_API_OIDC_JWKS_URL")
        disabled_flag = os.getenv("AURUM_API_AUTH_DISABLED")
        disabled = False if disabled_flag is None else disabled_flag.lower() in {"1", "true", "yes"}
        if not issuer or not jwks_url:
            disabled = True
        leeway = int(os.getenv("AURUM_API_JWT_LEEWAY", "60") or 60)

        forward_auth_header = os.getenv("AURUM_API_FORWARD_AUTH_HEADER")
        forward_auth_claims_header = os.getenv("AURUM_API_FORWARD_AUTH_CLAIMS_HEADER")
        required_scopes = tuple(
            scope.strip()
            for scope in os.getenv("AURUM_API_AUTH_REQUIRED_SCOPES", "").split(",")
            if scope.strip()
        )
        admin_groups = tuple(
            group.strip().lower()
            for group in os.getenv("AURUM_API_ADMIN_GROUP", "").split(",")
            if group.strip()
        )
        cookie = CookieConfig(
            enabled=os.getenv("AURUM_API_AUTH_COOKIE_ENABLED", "0").lower() in {"1", "true", "yes"},
            name=os.getenv("AURUM_API_AUTH_COOKIE_NAME", "aurum_access_token"),
            secure=os.getenv("AURUM_API_AUTH_COOKIE_SECURE", "1").lower() in {"1", "true", "yes"},
            http_only=os.getenv("AURUM_API_AUTH_COOKIE_HTTP_ONLY", "1").lower() in {"1", "true", "yes"},
            same_site=(os.getenv("AURUM_API_AUTH_COOKIE_SAMESITE", "lax") or "lax").lower(),
            domain=os.getenv("AURUM_API_AUTH_COOKIE_DOMAIN"),
            path=os.getenv("AURUM_API_AUTH_COOKIE_PATH", "/"),
        )

        audiences_env = tuple(
            value.strip()
            for value in os.getenv("AURUM_API_AUTH_AUDIENCES", "").split(",")
            if value.strip()
        )
        audiences = audiences_env or ((audience,) if audience else tuple())

        return cls(
            issuer=issuer,
            audience=audience,
            audiences=audiences,
            jwks_url=jwks_url,
            disabled=disabled,
            leeway=leeway,
            forward_auth_header=forward_auth_header,
            forward_auth_claims_header=forward_auth_claims_header,
            required_scopes=required_scopes,
            admin_groups=admin_groups,
            cookie=cookie,
        )


class JWKSCache:
    """Thread-safe JWKS cache with automatic refresh."""

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

    def get_key(self, kid: str) -> Optional[dict[str, Any]]:
        with self._lock:
            now = time.time()
            if self._cached is None or now >= self._expires_at:
                self._refresh_locked()
            key = self._cache_by_kid.get(kid)
            if key is not None:
                return key
            self._refresh_locked()
            return self._cache_by_kid.get(kid)


class AuthMiddleware:
    """ASGI middleware that validates Bearer tokens and attaches principals."""

    def __init__(self, app: ASGIApp, config: OIDCConfig, token_service: TokenService | None = None) -> None:
        self.app = app
        self.config = config
        self._jwks = JWKSCache(config.jwks_url, ttl_seconds=300) if config.jwks_url else None
        self._exempt_paths = {"/health", "/metrics", "/docs", "/openapi.json", "/ready"}
        self._token_service = token_service

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        scope.setdefault("state", {})

        if self.config.disabled or path in self._exempt_paths:
            scope["state"].setdefault("principal", _anonymous_principal())
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        try:
            principal = await self._authenticate_request(request, self._resolve_token_service(request))
        except HTTPException as exc:
            subject = None
            tenant = None
            detail = exc.detail
            reason = "unauthorized"
            if isinstance(detail, dict):
                subject = detail.get("subject")
                tenant = detail.get("tenant")
                reason = detail.get("error", reason)
                if detail.get("message"):
                    reason = f"{reason}:{detail['message']}"
                missing = detail.get("missing")
                if missing:
                    reason = f"missing_scope:{','.join(missing)}"
            elif isinstance(detail, str):
                reason = detail
            self._audit_failure(request, reason=reason, subject=subject, tenant=tenant)
            response = _error_response(exc.status_code, detail)
            await response(scope, receive, send)
            return

        scope["state"]["principal"] = principal
        scope["state"]["claims"] = principal.claims
        if principal.tenant_id:
            scope["state"]["tenant"] = principal.tenant_id

        await self.app(scope, receive, send)

    async def _authenticate_request(self, request: Request, token_service: Optional[TokenService]) -> Principal:
        forward_principal = self._extract_forward_auth_principal(request)
        if forward_principal is not None:
            self._audit_success(forward_principal, request, method="forward-auth")
            return forward_principal

        token, location = self._extract_token(request)
        if not token:
            self._audit_failure(request, reason="authorization_header_missing")
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="authorization_header_missing")

        claims = self._verify_jwt(token)
        principal = build_principal_from_claims(claims, self.config.admin_groups)

        missing_scopes = self._missing_scopes(principal, claims)
        if missing_scopes:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "missing_scope",
                    "missing": missing_scopes,
                    "subject": principal.subject,
                    "tenant": principal.tenant_id,
                },
            )

        if token_service and claims.get("iss") == token_service.config.issuer:
            if token_service.is_session_revoked(claims.get("sid")):
                self._audit_failure(request, reason="session_revoked", subject=principal.subject, tenant=principal.tenant_id)
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="session_revoked")

        self._audit_success(principal, request, method=location or "bearer")
        return principal

    def _extract_token(self, request: Request) -> Tuple[Optional[str], Optional[str]]:
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1].strip()
            if token:
                return token, "header"

        if self.config.cookie.enabled:
            cookie_value = request.cookies.get(self.config.cookie.name)
            if cookie_value:
                return cookie_value.strip(), "cookie"

        return None, None

    def _extract_forward_auth_principal(self, request: Request) -> Optional[Principal]:
        if not self.config.forward_auth_header:
            return None

        user_header = request.headers.get(self.config.forward_auth_header)
        if not user_header:
            return None

        claims: Dict[str, Any] = {"sub": user_header, "claims_source": "forward_auth"}

        if self.config.forward_auth_claims_header:
            claims_json = request.headers.get(self.config.forward_auth_claims_header)
            if not claims_json:
                return None
            try:
                claims_payload = json.loads(claims_json)
            except json.JSONDecodeError:
                return None
            claims.update(claims_payload)

        email = claims.get("email") or claims.get("preferred_username")
        if email:
            claims["email"] = email

        return build_principal_from_claims(claims, self.config.admin_groups)

    def _verify_jwt(self, token: str) -> dict[str, Any]:
        if not (self.config.issuer and self.config.jwks_url):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="oidc_not_configured")

        if jwt is None:
            detail = "JWT verification backend unavailable"
            if _JWT_IMPORT_ERROR is not None:
                detail = f"{detail}: {_JWT_IMPORT_ERROR}"
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)

        try:
            unverified = jwt.get_unverified_header(token)
        except Exception:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid_token_header")

        kid = unverified.get("kid")
        if not kid:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="missing_key_id")

        try:
            public_key = self._jwks.get_key(kid) if self._jwks else None
        except Exception as exc:  # pragma: no cover - network failure path
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="jwks_fetch_failed") from exc

        if public_key is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="signing_key_not_found")

        algorithms = [public_key.get("alg") or "RS256", "RS256", "ES256"]
        audience = _build_audience_param(self.config)

        try:
            claims = jwt.decode(
                token,
                public_key,
                algorithms=algorithms,
                audience=audience,
                issuer=self.config.issuer,
                options={"leeway": self.config.leeway},
            )
        except Exception:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="token_validation_failed")

        return claims

    def _missing_scopes(self, principal: Principal, claims: Mapping[str, Any]) -> Tuple[str, ...]:
        if not self.config.required_scopes:
            return tuple()
        available = set(_collect_scope_tokens(claims))
        missing = tuple(scope for scope in self.config.required_scopes if scope not in available)
        if missing:
            return missing
        return tuple()

    def _audit_success(self, principal: Principal, request: Request, *, method: str) -> None:
        try:
            security_audit.log_auth_success(
                user_id=principal.subject,
                tenant_id=principal.tenant_id,
                ip_address=_client_ip(request),
                user_agent=request.headers.get("user-agent"),
                session_id=principal.token_id,
                auth_method=method,
            )
        except Exception:  # pragma: no cover - audit failures must not break auth
            pass

    def _audit_failure(
        self,
        request: Request,
        *,
        reason: str,
        subject: Optional[str] = None,
        tenant: Optional[str] = None,
    ) -> None:
        try:
            security_audit.log_auth_failure(
                user_id=subject or "anonymous",
                tenant_id=tenant,
                ip_address=_client_ip(request),
                user_agent=request.headers.get("user-agent"),
                reason=reason,
            )
        except Exception:  # pragma: no cover - audit failures must not break auth
            pass

    def _resolve_token_service(self, request: Request) -> Optional[TokenService]:
        if self._token_service is not None:
            return self._token_service
        app_state = getattr(request.app, "state", None)
        if app_state is None:
            return None
        return getattr(app_state, "token_service", None)


def build_principal_from_claims(
    claims: Mapping[str, Any],
    admin_groups: Sequence[str],
) -> Principal:
    """Create a Principal from raw token claims."""

    subject = str(claims.get("sub") or claims.get("client_id") or "anonymous")
    email = claims.get("email") or claims.get("preferred_username")
    groups = _collect_group_tokens(claims)
    roles = _derive_roles(claims, groups, admin_groups)
    permissions = _derive_permissions(claims, roles)
    scopes = tuple(_collect_scope_tokens(claims))
    tenant_candidate = _resolve_tenant(claims, email=email, groups=groups)
    tenant = _normalize_tenant(tenant_candidate)

    enriched_claims: Dict[str, Any] = dict(claims)
    enriched_claims.setdefault("groups", groups)
    enriched_claims.setdefault("roles", [role.value for role in roles])
    enriched_claims.setdefault("permissions", [permission.value for permission in permissions])
    if tenant:
        enriched_claims["tenant"] = tenant

    return Principal(
        subject=subject,
        tenant_id=tenant,
        email=email,
        roles=roles,
        permissions=permissions,
        scopes=scopes,
        claims=enriched_claims,
        token_id=claims.get("jti"),
        issued_at=claims.get("iat"),
        expires_at=claims.get("exp"),
        not_before=claims.get("nbf"),
    )


def require_permission(
    principal: Mapping[str, Any] | Principal | None,
    permission: Permission,
    tenant_id: Optional[str] = None,
) -> None:
    """Backwards compatible imperative permission check."""

    if not principal:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="authentication_required")

    principal_obj = principal if isinstance(principal, Principal) else Principal.from_mapping(principal)
    tenant_context = tenant_id or principal_obj.tenant_id

    if not principal_obj.has_permission(permission, tenant_context):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={
                "error": "access_denied",
                "required_permission": permission.value,
                "tenant": tenant_context,
                "subject": principal_obj.subject,
            },
            headers={"X-Required-Permission": permission.value},
        )


def _build_audience_param(config: OIDCConfig) -> Optional[Sequence[str] | str]:
    if config.audiences:
        return list(config.audiences)
    return config.audience


def _collect_group_tokens(claims: Mapping[str, Any]) -> Tuple[str, ...]:
    groups_claim = claims.get("groups")
    roles_claim = claims.get("roles")
    tokens: list[str] = []
    for value in _to_iterable(groups_claim) + _to_iterable(roles_claim):
        lowered = str(value).strip().lower()
        if lowered:
            tokens.append(lowered)
    return tuple(dict.fromkeys(tokens))


def _collect_scope_tokens(claims: Mapping[str, Any]) -> Tuple[str, ...]:
    candidates = []
    for key in ("scope", "scopes", "scp"):
        value = claims.get(key)
        candidates.extend(_to_iterable(value))
    tokens: list[str] = []
    for candidate in candidates:
        if isinstance(candidate, str):
            parts = [part.strip() for part in candidate.replace(",", " ").split(" ") if part.strip()]
            tokens.extend(parts)
        elif candidate:
            tokens.append(str(candidate))
    return tuple(dict.fromkeys(token.lower() for token in tokens))


def _derive_roles(
    claims: Mapping[str, Any],
    groups: Sequence[str],
    admin_groups: Sequence[str],
) -> Tuple[Role, ...]:
    detected: list[Role] = []
    admin_group_set = {group.lower() for group in admin_groups}

    for token in groups:
        role = _match_role_token(token)
        if role:
            detected.append(role)
        elif token in admin_group_set:
            detected.append(Role.ADMIN)
        elif "super" in token and "admin" in token:
            detected.append(Role.SUPER_ADMIN)
        elif "admin" in token:
            detected.append(Role.ADMIN)
        elif "trader" in token:
            detected.append(Role.TRADER)
        elif "analyst" in token:
            detected.append(Role.ANALYST)

    if claims.get("is_admin") or claims.get("admin"):
        detected.append(Role.ADMIN)

    if not detected:
        detected.append(Role.USER)

    return tuple(dict.fromkeys(detected))


def _derive_permissions(claims: Mapping[str, Any], _roles: Sequence[Role]) -> Tuple[Permission, ...]:
    explicit_permissions = [_match_permission_token(value) for value in _to_iterable(claims.get("permissions"))]
    explicit_permissions = [permission for permission in explicit_permissions if permission]

    scope_permissions = [_SCOPE_PERMISSION_MAP[token] for token in _collect_scope_tokens(claims) if token in _SCOPE_PERMISSION_MAP]

    if claims.get("is_admin") or claims.get("admin"):
        scope_permissions.append(Permission.ADMIN)

    merged = merge_permissions(explicit_permissions, scope_permissions)
    return merged


def _resolve_tenant(
    claims: Mapping[str, Any],
    *,
    email: Optional[str],
    groups: Sequence[str],
) -> Optional[str]:
    candidate = (
        claims.get("tenant")
        or claims.get("tenant_id")
        or claims.get("org")
        or claims.get("organization")
        or claims.get("aurum_tenant")
    )

    if not candidate and email and "@" in email:
        domain = email.split("@", 1)[1]
        if "." in domain:
            candidate = domain.split(".")[0]

    if not candidate:
        for group in groups:
            if ":" not in group:
                continue
            prefix, value = group.split(":", 1)
            if prefix in {"tenant", "org", "organization"} and value:
                candidate = value
                break

    return candidate


def _normalize_tenant(candidate: Optional[str]) -> Optional[str]:
    if candidate is None:
        return None
    try:
        return normalize_tenant_id(str(candidate))
    except TenantIdValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="invalid_tenant_id") from exc


def _match_role_token(token: str) -> Optional[Role]:
    slug = token.replace("-", "_")
    try:
        return Role(slug)
    except ValueError:
        return None


def _match_permission_token(token: Any) -> Optional[Permission]:
    try:
        return Permission(str(token))
    except ValueError:
        return None


_SCOPE_PERMISSION_MAP: Dict[str, Permission] = {
    "curves:read": Permission.CURVES_READ,
    "curves:write": Permission.CURVES_WRITE,
    "curves:delete": Permission.DELETE,
    "scenarios:read": Permission.SCENARIOS_READ,
    "scenarios:write": Permission.SCENARIOS_WRITE,
    "scenarios:delete": Permission.SCENARIOS_DELETE,
    "admin": Permission.ADMIN,
    "admin:read": Permission.ADMIN_READ,
    "admin:write": Permission.ADMIN_WRITE,
    "audit:read": Permission.AUDIT,
}


def _to_iterable(value: Any) -> Tuple[Any, ...]:
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple, set)):
        return tuple(value)
    return (value,)


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


def _anonymous_principal() -> Principal:
    return Principal.from_mapping(
        {
            "sub": "anonymous",
            "tenant": None,
            "email": None,
            "roles": [Role.USER.value],
            "permissions": [],
            "claims": {},
        }
    )


def _error_response(status_code: int, detail: Any) -> JSONResponse:
    if isinstance(detail, dict):
        payload = dict(detail)
        payload.setdefault("error", "unauthorized" if status_code == 401 else "forbidden")
    else:
        payload = {
            "error": "unauthorized" if status_code == 401 else "forbidden",
            "message": str(detail),
        }
    request_id = get_request_id()
    if request_id:
        payload.setdefault("request_id", request_id)
    return JSONResponse(payload, status_code=status_code)


__all__ = [
    "AuthMiddleware",
    "OIDCConfig",
    "Permission",
    "Role",
    "Principal",
    "current_principal",
    "get_principal",
    "require_permission",
    "require_permissions",
    "require_role",
]


get_principal = current_principal
