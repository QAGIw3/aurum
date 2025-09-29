"""Request authorization policies shared by security middleware."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, Set

from fastapi import HTTPException, Request, status

from aurum.security.rbac import Permission, Principal, Role
from aurum.telemetry.context import get_tenant_id, log_structured

from .audit import log_access_denied


@dataclass(frozen=True)
class AuthPolicy:
    """Declarative policy describing who may access a resource."""

    resource: str
    permissions: Set[Permission]
    methods: Set[str]
    tenant_scoped: bool = True
    requires_admin: bool = False
    predicate: Optional[Callable[[Principal, Request], bool]] = None

    def matches(self, path: str, method: str) -> bool:
        normalized_path = re.sub(r"/{[^}]+}", "/{id}", path)
        pattern = self.resource.replace("{id}", r"[^/]+")
        pattern = pattern.replace("*", r".*")
        regex = f"^{pattern}$"
        if not re.match(regex, normalized_path):
            return False
        if "*" in self.methods:
            return True
        return method.upper() in self.methods

    def allows(self, principal: Principal, permission: Permission, request: Request) -> bool:
        if self.requires_admin and not _is_admin(principal):
            return False
        if permission not in self.permissions:
            return False
        if self.predicate and not self.predicate(principal, request):
            return False
        if not self.tenant_scoped:
            return principal.has_permission(permission, None)
        tenant_context = getattr(request.state, "tenant", None) or get_tenant_id() or principal.tenant_id
        return principal.has_permission(permission, tenant_context)


class AuthorizationManager:
    """Policy engine that validates principals against request metadata."""

    def __init__(self, policies: Optional[Sequence[AuthPolicy]] = None):
        self._policies: List[AuthPolicy] = list(policies or self._default_policies())

    def _default_policies(self) -> Sequence[AuthPolicy]:
        def policy(path: str, methods: Iterable[str], permissions: Iterable[Permission], **kwargs) -> AuthPolicy:
            return AuthPolicy(
                resource=path,
                methods={method.upper() for method in methods},
                permissions=set(permissions),
                tenant_scoped=kwargs.get("tenant_scoped", True),
                requires_admin=kwargs.get("requires_admin", False),
                predicate=kwargs.get("predicate"),
            )

        return (
            policy("/health", ["GET"], {Permission.READ}, tenant_scoped=False),
            policy("/ready", ["GET"], {Permission.READ}, tenant_scoped=False),
            policy("/metrics", ["GET"], {Permission.READ}, tenant_scoped=False),
            policy("/docs", ["GET"], {Permission.READ}, tenant_scoped=False),
            policy("/openapi.json", ["GET"], {Permission.READ}, tenant_scoped=False),
            policy("/v2/scenarios", ["GET"], {Permission.SCENARIOS_READ}),
            policy("/v2/scenarios/{id}", ["GET"], {Permission.SCENARIOS_READ}),
            policy(
                "/v2/scenarios",
                ["POST", "PUT", "PATCH"],
                {Permission.SCENARIOS_WRITE},
                requires_admin=True,
            ),
            policy(
                "/v2/scenarios/{id}",
                ["DELETE"],
                {Permission.SCENARIOS_DELETE},
                requires_admin=True,
            ),
            policy("/v2/admin/*", ["*"], {Permission.ADMIN}, requires_admin=True),
        )

    def authorize_request(
        self,
        request: Request,
        principal: Optional[Principal],
        *,
        path: Optional[str] = None,
        method: Optional[str] = None,
    ) -> None:
        """Raise if the principal is not allowed to execute the request."""

        resolved_path = path or request.url.path
        resolved_method = method or request.method

        policy = self.get_policy(resolved_path, resolved_method)
        if policy is None:
            log_structured(
                "warning",
                "no_policy_defined",
                path=resolved_path,
                method=resolved_method,
            )
            return

        if principal is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="authentication_required")

        permission = _permission_for_method(resolved_method)
        if not policy.allows(principal, permission, request):
            tenant_context = getattr(request.state, "tenant", None) or principal.tenant_id
            log_access_denied(
                user_id=principal.subject,
                tenant_id=tenant_context,
                resource=resolved_path,
                action=resolved_method,
                ip_address=_client_ip(request),
                reason=f"missing_permission:{permission.value}",
            )
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "access_denied",
                    "required_permission": permission.value,
                    "tenant": tenant_context,
                    "subject": principal.subject,
                },
            )

    def get_policy(self, path: str, method: str) -> Optional[AuthPolicy]:
        for policy in self._policies:
            if policy.matches(path, method):
                return policy
        return None

    def register_policy(self, policy: AuthPolicy) -> None:
        self._policies.append(policy)


def _permission_for_method(method: str) -> Permission:
    mapping = {
        "GET": Permission.READ,
        "HEAD": Permission.READ,
        "POST": Permission.WRITE,
        "PUT": Permission.WRITE,
        "PATCH": Permission.WRITE,
        "DELETE": Permission.DELETE,
    }
    return mapping.get(method.upper(), Permission.READ)


def _is_admin(principal: Principal) -> bool:
    return principal.has_role(Role.ADMIN) or principal.has_role(Role.SUPER_ADMIN)


def _client_ip(request: Request) -> Optional[str]:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.headers.get("x-real-ip"):
        return request.headers.get("x-real-ip")
    if request.client:
        return request.client.host
    return None


_auth_manager: Optional[AuthorizationManager] = None


def get_auth_manager() -> AuthorizationManager:
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = AuthorizationManager()
    return _auth_manager


__all__ = [
    "AuthPolicy",
    "AuthorizationManager",
    "get_auth_manager",
]
