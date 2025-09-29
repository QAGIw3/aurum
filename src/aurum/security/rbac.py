"""Role-based access control primitives shared across the Aurum API."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence, Set, Tuple

from fastapi import Depends, HTTPException, Request, status


class Permission(str, Enum):
    """Fine-grained permissions recognised by the Aurum API."""

    # Domain permissions
    CURVES_READ = "curves:read"
    CURVES_WRITE = "curves:write"
    SCENARIOS_READ = "scenarios:read"
    SCENARIOS_WRITE = "scenarios:write"
    SCENARIOS_RUN = "scenarios:run"
    SCENARIOS_DELETE = "scenarios:delete"
    ADMIN_READ = "admin:read"
    ADMIN_WRITE = "admin:write"
    FEATURE_FLAGS_MANAGE = "admin:feature_flags"
    CONFIG_MANAGE = "admin:config"
    RATE_LIMIT_MANAGE = "admin:rate_limits"
    TRINO_ADMIN = "admin:trino"
    TENANT_MANAGE = "tenant:manage"
    DEVELOPER_WORKSPACE_READ = "developer_workspace:read"
    DEVELOPER_WORKSPACE_WRITE = "developer_workspace:write"
    MODEL_REGISTRY_READ = "model_registry:read"
    MODEL_REGISTRY_WRITE = "model_registry:write"

    # Generic fallbacks for legacy paths
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    AUDIT = "audit"


class Role(str, Enum):
    """User roles available within the platform."""

    USER = "user"
    ANALYST = "analyst"
    TRADER = "trader"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"
    SYSTEM = "system"


ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.USER: {
        Permission.CURVES_READ,
        Permission.SCENARIOS_READ,
        Permission.DEVELOPER_WORKSPACE_READ,
        Permission.MODEL_REGISTRY_READ,
        Permission.READ,
    },
    Role.ANALYST: {
        Permission.CURVES_READ,
        Permission.SCENARIOS_READ,
        Permission.SCENARIOS_RUN,
        Permission.DEVELOPER_WORKSPACE_READ,
        Permission.MODEL_REGISTRY_READ,
        Permission.READ,
    },
    Role.TRADER: {
        Permission.CURVES_READ,
        Permission.CURVES_WRITE,
        Permission.SCENARIOS_READ,
        Permission.SCENARIOS_WRITE,
        Permission.SCENARIOS_RUN,
        Permission.SCENARIOS_DELETE,
        Permission.DEVELOPER_WORKSPACE_READ,
        Permission.DEVELOPER_WORKSPACE_WRITE,
        Permission.MODEL_REGISTRY_READ,
        Permission.WRITE,
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
        Permission.CONFIG_MANAGE,
        Permission.RATE_LIMIT_MANAGE,
        Permission.TRINO_ADMIN,
        Permission.DEVELOPER_WORKSPACE_READ,
        Permission.DEVELOPER_WORKSPACE_WRITE,
        Permission.MODEL_REGISTRY_READ,
        Permission.MODEL_REGISTRY_WRITE,
        Permission.ADMIN,
        Permission.WRITE,
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
        Permission.DEVELOPER_WORKSPACE_READ,
        Permission.DEVELOPER_WORKSPACE_WRITE,
        Permission.MODEL_REGISTRY_READ,
        Permission.MODEL_REGISTRY_WRITE,
        Permission.ADMIN,
        Permission.WRITE,
        Permission.DELETE,
    },
    Role.SYSTEM: {
        Permission.ADMIN,
        Permission.ADMIN_WRITE,
        Permission.ADMIN_READ,
        Permission.AUDIT,
    },
}


@dataclass(frozen=True)
class Principal(MappingABC[str, Any]):
    """Canonical representation of an authenticated identity."""

    subject: str
    tenant_id: Optional[str]
    email: Optional[str]
    roles: Tuple[Role, ...] = ()
    permissions: Tuple[Permission, ...] = ()
    scopes: Tuple[str, ...] = ()
    claims: Mapping[str, Any] = field(default_factory=dict)
    token_id: Optional[str] = None
    issued_at: Optional[int] = None
    expires_at: Optional[int] = None
    not_before: Optional[int] = None

    def has_permission(self, permission: Permission, tenant: Optional[str] = None) -> bool:
        """Check if the principal can perform a permission, optionally within a tenant."""

        if tenant and self.tenant_id and tenant != self.tenant_id:
            return False

        permission_set = set(self.permissions)
        if permission in permission_set:
            return True

        role_permissions = set()
        for role in self.roles:
            role_permissions.update(ROLE_PERMISSIONS.get(role, set()))

        return permission in role_permissions

    def has_role(self, role: Role) -> bool:
        return role in self.roles

    @staticmethod
    def from_mapping(data: Mapping[str, Any]) -> "Principal":
        """Build a principal from a mapping, tolerating legacy dict payloads."""

        subject = str(data.get("sub") or data.get("subject") or "")
        tenant = data.get("tenant") or data.get("tenant_id")
        email = data.get("email")
        claims = data.get("claims") or data
        roles_raw = data.get("roles") or data.get("groups") or []
        permissions_raw = data.get("permissions") or []
        scopes_raw = data.get("scopes") or data.get("scope") or []

        roles = _coerce_roles(roles_raw)
        permissions = _coerce_permissions(permissions_raw)
        scopes = _coerce_scopes(scopes_raw)

        return Principal(
            subject=subject,
            tenant_id=tenant,
            email=email,
            roles=roles,
            permissions=permissions,
            scopes=scopes,
            claims=claims,
            token_id=data.get("jti"),
            issued_at=data.get("iat"),
            expires_at=data.get("exp"),
            not_before=data.get("nbf"),
        )

    def __getitem__(self, key: str) -> Any:
        legacy = self._legacy_mapping()
        if key in legacy:
            return legacy[key]
        if key in self.claims:
            return self.claims[key]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        legacy_keys = list(self._legacy_mapping().keys())
        claims_keys = [str(key) for key in self.claims.keys()]
        seen = set()
        for key in legacy_keys + claims_keys:
            if key not in seen:
                seen.add(key)
                yield key

    def __len__(self) -> int:
        legacy_keys = set(self._legacy_mapping().keys())
        claims_keys = {str(key) for key in self.claims.keys()}
        return len(legacy_keys | claims_keys)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def _legacy_mapping(self) -> Dict[str, Any]:
        permissions = [permission.value for permission in self.permissions]
        roles = [role.value for role in self.roles]
        raw_groups = self.claims.get("groups")
        if isinstance(raw_groups, (list, tuple, set)):
            groups_candidates = [str(value) for value in raw_groups if value is not None]
        elif raw_groups:
            groups_candidates = [str(raw_groups)]
        else:
            groups_candidates = roles.copy()
        groups = list(dict.fromkeys(groups_candidates))
        scopes = list(self.scopes)
        tenant = self.tenant_id
        payload = {
            "sub": self.subject,
            "subject": self.subject,
            "user_id": self.subject,
            "email": self.email,
            "tenant": tenant,
            "tenant_id": tenant,
            "token_id": self.token_id,
            "roles": roles,
            "groups": groups,
            "permissions": permissions,
            "scopes": scopes,
            "claims": dict(self.claims),
        }
        return payload


@dataclass(frozen=True)
class PolicyRule:
    """Declarative policy with optional ABAC-style predicate."""

    permissions: Tuple[Permission, ...]
    tenant_scoped: bool = True
    predicate: Optional[Callable[[Principal, Request], bool]] = None
    description: Optional[str] = None


def merge_permissions(primary: Iterable[Permission], additional: Iterable[Permission] = ()) -> Tuple[Permission, ...]:
    """Return a tuple of unique permissions preserving order."""

    seen: Set[Permission] = set()
    ordered: list[Permission] = []
    for permission in (*primary, *additional):
        if permission in seen:
            continue
        seen.add(permission)
        ordered.append(permission)
    return tuple(ordered)


async def current_principal(request: Request) -> Principal:
    """Resolve the current principal from the request state."""

    principal = getattr(request.state, "principal", None)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "authentication_required"},
        )

    if isinstance(principal, Principal):
        return principal

    constructed = Principal.from_mapping(principal)
    request.state.principal = constructed
    return constructed


def require_permissions(
    *permissions: Permission,
    tenant_scoped: bool = True,
    policy: Optional[PolicyRule] = None,
) -> Callable[[Principal], Principal]:
    """FastAPI dependency that enforces permissions for the current principal."""

    if not permissions and not policy:
        raise ValueError("At least one permission or policy must be provided")

    required_permissions: Tuple[Permission, ...]
    if policy:
        required_permissions = tuple(policy.permissions)
    else:
        required_permissions = tuple(permissions)

    async def _dependency(
        request: Request,
        principal: Principal = Depends(current_principal),
    ) -> Principal:
        tenant_context: Optional[str] = None
        if tenant_scoped or (policy and policy.tenant_scoped):
            tenant_context = getattr(request.state, "tenant", None)

        for permission in required_permissions:
            if not principal.has_permission(permission, tenant_context):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "error": "access_denied",
                        "required_permission": permission.value,
                    },
                    headers={"X-Required-Permission": permission.value},
                )

        if policy and policy.predicate and not policy.predicate(principal, request):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"error": "access_denied", "reason": policy.description or "policy"},
            )

        return principal

    return _dependency


def require_role(role: Role) -> Callable[[Principal], Principal]:
    """FastAPI dependency that enforces a specific role."""

    async def _dependency(principal: Principal = Depends(current_principal)) -> Principal:
        if principal.has_role(role):
            return principal
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "access_denied", "required_role": role.value},
        )

    return _dependency


def _coerce_roles(values: Any) -> Tuple[Role, ...]:
    roles: list[Role] = []
    if isinstance(values, (list, tuple, set)):
        iterable: Iterable[Any] = values
    elif values:
        iterable = [values]
    else:
        iterable = []

    for value in iterable:
        if value is None:
            continue
        try:
            roles.append(Role(str(value).lower()))
        except ValueError:
            continue
    return tuple(dict.fromkeys(roles))


def _coerce_permissions(values: Any) -> Tuple[Permission, ...]:
    permissions: list[Permission] = []
    if isinstance(values, (list, tuple, set)):
        iterable: Iterable[Any] = values
    elif values:
        iterable = [values]
    else:
        iterable = []

    for value in iterable:
        if value is None:
            continue
        try:
            permissions.append(Permission(str(value)))
        except ValueError:
            continue
    return tuple(dict.fromkeys(permissions))


def _coerce_scopes(values: Any) -> Tuple[str, ...]:
    if isinstance(values, str):
        tokens = [token.strip() for token in values.split() if token.strip()]
    elif isinstance(values, Iterable):
        tokens = [str(token).strip() for token in values if token]
    else:
        tokens = []
    return tuple(dict.fromkeys(tokens))


__all__ = [
    "Permission",
    "Role",
    "ROLE_PERMISSIONS",
    "Principal",
    "PolicyRule",
    "current_principal",
    "require_permissions",
    "require_role",
    "merge_permissions",
]
