"""Request middleware that binds tenant context to FastAPI requests."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, MutableMapping, Optional, Sequence

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from aurum.telemetry.context import (
    reset_tenant_id,
    reset_user_id,
    set_tenant_id,
    set_user_id,
)
from aurum.tenancy import (
    TenantLifecycleState,
    TenantManager,
    TenantProvisioningError,
)


@dataclass
class TenantContextOptions:
    """Configuration flags for tenant context resolution."""

    header_name: str = "X-Aurum-Tenant"
    query_param: str = "tenant_id"
    default_tenant: Optional[str] = None
    require_tenant: bool = True
    allow_cross_tenant_roles: Sequence[str] = ("aurum:admin", "aurum:superadmin")
    auto_provision: bool = False


class TenantContextMiddleware(BaseHTTPMiddleware):
    """Populate request state and telemetry context with tenant metadata."""

    def __init__(self, app: ASGIApp, *, manager: TenantManager, options: Optional[TenantContextOptions] = None) -> None:
        super().__init__(app)
        self.manager = manager
        self.options = options or TenantContextOptions()
        self._allowed_cross_tenant = {role.lower() for role in self.options.allow_cross_tenant_roles}

    async def dispatch(self, request: Request, call_next):
        tenant_id = self._resolve_tenant(request)
        tenant_record = None
        principal = getattr(request.state, "principal", {}) or {}

        if tenant_id is None:
            if self.options.require_tenant:
                raise HTTPException(status_code=400, detail="tenant_id is required")
        else:
            tenant_record = self.manager.get_tenant(tenant_id)
            if tenant_record is None and self.options.auto_provision:
                try:
                    tenant_record = self.manager.provision_tenant(tenant_id).tenant
                except TenantProvisioningError as exc:
                    raise HTTPException(status_code=500, detail=str(exc)) from exc
            if tenant_record is None:
                raise HTTPException(status_code=404, detail="tenant_not_found")
            self._enforce_lifecycle(tenant_record)
            if self._violates_principal_scope(principal, tenant_id):
                # Return a 403 response rather than raising to avoid bubbling
                # HTTPException through BaseHTTPMiddleware task group handling in tests.
                from starlette.responses import JSONResponse
                return JSONResponse({"detail": "cross_tenant_access_forbidden"}, status_code=403)

        tenant_token = user_token = None
        try:
            if tenant_record is not None:
                tenant_token = set_tenant_id(tenant_record.tenant_id)
                request.state.tenant = tenant_record.tenant_id
                request.state.tenant_record = tenant_record
                request.state.tenant_configuration = tenant_record.configuration
                request.state.tenant_quotas = tenant_record.quotas
                request.state.tenant_session_settings = self.manager.isolation.session_settings(tenant_record.tenant_id)
            elif tenant_id:
                tenant_token = set_tenant_id(tenant_id)
            if principal:
                subject = principal.get("sub")
                if subject:
                    user_token = set_user_id(str(subject))
            response = await call_next(request)
        finally:
            if tenant_token is not None:
                reset_tenant_id(tenant_token)
            if user_token is not None:
                reset_user_id(user_token)
        return response

    def _resolve_tenant(self, request: Request) -> Optional[str]:
        state_tenant = getattr(request.state, "tenant", None)
        if state_tenant:
            return state_tenant
        header_tenant = request.headers.get(self.options.header_name)
        if header_tenant:
            return header_tenant.strip()
        query_tenant = request.query_params.get(self.options.query_param)
        if query_tenant:
            return query_tenant.strip()
        return self.options.default_tenant

    def _violates_principal_scope(self, principal: Mapping[str, object], tenant_id: str) -> bool:
        principal_tenant = principal.get("tenant") if isinstance(principal, Mapping) else None
        if principal_tenant in (None, tenant_id):
            return False
        groups: Sequence[str] | None = None
        raw_groups = principal.get("groups") if isinstance(principal, Mapping) else None
        if isinstance(raw_groups, (list, tuple, set)):
            groups = [str(group).lower() for group in raw_groups]
        elif isinstance(raw_groups, str):
            groups = [raw_groups.lower()]
        else:
            groups = []
        return not any(group in self._allowed_cross_tenant for group in groups)

    def _enforce_lifecycle(self, tenant) -> None:
        if tenant.status == TenantLifecycleState.PROVISIONING:
            raise HTTPException(status_code=503, detail="tenant_provisioning")
        if tenant.status == TenantLifecycleState.SUSPENDED:
            raise HTTPException(status_code=423, detail="tenant_suspended")
        if tenant.status == TenantLifecycleState.OFFBOARDING:
            raise HTTPException(status_code=410, detail="tenant_offboarding")
        if tenant.status == TenantLifecycleState.DEPROVISIONED:
            raise HTTPException(status_code=410, detail="tenant_deprovisioned")


__all__ = ["TenantContextMiddleware", "TenantContextOptions"]
