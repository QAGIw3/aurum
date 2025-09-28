from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from aurum.api.middleware.tenant_context import TenantContextMiddleware, TenantContextOptions
from aurum.tenancy import (
    InMemoryTenantStore,
    TenantIsolationController,
    TenantManager,
    set_tenant_manager,
)


@contextmanager
def _tenant_manager(**kwargs):
    manager = TenantManager(
        store=InMemoryTenantStore(),
        isolation=TenantIsolationController(tuple()),
        **kwargs,
    )
    set_tenant_manager(manager)
    try:
        yield manager
    finally:
        set_tenant_manager(None)


def _app(manager: TenantManager, options: TenantContextOptions) -> FastAPI:
    app = FastAPI()

    app.add_middleware(TenantContextMiddleware, manager=manager, options=options)

    @app.get("/whoami")
    async def whoami(request: Request):
        return {
            "tenant": getattr(request.state, "tenant", None),
            "settings": getattr(request.state, "tenant_session_settings", {}),
        }

    return app


def test_tenant_context_binds_request_state() -> None:
    options = TenantContextOptions(require_tenant=True)
    with _tenant_manager() as manager:
        manager.provision_tenant("acme")
        app = _app(manager, options)
        client = TestClient(app)
        response = client.get("/whoami", headers={"X-Aurum-Tenant": "acme"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["tenant"] == "acme"


def test_tenant_context_rejects_cross_tenant_principal() -> None:
    options = TenantContextOptions(require_tenant=True)
    with _tenant_manager() as manager:
        manager.provision_tenant("alpha")
        app = _app(manager, options)

        @app.middleware("http")
        async def fake_principal(request: Request, call_next):  # type: ignore[override]
            request.state.principal = {"tenant": "beta", "groups": []}
            return await call_next(request)

        client = TestClient(app)
        response = client.get("/whoami", headers={"X-Aurum-Tenant": "alpha"})
        assert response.status_code == 403


def test_tenant_context_auto_provisions_unknown_tenant() -> None:
    options = TenantContextOptions(require_tenant=True, auto_provision=True)
    with _tenant_manager() as manager:
        app = _app(manager, options)
        client = TestClient(app)
        response = client.get("/whoami", headers={"X-Aurum-Tenant": "gamma"})
        assert response.status_code == 200
        assert manager.get_tenant("gamma") is not None
