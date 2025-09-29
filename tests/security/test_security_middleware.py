from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.responses import PlainTextResponse

from aurum.security.auth import AuthPolicy, AuthorizationManager
from aurum.security.middleware import SecurityMiddleware
from aurum.security.rbac import Permission


def test_security_middleware_adds_headers() -> None:
    app = FastAPI()

    @app.get("/")
    def index() -> PlainTextResponse:
        return PlainTextResponse("ok")

    # Use a policy that allows anonymous access for this route
    policy = AuthPolicy(resource="/", permissions={Permission.READ}, methods={"GET"}, tenant_scoped=False)
    app.add_middleware(
        SecurityMiddleware,
        auth_manager=AuthorizationManager(policies=[policy]),
        security_headers=True,
        csp_policy="default-src 'self'",
    )

    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["Content-Security-Policy"] == "default-src 'self'"
    assert response.headers["X-Content-Type-Options"] == "nosniff"


