from __future__ import annotations

"""Admin route guard middleware.

This middleware protects admin endpoints under `/v1/admin` and `/v2/admin`
paths by requiring an admin permission on the attached principal. It coexists
with legacy group-based checks used in specific routers.
"""

from typing import Any, Optional

from fastapi import HTTPException
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from aurum.api.auth import Permission, has_permission


class AdminRouteGuard:
    def __init__(self, app: ASGIApp, enabled: bool = True) -> None:
        self.app = app
        self.enabled = enabled
        self._prefixes = ("/v1/admin", "/v2/admin")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self.enabled or scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if not any(path.startswith(pfx) for pfx in self._prefixes):
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        principal: Optional[dict[str, Any]] = getattr(request.state, "principal", None)
        if not has_permission(principal or {}, Permission.ADMIN_READ):
            # Mirror legacy behavior: return 403 with a header indicating requirement
            raise HTTPException(
                status_code=403,
                detail="admin_required",
                headers={"X-Required-Permission": Permission.ADMIN_READ.value},
            )

        await self.app(scope, receive, send)


__all__ = ["AdminRouteGuard"]

