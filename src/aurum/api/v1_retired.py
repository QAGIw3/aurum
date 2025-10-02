from __future__ import annotations

from fastapi import APIRouter, Request, Response, status


router = APIRouter(prefix="/v1")


@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    status_code=status.HTTP_410_GONE,
)
async def retired_v1_endpoint(path: str, request: Request, response: Response):
    response.headers.setdefault("Deprecation", "true")
    response.headers.setdefault("Sunset", "Thu, 30 Oct 2025 23:59:59 GMT")
    response.headers.setdefault("Link", '<https://docs.aurum.dev/api/migration-v1-to-v2>; rel="deprecation"; type="text/html"')
    response.headers.setdefault("X-API-Version", "v1")
    response.headers.setdefault("X-API-Lifecycle", "retired")
    response.headers.setdefault("X-API-Migration-Guide", "https://docs.aurum.dev/api/migration-v1-to-v2")
    return {"error": "API v1 has been retired. Please migrate to /v2.", "path": f"/v1/{path}"}

"""Retired v1 catch-all router returning 410 Gone.

This router is included only when the v2-only switch is enabled and the
environment flag `AURUM_API_V1_RETIRE_STUB=1` is set. It provides a consistent
Problem Details response and deprecation headers to guide clients to v2.
"""

from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .models.common import ProblemDetail


router = APIRouter()


def _v1_deprecation_headers() -> Dict[str, str]:
    """Standard deprecation headers for retired v1 endpoints."""
    sunset = os.getenv("AURUM_API_V1_SUNSET", "Thu, 30 Oct 2025 23:59:59 GMT")
    guide = os.getenv(
        "AURUM_API_V1_MIGRATION_GUIDE",
        "https://docs.aurum.dev/api/migration-v1-to-v2",
    )
    return {
        "Deprecation": "true",
        "Sunset": sunset,
        "Link": f'<{guide}>; rel="deprecation"; type="text/html"',
        "X-API-Version": "v1",
        "X-API-Lifecycle": "retired",
        "X-API-Migration-Guide": guide,
    }


@router.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"], include_in_schema=False)
async def v1_gone(request: Request, path: str) -> JSONResponse:  # noqa: D401
    """Return 410 Gone for any v1 endpoint with guidance headers."""
    problem = ProblemDetail(
        type="https://docs.aurum.dev/problems/api-version-retired",
        title="API version retired",
        status=410,
        detail="API v1 has been retired. Please migrate to /v2.",
        instance=str(request.url),
    )
    return JSONResponse(
        status_code=410,
        content=problem.model_dump(exclude_none=True),
        headers=_v1_deprecation_headers(),
        media_type="application/problem+json",
    )

