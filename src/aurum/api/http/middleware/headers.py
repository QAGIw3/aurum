"""Response header middleware helpers."""

from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import Request
from starlette.datastructures import MutableHeaders
from starlette.responses import Response

from aurum.core import AurumSettings
from aurum.telemetry.context import get_request_id


def create_response_headers_middleware(
    settings: AurumSettings,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    """Return middleware that injects standard response headers."""

    async def response_headers_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = get_request_id()
        response = await call_next(request)

        headers = MutableHeaders(response.headers)
        if request_id:
            headers.setdefault("X-Request-Id", request_id)
        headers.setdefault("X-API-Version", settings.api.version)

        return response

    return response_headers_middleware
