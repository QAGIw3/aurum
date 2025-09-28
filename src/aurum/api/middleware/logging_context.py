"""Logging context middleware for unified correlation and tracing headers.

Ensures that each request has a `request_id` and `correlation_id`, and that
context variables are populated for downstream structured logging. Adds
`traceparent` response header when tracing identifiers are available.
"""

from __future__ import annotations

import uuid
from typing import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

from aurum.telemetry.context import (
    request_id_context,
    correlation_context,
    get_trace_span_ids,
)


async def logging_context_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Bind correlation context for the duration of the request."""

    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    correlation_id = request.headers.get("x-correlation-id") or request_id
    tenant_id = request.headers.get("x-aurum-tenant")
    user_id = request.headers.get("x-user-id")

    with request_id_context(request_id), correlation_context(
        correlation_id=correlation_id,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=request_id,
    ):
        response = await call_next(request)
        # reflect IDs into response headers if missing
        response.headers.setdefault("X-Request-Id", request_id)
        response.headers.setdefault("X-Correlation-Id", correlation_id)

        trace_id, span_id = get_trace_span_ids()
        if trace_id:
            response.headers.setdefault("X-Trace-Id", trace_id)
        if span_id:
            response.headers.setdefault("X-Span-Id", span_id)
        if trace_id and span_id:
            response.headers.setdefault("traceparent", f"00-{trace_id}-{span_id}-01")
        return response


__all__ = ["logging_context_middleware"]


