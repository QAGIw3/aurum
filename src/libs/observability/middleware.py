from __future__ import annotations

import ipaddress
from typing import Callable
from uuid import uuid4

from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


def _client_ip_from_request(request: Request) -> str | None:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        try:
            ipaddress.ip_address(first)
            return first
        except Exception:
            return None
    client = request.client
    return client.host if client else None


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach standard headers and span tags for request_id and tenant.

    - Ensures X-Request-Id header is present in responses
    - Echoes X-Aurum-Tenant back if provided
    - Sets span attributes: aurum.request_id, aurum.tenant_id, aurum.client_ip, http.method, http.target
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or f"req-{uuid4().hex}"
        tenant_id = request.headers.get("x-aurum-tenant")
        client_ip = _client_ip_from_request(request)

        span = trace.get_current_span()
        try:
            if span is not None:
                span.set_attribute("aurum.request_id", request_id)
                if tenant_id:
                    span.set_attribute("aurum.tenant_id", tenant_id)
                if client_ip:
                    span.set_attribute("aurum.client_ip", client_ip)
                span.set_attribute("http.method", request.method)
                span.set_attribute("http.target", request.url.path)
        except Exception:
            # best-effort tagging
            pass

        response = await call_next(request)
        response.headers.setdefault("X-Request-Id", request_id)
        if tenant_id:
            response.headers.setdefault("X-Aurum-Tenant", tenant_id)
        return response


__all__ = ["RequestContextMiddleware"]


