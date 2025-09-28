"""Phase 3: GraphQL router integration with FastAPI.

This module integrates GraphQL with the existing FastAPI application,
providing GraphQL endpoint with subscriptions support.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, Response, WebSocket, WebSocketException, status
from strawberry.fastapi import GraphQLRouter

from ...telemetry.context import (
    log_structured,
    get_request_id,
    get_correlation_id,
    get_trace_span_ids,
    normalize_tenant_id,
    TenantIdValidationError,
)
from ...core import AurumSettings
from ..deps import require_tenant_id
from .schema import schema
from .cache import GraphQLResponseCache


logger = logging.getLogger(__name__)


_DEPRECATION_HEADERS = {
    "Deprecation": "true",
    "Sunset": "Wed, 31 Dec 2025 23:59:59 GMT",
    "Link": '<https://docs.aurum.dev/api/graphql#rest-alignment>; rel="deprecation"; type="text/html"',
    "X-GraphQL-Deprecated-Endpoint": "true",
}


class AurumGraphQLRouter(GraphQLRouter):
    """Custom GraphQL router with Aurum-specific features.

    In addition to context enrichment and subscription support, this router
    wraps the Strawberry handler with a read-through cache that issues ETags
    and honours conditional requests (304/412) on a per-tenant basis.
    """
    def __init__(self, *args, cache: Optional[GraphQLResponseCache] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._response_cache = cache or GraphQLResponseCache()

    async def get_context(self, request: Request = None, websocket: WebSocket = None) -> Dict[str, Any]:
        """Get GraphQL context with tenant and user information."""
        context = {}

        if request:
            # Extract tenant and user from headers
            tenant_id = getattr(request.state, "tenant_id", None)
            if tenant_id is None:
                tenant_id = require_tenant_id(request)
            context["tenant_id"] = tenant_id
            context["user_id"] = request.headers.get("X-Aurum-User")
            context["correlation_id"] = request.headers.get("X-Correlation-ID")
            context["request"] = request
            
        elif websocket:
            # Handle WebSocket context for subscriptions
            raw_tenant = websocket.headers.get("X-Aurum-Tenant")
            try:
                tenant_id = normalize_tenant_id(raw_tenant)
            except TenantIdValidationError as exc:
                raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc
            if not tenant_id:
                raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
            context["tenant_id"] = tenant_id
            context["user_id"] = websocket.headers.get("X-Aurum-User")
            context["websocket"] = websocket
        
        await log_structured(
            "graphql_context_created",
            tenant_id=context.get("tenant_id"),
            user_id=context.get("user_id"),
            has_request=bool(request),
            has_websocket=bool(websocket)
        )
        
        return context

    async def graphql_http_server(self, request: Request) -> Response:
        """Handle HTTP GraphQL requests with read-through caching."""
        tenant_id = require_tenant_id(request)
        request.state.tenant_id = tenant_id  # ensure downstream consumers see enforced tenant
        params = await self._response_cache.resolve_params(request)
        if params is None:
            response = await super().graphql_http_server(request)
            response.headers.setdefault("X-Cache", "BYPASS")
            return response

        lock = await self._response_cache.key_lock(params.cache_key)
        async with lock:
            cached = await self._response_cache.get_cached_response(params, request)
            if cached is not None:
                await log_structured(
                    "graphql_cache_hit",
                    tenant_id=params.tenant_id,
                    cache_key=params.cache_key,
                    operation=params.operation_hint,
                )
                return cached

            response = await super().graphql_http_server(request)
            cached_response = await self._response_cache.store_response(params, request, response)

            await log_structured(
                "graphql_cache_miss",
                tenant_id=params.tenant_id,
                cache_key=params.cache_key,
                operation=params.operation_hint,
                ttl=params.ttl_seconds,
            )

            return cached_response


def create_graphql_router(settings: AurumSettings | None = None) -> APIRouter:
    """Create GraphQL router with proper configuration."""

    settings = settings or AurumSettings.from_env()
    enable_interactive_tools = settings.is_development()

    # Create custom GraphQL router
    graphql_router = AurumGraphQLRouter(
        schema,
        graphiql=enable_interactive_tools,
        subscriptions_enabled=True,
        introspection=enable_interactive_tools,
    )
    
    # Create FastAPI router
    router = APIRouter(prefix="/graphql", tags=["GraphQL"])
    
    # Add GraphQL endpoints
    router.include_router(graphql_router, path="")
    
    # Health check for GraphQL
    @router.get("/health", deprecated=True)
    async def graphql_health(response: Response):
        """GraphQL service health check (deprecated in favour of `/health`)."""

        await log_structured("graphql_health_route_deprecated", endpoint="/graphql/health")

        for header, value in _DEPRECATION_HEADERS.items():
            response.headers.setdefault(header, value)

        response.headers["Link"] = '<https://docs.aurum.dev/api/health#graphql>; rel="alternate"; type="text/html"'

        # Propagate correlation headers for observability
        try:
            req_id = get_request_id()
            corr_id = get_correlation_id()
            trace_id, span_id = get_trace_span_ids()
            if req_id:
                response.headers.setdefault("X-Request-Id", req_id)
            if corr_id:
                response.headers.setdefault("X-Correlation-Id", corr_id)
            if trace_id:
                response.headers.setdefault("X-Trace-Id", trace_id)
            if span_id:
                response.headers.setdefault("X-Span-Id", span_id)
        except Exception:
            pass

        return {
            "status": "healthy",
            "service": "graphql",
            "schema_types": len(schema.schema.type_map),
            "subscriptions_enabled": True,
            "deprecated": True,
            "replacement": "/health",
        }
    
    # Schema introspection endpoint
    @router.get("/schema", deprecated=True)
    async def get_schema(response: Response):
        """Get GraphQL schema definition (deprecated in favour of SDL introspection)."""
        from strawberry.printer import print_schema

        await log_structured("graphql_schema_route_deprecated", endpoint="/graphql/schema")

        for header, value in _DEPRECATION_HEADERS.items():
            response.headers.setdefault(header, value)

        response.headers["Link"] = '<https://docs.aurum.dev/api/graphql#introspection>; rel="alternate"; type="text/html"'

        # Propagate correlation headers for observability
        try:
            req_id = get_request_id()
            corr_id = get_correlation_id()
            trace_id, span_id = get_trace_span_ids()
            if req_id:
                response.headers.setdefault("X-Request-Id", req_id)
            if corr_id:
                response.headers.setdefault("X-Correlation-Id", corr_id)
            if trace_id:
                response.headers.setdefault("X-Trace-Id", trace_id)
            if span_id:
                response.headers.setdefault("X-Span-Id", span_id)
        except Exception:
            pass

        return {
            "schema": print_schema(schema),
            "version": "3.0.0",
            "description": "Aurum GraphQL API Schema",
            "deprecated": True,
            "replacement": "POST /graphql (introspection query)",
        }
    
    return router


# Performance monitoring middleware
async def graphql_performance_middleware(request: Request, call_next):
    """Monitor GraphQL query performance."""
    import time
    
    start_time = time.time()
    
    # Check if this is a GraphQL request
    if request.url.path.startswith("/graphql"):
        # Extract query info
        if request.method == "POST":
            try:
                body = await request.body()
                # Would parse GraphQL query here in full implementation
                query_type = "unknown"
            except:
                query_type = "parse_error"
        else:
            query_type = "introspection"
        
        response = await call_next(request)
        
        execution_time = time.time() - start_time
        
        await log_structured(
            "graphql_request_completed",
            query_type=query_type,
            execution_time=execution_time,
            status_code=response.status_code
        )
        
        # Add performance headers
        response.headers["X-GraphQL-Execution-Time"] = str(execution_time)
        
        return response
    
    return await call_next(request)
