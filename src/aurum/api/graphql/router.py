"""Phase 3: GraphQL router integration with FastAPI.

This module integrates GraphQL with the existing FastAPI application,
providing GraphQL endpoint with subscriptions support.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Request, WebSocket
from strawberry.fastapi import GraphQLRouter

from ...telemetry.context import log_structured
from .schema import schema


logger = logging.getLogger(__name__)


class AurumGraphQLRouter(GraphQLRouter):
    """Custom GraphQL router with Aurum-specific features."""
    
    async def get_context(self, request: Request = None, websocket: WebSocket = None) -> Dict[str, Any]:
        """Get GraphQL context with tenant and user information."""
        context = {}
        
        if request:
            # Extract tenant and user from headers
            context["tenant_id"] = request.headers.get("X-Aurum-Tenant")
            context["user_id"] = request.headers.get("X-Aurum-User")
            context["correlation_id"] = request.headers.get("X-Correlation-ID")
            context["request"] = request
            
        elif websocket:
            # Handle WebSocket context for subscriptions
            context["tenant_id"] = websocket.headers.get("X-Aurum-Tenant")
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


def create_graphql_router() -> APIRouter:
    """Create GraphQL router with proper configuration."""
    
    # Create custom GraphQL router
    graphql_router = AurumGraphQLRouter(
        schema,
        graphiql=True,  # Enable GraphiQL interface for development
        subscriptions_enabled=True,  # Enable WebSocket subscriptions
        introspection=True  # Enable schema introspection
    )
    
    # Create FastAPI router
    router = APIRouter(prefix="/graphql", tags=["GraphQL"])
    
    # Add GraphQL endpoints
    router.include_router(graphql_router, path="")
    
    # Health check for GraphQL
    @router.get("/health")
    async def graphql_health():
        """GraphQL service health check."""
        return {
            "status": "healthy",
            "service": "graphql",
            "schema_types": len(schema.schema.type_map),
            "subscriptions_enabled": True
        }
    
    # Schema introspection endpoint
    @router.get("/schema")
    async def get_schema():
        """Get GraphQL schema definition."""
        from strawberry.printer import print_schema
        
        return {
            "schema": print_schema(schema),
            "version": "3.0.0",
            "description": "Aurum GraphQL API Schema"
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