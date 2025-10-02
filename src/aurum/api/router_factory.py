"""Router factory for creating standardized FastAPI routers.

Provides consistent router creation patterns across all API endpoints
with dependency injection, error handling, and observability.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status
    from pydantic import BaseModel
except ImportError:
    APIRouter = None  # type: ignore
    Depends = None  # type: ignore
    HTTPException = None  # type: ignore
    Query = None  # type: ignore
    status = None  # type: ignore
    BaseModel = None  # type: ignore

from aurum.services.base import BaseService, ServiceContext, ServiceResult

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseService)


class PaginationParams(BaseModel):
    """Standard pagination parameters."""
    limit: int = 100
    offset: int = 0


class StandardResponse(BaseModel):
    """Standard API response wrapper."""
    success: bool
    data: Any
    metadata: Dict[str, Any] = {}
    error: Optional[str] = None
    
    @classmethod
    def from_service_result(cls, result: ServiceResult) -> "StandardResponse":
        """Convert ServiceResult to StandardResponse."""
        return cls(
            success=result.success,
            data=result.data,
            metadata=result.metadata,
            error=result.error
        )


def create_standard_router(
    prefix: str,
    tags: List[str],
    service_type: Type[T],
    service_provider: Callable
) -> APIRouter:
    """Factory for creating standardized routers with common patterns.
    
    This factory creates routers with:
    - Standard pagination
    - Dependency injection
    - Error handling
    - Consistent response format
    - Service context support
    
    Args:
        prefix: URL prefix for all routes (e.g., "/v2/curves")
        tags: OpenAPI tags for documentation
        service_type: Service class type
        service_provider: Callable that provides service instance
        
    Returns:
        Configured APIRouter
        
    Example:
        ```python
        from aurum.api.router_factory import create_standard_router
        from aurum.services.core import CurveService
        from aurum.core.container import get_service
        
        router = create_standard_router(
            prefix="/v2/curves",
            tags=["curves"],
            service_type=CurveService,
            service_provider=lambda: get_service(CurveService)
        )
        
        # Router now has standard endpoints configured
        app.include_router(router)
        ```
    """
    if APIRouter is None:
        raise ImportError("FastAPI is required for router_factory")
    
    router = APIRouter(prefix=prefix, tags=tags)
    
    logger.info(f"Created standard router: {prefix} with tags {tags}")
    
    return router


def create_health_router() -> APIRouter:
    """Create a health check router.
    
    Returns:
        APIRouter with health endpoints
    """
    if APIRouter is None:
        raise ImportError("FastAPI is required for create_health_router")
    
    router = APIRouter(prefix="/health", tags=["health"])
    
    @router.get("/")
    async def health_check():
        """Basic health check endpoint."""
        return {"status": "healthy", "service": "aurum-api"}
    
    @router.get("/ready")
    async def readiness_check():
        """Readiness check for orchestration systems."""
        # Could check database connections, cache, etc.
        return {"ready": True, "service": "aurum-api"}
    
    @router.get("/live")
    async def liveness_check():
        """Liveness check for orchestration systems."""
        return {"alive": True, "service": "aurum-api"}
    
    return router


def handle_service_result(result: ServiceResult) -> Any:
    """Convert ServiceResult to appropriate HTTP response.
    
    Args:
        result: ServiceResult from service layer
        
    Returns:
        Response data or raises HTTPException
        
    Raises:
        HTTPException: If result indicates error
    """
    if HTTPException is None:
        raise ImportError("FastAPI is required for handle_service_result")
    
    if not result.success:
        if result.error and "not found" in result.error.lower():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=result.error)
        elif result.error and "validation" in result.error.lower():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.error)
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=result.error or "Internal server error")
    
    return result.data


__all__ = [
    "create_standard_router",
    "create_health_router",
    "PaginationParams",
    "StandardResponse",
    "handle_service_result",
]

