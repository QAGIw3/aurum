"""Middleware stack manager for optimized middleware ordering.

Provides centralized middleware configuration and ordering to ensure
correct middleware execution order and avoid conflicts.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Type

try:
    from fastapi import FastAPI
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError:
    FastAPI = None  # type: ignore
    BaseHTTPMiddleware = None  # type: ignore

from aurum.core.settings import AurumSettings

logger = logging.getLogger(__name__)


class MiddlewareStackManager:
    """Manager for configuring and applying middleware stack.
    
    Middleware order matters! This class ensures correct ordering:
    1. Error handling (outermost)
    2. CORS
    3. Rate limiting
    4. Tenant isolation
    5. Request tracing
    6. Metrics collection (innermost)
    
    Middleware is applied in reverse order to achieve this execution flow.
    """
    
    def __init__(self, settings: AurumSettings):
        """Initialize with settings.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self._middleware_stack: List[Tuple[Type, Dict[str, Any]]] = []
    
    def configure_standard_stack(self) -> "MiddlewareStackManager":
        """Configure the standard middleware stack.
        
        Returns:
            Self for method chaining
        """
        # Import middleware classes (lazy import to avoid circular dependencies)
        middleware_configs = []
        
        # 6. Metrics (innermost - closest to routes)
        try:
            from aurum.api.middleware import MetricsMiddleware
            if hasattr(self.settings, 'metrics') and self.settings.metrics:
                middleware_configs.append((
                    MetricsMiddleware,
                    {"settings": self.settings.metrics}
                ))
        except ImportError:
            logger.debug("MetricsMiddleware not available")
        
        # 5. Request tracing
        try:
            from aurum.api.middleware import RequestTracingMiddleware
            if hasattr(self.settings, 'observability') and self.settings.observability:
                middleware_configs.append((
                    RequestTracingMiddleware,
                    {"settings": self.settings.observability}
                ))
        except ImportError:
            logger.debug("RequestTracingMiddleware not available")
        
        # 4. Tenant isolation
        try:
            from aurum.api.middleware import TenantIsolationMiddleware
            if hasattr(self.settings, 'tenancy') and self.settings.tenancy:
                middleware_configs.append((
                    TenantIsolationMiddleware,
                    {"settings": self.settings.tenancy}
                ))
        except ImportError:
            logger.debug("TenantIsolationMiddleware not available")
        
        # 3. Rate limiting
        try:
            from aurum.api.middleware import RateLimitMiddleware
            if hasattr(self.settings, 'rate_limiting') and self.settings.rate_limiting:
                middleware_configs.append((
                    RateLimitMiddleware,
                    {"settings": self.settings.rate_limiting}
                ))
        except ImportError:
            logger.debug("RateLimitMiddleware not available")
        
        # 2. CORS
        try:
            from starlette.middleware.cors import CORSMiddleware
            cors_settings = getattr(self.settings, 'cors', None)
            if cors_settings and getattr(cors_settings, 'enabled', False):
                middleware_configs.append((
                    CORSMiddleware,
                    {
                        "allow_origins": getattr(cors_settings, 'allow_origins', ["*"]),
                        "allow_credentials": getattr(cors_settings, 'allow_credentials', True),
                        "allow_methods": getattr(cors_settings, 'allow_methods', ["*"]),
                        "allow_headers": getattr(cors_settings, 'allow_headers', ["*"]),
                    }
                ))
        except ImportError:
            logger.debug("CORSMiddleware not available")
        
        # 1. Error handling (outermost)
        try:
            from aurum.api.middleware import ErrorHandlingMiddleware
            middleware_configs.append((
                ErrorHandlingMiddleware,
                {"settings": self.settings}
            ))
        except ImportError:
            logger.debug("ErrorHandlingMiddleware not available")
        
        self._middleware_stack = middleware_configs
        return self
    
    def add_custom_middleware(
        self,
        middleware_class: Type,
        config: Optional[Dict[str, Any]] = None,
        position: str = "end"
    ) -> "MiddlewareStackManager":
        """Add custom middleware to the stack.
        
        Args:
            middleware_class: Middleware class to add
            config: Configuration dict for middleware
            position: Where to add ("start", "end", or index)
            
        Returns:
            Self for method chaining
        """
        if position == "start":
            self._middleware_stack.insert(0, (middleware_class, config or {}))
        elif position == "end":
            self._middleware_stack.append((middleware_class, config or {}))
        elif isinstance(position, int):
            self._middleware_stack.insert(position, (middleware_class, config or {}))
        else:
            raise ValueError(f"Invalid position: {position}")
        
        return self
    
    def apply_to_app(self, app: FastAPI) -> FastAPI:
        """Apply middleware stack to FastAPI application.
        
        Middleware is applied in reverse order so that the outermost
        middleware (error handling) executes first.
        
        Args:
            app: FastAPI application
            
        Returns:
            The same FastAPI app (for chaining)
        """
        if FastAPI is None:
            raise ImportError("FastAPI is required for apply_to_app")
        
        # Apply in reverse order
        for middleware_class, config in reversed(self._middleware_stack):
            try:
                app.add_middleware(middleware_class, **config)
                logger.info(f"Applied middleware: {middleware_class.__name__}")
            except Exception as e:
                logger.warning(f"Failed to apply {middleware_class.__name__}: {e}")
        
        logger.info(f"Applied {len(self._middleware_stack)} middleware layers")
        return app
    
    def get_middleware_stack(self) -> List[Tuple[Type, Dict[str, Any]]]:
        """Get the current middleware stack configuration.
        
        Returns:
            List of (middleware_class, config) tuples in application order
        """
        return list(reversed(self._middleware_stack))


def configure_middleware(app: FastAPI, settings: AurumSettings) -> FastAPI:
    """Convenience function to configure standard middleware stack.
    
    Args:
        app: FastAPI application
        settings: Application settings
        
    Returns:
        FastAPI application with middleware configured
    """
    manager = MiddlewareStackManager(settings)
    manager.configure_standard_stack()
    return manager.apply_to_app(app)


__all__ = [
    "MiddlewareStackManager",
    "configure_middleware",
]

