"""Enhanced middleware registry with unified management and API governance controls.

This module provides comprehensive middleware management with:
- Unified middleware registration and ordering
- Dynamic middleware configuration
- API governance controls
- Monitoring and metrics
- Hot-reload capabilities
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union
from pathlib import Path

from fastapi import FastAPI, Request, Response
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from aurum.core import AurumSettings
from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from ..rate_limiting.unified_rate_limiter import get_unified_rate_limiter
from ..telemetry.context import extract_tenant_id_from_headers, get_request_id


class MiddlewareType(str, Enum):
    """Types of middleware available."""
    CORS = "cors"
    GZIP = "gzip" 
    RATE_LIMITING = "rate_limiting"
    CONCURRENCY = "concurrency"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    LOGGING = "logging"
    METRICS = "metrics"
    GOVERNANCE = "governance"
    SECURITY = "security"


class MiddlewarePriority(int, Enum):
    """Middleware priority levels (higher = applied first)."""
    SECURITY = 1000    # Security headers, CORS
    LOGGING = 900      # Request/response logging
    METRICS = 800      # Metrics collection
    AUTH = 700         # Authentication/authorization
    GOVERNANCE = 600   # API governance controls
    RATE_LIMITING = 500 # Rate limiting
    CONCURRENCY = 400  # Concurrency control
    COMPRESSION = 300  # GZip compression
    CUSTOM = 200       # Custom middleware
    SYSTEM = 100       # System middleware


@dataclass
class MiddlewareConfig:
    """Configuration for a middleware component."""
    name: str
    middleware_type: MiddlewareType
    priority: int = MiddlewarePriority.CUSTOM
    enabled: bool = True
    
    # Configuration options
    options: Dict[str, Any] = field(default_factory=dict)
    
    # Conditional application
    path_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    
    # Monitoring
    track_metrics: bool = True
    log_requests: bool = False


class BaseMiddleware(ABC):
    """Base class for middleware components."""
    
    def __init__(self, config: MiddlewareConfig):
        self.config = config
        self.logger = get_logger(f"middleware.{config.name}")
        self.metrics = get_metrics_client()
        self._stats = {
            "requests_processed": 0,
            "errors": 0,
            "average_duration": 0.0,
        }
    
    @abstractmethod
    async def process_request(self, request: Request) -> Optional[Response]:
        """Process incoming request. Return Response to short-circuit."""
        pass
    
    @abstractmethod
    async def process_response(self, request: Request, response: Response) -> Response:
        """Process outgoing response."""
        pass
    
    def should_apply(self, request: Request) -> bool:
        """Check if middleware should be applied to this request."""
        if not self.config.enabled:
            return False
            
        path = request.url.path
        
        # Check path patterns
        if self.config.path_patterns:
            if not any(pattern in path for pattern in self.config.path_patterns):
                return False
        
        # Check exclude patterns  
        if self.config.exclude_patterns:
            if any(pattern in path for pattern in self.config.exclude_patterns):
                return False
        
        return True
    
    def record_metrics(self, duration: float, success: bool = True):
        """Record middleware metrics."""
        if not self.config.track_metrics:
            return
            
        self._stats["requests_processed"] += 1
        if not success:
            self._stats["errors"] += 1
            
        # Update average duration
        current_avg = self._stats["average_duration"]
        count = self._stats["requests_processed"]
        self._stats["average_duration"] = ((current_avg * (count - 1)) + duration) / count
        
        self.metrics.increment_counter(
            "middleware_requests_total",
            tags={"middleware": self.config.name, "success": str(success)}
        )
        self.metrics.record_histogram(
            "middleware_duration_seconds",
            duration,
            tags={"middleware": self.config.name}
        )


class RateLimitingMiddleware(BaseMiddleware):
    """Unified rate limiting middleware."""
    
    async def process_request(self, request: Request) -> Optional[Response]:
        """Apply rate limiting to request."""
        if not self.should_apply(request):
            return None
        
        start_time = time.time()
        
        try:
            # Extract tenant ID
            tenant_id = None
            try:
                tenant_id = extract_tenant_id_from_headers(request.headers)
            except Exception:
                pass  # No tenant ID available
            
            # Check rate limits
            rate_limiter = get_unified_rate_limiter()
            if rate_limiter:
                result = await rate_limiter.check_rate_limit(request, tenant_id)
                
                if not result.allowed:
                    # Rate limit exceeded
                    headers = {}
                    if result.retry_after:
                        headers["Retry-After"] = str(int(result.retry_after))
                    if result.remaining is not None:
                        headers["X-RateLimit-Remaining"] = str(result.remaining)
                    if result.reset_time:
                        headers["X-RateLimit-Reset"] = str(int(result.reset_time))
                    
                    self.record_metrics(time.time() - start_time, False)
                    
                    return JSONResponse(
                        status_code=429,
                        content={
                            "error": "Rate limit exceeded",
                            "message": f"Too many requests. Policy: {result.policy_name}",
                            "retry_after": result.retry_after,
                            "policy": result.policy_name,
                            "algorithm": result.algorithm_used,
                        },
                        headers=headers
                    )
                
                # Add rate limit headers for successful requests
                if hasattr(request.state, "rate_limit_headers"):
                    request.state.rate_limit_headers = {}
                else:
                    request.state.rate_limit_headers = {}
                
                if result.remaining is not None:
                    request.state.rate_limit_headers["X-RateLimit-Remaining"] = str(result.remaining)
                if result.reset_time:
                    request.state.rate_limit_headers["X-RateLimit-Reset"] = str(int(result.reset_time))
            
            self.record_metrics(time.time() - start_time, True)
            return None
            
        except Exception as e:
            self.logger.error("Rate limiting error", error=str(e))
            self.record_metrics(time.time() - start_time, False)
            return None  # Allow request on error
    
    async def process_response(self, request: Request, response: Response) -> Response:
        """Add rate limiting headers to response."""
        if hasattr(request.state, "rate_limit_headers"):
            for header, value in request.state.rate_limit_headers.items():
                response.headers[header] = value
        return response


class APIGovernanceMiddleware(BaseMiddleware):
    """API governance controls middleware."""
    
    def __init__(self, config: MiddlewareConfig):
        super().__init__(config)
        self.governance_rules = config.options.get("governance_rules", {})
        self.blocked_endpoints = set(config.options.get("blocked_endpoints", []))
        self.deprecated_endpoints = set(config.options.get("deprecated_endpoints", []))
    
    async def process_request(self, request: Request) -> Optional[Response]:
        """Apply API governance controls."""
        if not self.should_apply(request):
            return None
        
        start_time = time.time()
        path = request.url.path
        
        try:
            # Check blocked endpoints
            if path in self.blocked_endpoints:
                self.record_metrics(time.time() - start_time, False)
                return JSONResponse(
                    status_code=403,
                    content={
                        "error": "Endpoint blocked",
                        "message": f"Access to {path} is currently blocked by API governance policy",
                        "endpoint": path,
                    }
                )
            
            # Check deprecated endpoints
            if path in self.deprecated_endpoints:
                self.logger.warning("Access to deprecated endpoint", path=path)
                self.metrics.increment_counter(
                    "deprecated_endpoint_access",
                    tags={"endpoint": path}
                )
            
            # API version validation
            if "version_enforcement" in self.governance_rules:
                version_header = request.headers.get("API-Version")
                min_version = self.governance_rules["version_enforcement"].get("min_version")
                
                if min_version and version_header:
                    try:
                        if float(version_header) < float(min_version):
                            self.record_metrics(time.time() - start_time, False)
                            return JSONResponse(
                                status_code=400,
                                content={
                                    "error": "API version too old",
                                    "message": f"Minimum required version: {min_version}",
                                    "current_version": version_header,
                                    "min_version": min_version,
                                }
                            )
                    except ValueError:
                        pass  # Invalid version format
            
            self.record_metrics(time.time() - start_time, True)
            return None
            
        except Exception as e:
            self.logger.error("API governance error", error=str(e))
            self.record_metrics(time.time() - start_time, False)
            return None
    
    async def process_response(self, request: Request, response: Response) -> Response:
        """Add governance headers to response."""
        path = request.url.path
        
        # Add deprecation warnings
        if path in self.deprecated_endpoints:
            response.headers["Warning"] = f"299 - \"Endpoint {path} is deprecated\""
            response.headers["Sunset"] = "2025-12-31"  # Example sunset date
        
        return response


class MetricsMiddleware(BaseMiddleware):
    """Request metrics collection middleware."""
    
    async def process_request(self, request: Request) -> Optional[Response]:
        """Record request metrics."""
        if self.should_apply(request):
            request.state.start_time = time.time()
            request.state.request_id = get_request_id()
        return None
    
    async def process_response(self, request: Request, response: Response) -> Response:
        """Record response metrics."""
        if hasattr(request.state, "start_time"):
            duration = time.time() - request.state.start_time
            
            self.metrics.increment_counter(
                "http_requests_total",
                tags={
                    "method": request.method,
                    "endpoint": request.url.path,
                    "status_code": str(response.status_code),
                }
            )
            
            self.metrics.record_histogram(
                "http_request_duration_seconds",
                duration,
                tags={
                    "method": request.method,
                    "endpoint": request.url.path,
                }
            )
        
        return response


class MiddlewareWrapper(BaseHTTPMiddleware):
    """Wrapper to integrate custom middleware with FastAPI."""
    
    def __init__(self, app: ASGIApp, middleware: BaseMiddleware):
        super().__init__(app)
        self.middleware = middleware
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Dispatch request through middleware."""
        # Process request
        response = await self.middleware.process_request(request)
        if response:
            return response
        
        # Continue to next middleware/handler
        response = await call_next(request)
        
        # Process response
        response = await self.middleware.process_response(request, response)
        return response


class EnhancedMiddlewareRegistry:
    """Enhanced middleware registry with unified management."""
    
    def __init__(self):
        self.middleware_configs: List[MiddlewareConfig] = []
        self.middleware_instances: Dict[str, BaseMiddleware] = {}
        self.logger = get_logger("middleware.registry")
        self.metrics = get_metrics_client()
        
        # Built-in middleware types
        self.middleware_types = {
            MiddlewareType.RATE_LIMITING: RateLimitingMiddleware,
            MiddlewareType.GOVERNANCE: APIGovernanceMiddleware,
            MiddlewareType.METRICS: MetricsMiddleware,
        }
    
    def register_middleware_type(self, middleware_type: MiddlewareType, middleware_class: Type[BaseMiddleware]):
        """Register a new middleware type."""
        self.middleware_types[middleware_type] = middleware_class
        self.logger.info("Registered middleware type", type=middleware_type.value)
    
    def add_middleware(self, config: MiddlewareConfig) -> bool:
        """Add middleware configuration."""
        # Check if middleware already exists
        if any(m.name == config.name for m in self.middleware_configs):
            self.logger.warning("Middleware already exists", name=config.name)
            return False
        
        self.middleware_configs.append(config)
        self.middleware_configs.sort(key=lambda m: m.priority, reverse=True)
        
        self.logger.info("Added middleware", name=config.name, type=config.middleware_type.value)
        return True
    
    def remove_middleware(self, name: str) -> bool:
        """Remove middleware by name."""
        original_count = len(self.middleware_configs)
        self.middleware_configs = [m for m in self.middleware_configs if m.name != name]
        
        if name in self.middleware_instances:
            del self.middleware_instances[name]
        
        removed = len(self.middleware_configs) < original_count
        if removed:
            self.logger.info("Removed middleware", name=name)
        return removed
    
    def get_middleware_config(self, name: str) -> Optional[MiddlewareConfig]:
        """Get middleware configuration by name."""
        return next((m for m in self.middleware_configs if m.name == name), None)
    
    def enable_middleware(self, name: str) -> bool:
        """Enable middleware by name."""
        config = self.get_middleware_config(name)
        if config:
            config.enabled = True
            self.logger.info("Enabled middleware", name=name)
            return True
        return False
    
    def disable_middleware(self, name: str) -> bool:
        """Disable middleware by name."""
        config = self.get_middleware_config(name)
        if config:
            config.enabled = False
            self.logger.info("Disabled middleware", name=name)
            return True
        return False
    
    def apply_middleware_stack(self, app: FastAPI, settings: AurumSettings) -> ASGIApp:
        """Apply middleware stack to FastAPI app."""
        # Add default middleware configurations if not present
        self._add_default_middleware()
        
        # Create middleware instances
        for config in self.middleware_configs:
            if not config.enabled:
                continue
                
            middleware_class = self.middleware_types.get(config.middleware_type)
            if middleware_class:
                try:
                    instance = middleware_class(config)
                    self.middleware_instances[config.name] = instance
                    
                    # Wrap and add to app
                    app.add_middleware(MiddlewareWrapper, middleware=instance)
                    
                except Exception as e:
                    self.logger.error("Failed to create middleware", name=config.name, error=str(e))
        
        # Apply legacy middleware for compatibility
        self._apply_legacy_middleware(app, settings)
        
        return app
    
    def _add_default_middleware(self):
        """Add default middleware configurations."""
        default_configs = [
            MiddlewareConfig(
                name="metrics",
                middleware_type=MiddlewareType.METRICS,
                priority=MiddlewarePriority.METRICS,
                track_metrics=False  # Avoid recursion
            ),
            MiddlewareConfig(
                name="rate_limiting",
                middleware_type=MiddlewareType.RATE_LIMITING,
                priority=MiddlewarePriority.RATE_LIMITING,
            ),
            MiddlewareConfig(
                name="api_governance",
                middleware_type=MiddlewareType.GOVERNANCE,
                priority=MiddlewarePriority.GOVERNANCE,
                options={
                    "governance_rules": {
                        "version_enforcement": {"min_version": "1.0"}
                    },
                    "deprecated_endpoints": ["/v1/legacy"],
                    "blocked_endpoints": [],
                }
            ),
        ]
        
        for config in default_configs:
            if not any(m.name == config.name for m in self.middleware_configs):
                self.middleware_configs.append(config)
        
        self.middleware_configs.sort(key=lambda m: m.priority, reverse=True)
    
    def _apply_legacy_middleware(self, app: FastAPI, settings: AurumSettings):
        """Apply legacy middleware for backward compatibility."""
        # CORS
        cors_origins = getattr(settings.api, "cors_origins", []) or []
        if cors_origins:
            from fastapi.middleware.cors import CORSMiddleware
            allow_origins = cors_origins if cors_origins else ["*"]
            app.add_middleware(
                CORSMiddleware,
                allow_origins=allow_origins,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )
        
        # GZip
        gzip_min = int(getattr(settings.api, "gzip_min_bytes", 0) or 0)
        if gzip_min > 0:
            from fastapi.middleware.gzip import GZipMiddleware
            app.add_middleware(GZipMiddleware, minimum_size=gzip_min)
    
    async def get_middleware_stats(self) -> Dict[str, Any]:
        """Get middleware statistics."""
        stats = {
            "total_middleware": len(self.middleware_configs),
            "enabled_middleware": len([m for m in self.middleware_configs if m.enabled]),
            "middleware_by_type": {},
            "middleware_stats": {},
        }
        
        # Count by type
        for config in self.middleware_configs:
            middleware_type = config.middleware_type.value
            stats["middleware_by_type"][middleware_type] = (
                stats["middleware_by_type"].get(middleware_type, 0) + 1
            )
        
        # Individual middleware stats
        for name, instance in self.middleware_instances.items():
            stats["middleware_stats"][name] = instance._stats
        
        return stats


# Global registry instance
_enhanced_middleware_registry: Optional[EnhancedMiddlewareRegistry] = None

def get_enhanced_middleware_registry() -> EnhancedMiddlewareRegistry:
    """Get the global enhanced middleware registry."""
    global _enhanced_middleware_registry
    if _enhanced_middleware_registry is None:
        _enhanced_middleware_registry = EnhancedMiddlewareRegistry()
    return _enhanced_middleware_registry


__all__ = [
    "MiddlewareType",
    "MiddlewarePriority",
    "MiddlewareConfig",
    "BaseMiddleware",
    "RateLimitingMiddleware",
    "APIGovernanceMiddleware",
    "MetricsMiddleware",
    "EnhancedMiddlewareRegistry",
    "get_enhanced_middleware_registry",
]