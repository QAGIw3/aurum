"""Enhanced application factory with comprehensive improvements."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Optional, List, Any

try:
    from fastapi import FastAPI, Request, Response
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
except ImportError:
    FastAPI = None
    Request = None
    Response = None
    CORSMiddleware = None
    GZipMiddleware = None

from ..core import (
    get_container,
    register_singleton,
    MultiLevelCache,
    create_multi_level_cache,
    CacheLevel,
    create_memory_cache,
    BackpressureManager,
    ResilienceManager,
    create_resilient_service,
)

from .middleware.performance_middleware import PerformanceHTTPMiddleware
from .security.security_middleware import SecurityHTTPMiddleware, SecurityConfig
from .validation.input_validation import EnhancedValidator
from .modules.curves.service import AsyncCurveService
from .modules.curves.router import router as curves_router

logger = logging.getLogger(__name__)


class EnhancedAppConfig:
    """Configuration for enhanced application."""
    
    def __init__(
        self,
        # Application settings
        title: str = "Aurum Energy Trading API",
        version: str = "2.0.0",
        description: str = "Enhanced Aurum API with performance and reliability improvements",
        
        # Performance settings
        enable_performance_monitoring: bool = True,
        max_concurrent_requests: int = 1000,
        request_timeout: float = 30.0,
        
        # Caching settings
        enable_caching: bool = True,
        memory_cache_size: int = 10000,
        memory_cache_mb: int = 500,
        cache_ttl_seconds: int = 3600,
        
        # Security settings
        security_config: Optional[SecurityConfig] = None,
        
        # Resilience settings
        enable_circuit_breakers: bool = True,
        enable_retries: bool = True,
        circuit_breaker_threshold: int = 5,
        retry_attempts: int = 3,
        
        # Compression settings
        enable_compression: bool = True,
        compression_threshold: int = 1024,
        
        # CORS settings
        cors_origins: List[str] = None,
        cors_methods: List[str] = None,
    ):
        self.title = title
        self.version = version
        self.description = description
        self.enable_performance_monitoring = enable_performance_monitoring
        self.max_concurrent_requests = max_concurrent_requests
        self.request_timeout = request_timeout
        self.enable_caching = enable_caching
        self.memory_cache_size = memory_cache_size
        self.memory_cache_mb = memory_cache_mb
        self.cache_ttl_seconds = cache_ttl_seconds
        self.security_config = security_config or SecurityConfig()
        self.enable_circuit_breakers = enable_circuit_breakers
        self.enable_retries = enable_retries
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.retry_attempts = retry_attempts
        self.enable_compression = enable_compression
        self.compression_threshold = compression_threshold
        self.cors_origins = cors_origins or ["*"]
        self.cors_methods = cors_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]


class EnhancedAppFactory:
    """Factory for creating enhanced application instances."""
    
    def __init__(self, config: EnhancedAppConfig):
        self.config = config
        self._cache: Optional[MultiLevelCache] = None
        self._resilience_manager: Optional[ResilienceManager] = None
        self._backpressure_manager: Optional[BackpressureManager] = None
    
    def create_app(self) -> FastAPI:
        """Create enhanced FastAPI application."""
        if FastAPI is None:
            raise RuntimeError("FastAPI is not available")
        
        # Create lifespan context manager
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting enhanced Aurum API...")
            await self._initialize_services()
            
            yield
            
            # Shutdown
            logger.info("Shutting down enhanced Aurum API...")
            await self._cleanup_services()
        
        # Create FastAPI app
        app = FastAPI(
            title=self.config.title,
            version=self.config.version,
            description=self.config.description,
            lifespan=lifespan
        )
        
        # Add middleware
        self._add_middleware(app)
        
        # Register routes
        self._register_routes(app)
        
        # Add event handlers
        self._add_event_handlers(app)
        
        return app
    
    async def _initialize_services(self) -> None:
        """Initialize application services."""
        container = get_container()
        
        # Initialize caching
        if self.config.enable_caching:
            self._cache = self._create_cache_system()
            register_singleton(MultiLevelCache, instance=self._cache)
        
        # Initialize backpressure management
        self._backpressure_manager = BackpressureManager(
            max_concurrent=self.config.max_concurrent_requests,
            max_queue_size=self.config.max_concurrent_requests * 2
        )
        register_singleton(BackpressureManager, instance=self._backpressure_manager)
        
        # Initialize resilience patterns
        if self.config.enable_circuit_breakers or self.config.enable_retries:
            self._resilience_manager = create_resilient_service(
                failure_threshold=self.config.circuit_breaker_threshold,
                retry_attempts=self.config.retry_attempts,
                timeout_seconds=self.config.request_timeout,
                max_concurrent=self.config.max_concurrent_requests
            )
            register_singleton(ResilienceManager, instance=self._resilience_manager)
        
        # Initialize business services
        if self._cache:
            curve_service = AsyncCurveService(
                cache=self._cache,
                data_source=None,  # Would be injected in real implementation
                backpressure_manager=self._backpressure_manager
            )
            register_singleton(AsyncCurveService, instance=curve_service)
        
        logger.info("Enhanced services initialized successfully")
    
    async def _cleanup_services(self) -> None:
        """Clean up application services."""
        container = get_container()
        
        try:
            # Cleanup would be handled by the container's dispose method
            await container.dispose()
            logger.info("Services cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during service cleanup: {e}")
    
    def _create_cache_system(self) -> MultiLevelCache:
        """Create multi-level cache system."""
        # Create memory cache backend
        memory_cache = create_memory_cache(
            max_size=self.config.memory_cache_size,
            max_memory_mb=self.config.memory_cache_mb
        )
        
        # Create multi-level cache (L1 only for now, could add Redis/DB later)
        cache = create_multi_level_cache(
            memory_config={
                "max_size": self.config.memory_cache_size,
                "max_memory_mb": self.config.memory_cache_mb
            }
        )
        
        logger.info(f"Created multi-level cache system with {self.config.memory_cache_mb}MB memory cache")
        return cache
    
    def _add_middleware(self, app: FastAPI) -> None:
        """Add middleware to the application."""
        
        # Security middleware (should be first)
        if SecurityHTTPMiddleware:
            app.add_middleware(SecurityHTTPMiddleware, config=self.config.security_config)
        
        # Performance monitoring middleware
        if self.config.enable_performance_monitoring and PerformanceHTTPMiddleware:
            app.add_middleware(
                PerformanceHTTPMiddleware,
                max_concurrent_requests=self.config.max_concurrent_requests,
                request_timeout=self.config.request_timeout
            )
        
        # CORS middleware
        if CORSMiddleware:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=self.config.cors_origins,
                allow_credentials=True,
                allow_methods=self.config.cors_methods,
                allow_headers=["*"],
            )
        
        # Compression middleware (should be last)
        if self.config.enable_compression and GZipMiddleware:
            app.add_middleware(
                GZipMiddleware,
                minimum_size=self.config.compression_threshold
            )
    
    def _register_routes(self, app: FastAPI) -> None:
        """Register API routes."""
        
        # Health check endpoint
        @app.get("/health")
        async def health_check() -> Dict[str, Any]:
            """Health check endpoint."""
            metrics = {}
            
            # Get performance metrics
            if hasattr(app.state, "performance_middleware"):
                metrics["performance"] = app.state.performance_middleware.get_performance_stats()
            
            # Get cache metrics
            if self._cache:
                metrics["cache"] = self._cache.get_combined_metrics()
            
            # Get resilience metrics
            if self._resilience_manager:
                metrics["resilience"] = self._resilience_manager.get_metrics()
            
            return {
                "status": "healthy",
                "version": self.config.version,
                "timestamp": asyncio.get_event_loop().time(),
                "metrics": metrics
            }
        
        # Enhanced curves router
        if curves_router:
            app.include_router(curves_router, prefix="/v2")
        
        # Legacy API routes (would be included from existing routes)
        # app.include_router(legacy_router, prefix="/v1")
    
    def _add_event_handlers(self, app: FastAPI) -> None:
        """Add application event handlers."""
        
        @app.middleware("http")
        async def add_process_time_header(request: Request, call_next):
            """Add processing time header to responses."""
            import time
            start_time = time.time()
            response = await call_next(request)
            process_time = time.time() - start_time
            response.headers["X-Process-Time"] = str(process_time)
            return response


# Convenience functions
def create_enhanced_app(config: Optional[EnhancedAppConfig] = None) -> FastAPI:
    """Create enhanced application with default configuration."""
    if config is None:
        config = EnhancedAppConfig()
    
    factory = EnhancedAppFactory(config)
    return factory.create_app()


def create_production_app() -> FastAPI:
    """Create production-ready application."""
    config = EnhancedAppConfig(
        # Production settings
        enable_performance_monitoring=True,
        max_concurrent_requests=2000,
        request_timeout=45.0,
        
        # Enhanced caching
        memory_cache_size=50000,
        memory_cache_mb=1024,  # 1GB
        
        # Strong security
        security_config=SecurityConfig(
            enable_rate_limiting=True,
            enable_ip_filtering=True,  # Configure IPs separately
            enable_api_key_validation=True,  # Configure keys separately
            default_rate_limit=5000,  # Higher for production
            burst_limit=500,
        ),
        
        # Resilience
        enable_circuit_breakers=True,
        enable_retries=True,
        circuit_breaker_threshold=10,
        retry_attempts=5,
        
        # Compression
        enable_compression=True,
        compression_threshold=512,  # Compress smaller responses
    )
    
    return create_enhanced_app(config)


def create_development_app() -> FastAPI:
    """Create development application with relaxed settings."""
    config = EnhancedAppConfig(
        # Development settings
        enable_performance_monitoring=True,
        max_concurrent_requests=100,
        request_timeout=60.0,
        
        # Smaller cache for development
        memory_cache_size=1000,
        memory_cache_mb=100,
        
        # Relaxed security
        security_config=SecurityConfig(
            enable_rate_limiting=False,
            enable_ip_filtering=False,
            enable_api_key_validation=False,
        ),
        
        # Resilience (enabled but permissive)
        enable_circuit_breakers=True,
        enable_retries=True,
        circuit_breaker_threshold=20,
        retry_attempts=2,
    )
    
    return create_enhanced_app(config)