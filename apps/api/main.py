"""Main FastAPI application with dependency injection and clean architecture."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from libs.common.config import AurumSettings, get_settings
from libs.common.cache import CacheManager
from libs.common.observability import configure_observability, get_observability
from libs.storage import TimescaleSeriesRepo, PostgresMetaRepo, TrinoAnalyticRepo

# Import routers (these will be created)
from . import routers

logger = logging.getLogger(__name__)


class DependencyContainer:
    """Dependency injection container for repositories and services."""
    
    def __init__(self, settings: AurumSettings):
        self.settings = settings
        
        # Initialize repositories
        self.timescale_repo = TimescaleSeriesRepo(settings.database)
        self.postgres_repo = PostgresMetaRepo(settings.database)  
        self.trino_repo = TrinoAnalyticRepo(settings.database)
        
        # Initialize cache manager
        self.cache_manager = CacheManager(settings.redis, settings.cache)
        
        # Initialize observability
        self.observability = configure_observability(settings.observability)
        
        logger.info("Dependency container initialized")
    
    async def close(self):
        """Clean up resources."""
        await self.timescale_repo.close()
        await self.postgres_repo.close()
        await self.trino_repo.close()
        await self.cache_manager.close()
        logger.info("Dependencies cleaned up")


# Global dependency container
_container: DependencyContainer | None = None


def get_container() -> DependencyContainer:
    """Get the dependency container."""
    if _container is None:
        raise RuntimeError("Dependency container not initialized")
    return _container


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI lifespan context manager for startup/shutdown."""
    global _container
    
    settings = get_settings()
    
    # Startup
    logger.info(f"Starting Aurum API v{settings.api.version}")
    _container = DependencyContainer(settings)
    
    yield
    
    # Shutdown
    if _container:
        await _container.close()
    logger.info("Aurum API shutdown complete")


def create_app(settings: AurumSettings | None = None) -> FastAPI:
    """Create FastAPI application with clean dependency injection."""
    if settings is None:
        settings = get_settings()
    
    app = FastAPI(
        title=settings.api.title,
        version=settings.api.version,
        lifespan=lifespan,
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
    )
    
    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(
        GZipMiddleware,
        minimum_size=settings.api.gzip_min_bytes,
    )
    
    # Routers - v2 only for clean API surface
    if settings.enable_v2_only:
        # Import routers dynamically to avoid circular imports
        from .routers import series, scenarios, catalog, admin
        app.include_router(series.router, prefix="/v2/series", tags=["series"])
        app.include_router(scenarios.router, prefix="/v2/scenarios", tags=["scenarios"])
        app.include_router(catalog.router, prefix="/v2/catalog", tags=["catalog"])
        app.include_router(admin.router, prefix="/v2/admin", tags=["admin"])
    else:
        # Include both v1 and v2 during migration
        from .routers import series, scenarios, catalog, admin
        app.include_router(series.router, prefix="/v2/series", tags=["series-v2"])
        app.include_router(scenarios.router, prefix="/v2/scenarios", tags=["scenarios-v2"])
        app.include_router(catalog.router, prefix="/v2/catalog", tags=["catalog-v2"])
        app.include_router(admin.router, prefix="/v2/admin", tags=["admin-v2"])
    
    # Instrument with OpenTelemetry
    observability = get_observability()
    if observability:
        observability.instrument_fastapi(app)
    
    # Health check
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "version": settings.api.version}
    
    @app.get("/")
    async def root():
        return {"message": f"Aurum API v{settings.api.version}", "docs": "/docs"}
    
    return app


# Dependency injection helpers
def get_timescale_repo() -> TimescaleSeriesRepo:
    """Get TimescaleDB repository."""
    return get_container().timescale_repo


def get_postgres_repo() -> PostgresMetaRepo:
    """Get PostgreSQL metadata repository.""" 
    return get_container().postgres_repo


def get_trino_repo() -> TrinoAnalyticRepo:
    """Get Trino analytics repository."""
    return get_container().trino_repo


def get_cache_manager() -> CacheManager:
    """Get cache manager."""
    return get_container().cache_manager