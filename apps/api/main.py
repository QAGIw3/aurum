"""Main FastAPI application with dependency injection and clean architecture."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from aurum.core import AurumSettings, get_settings
from aurum.api.cache.cache import CacheManager
from libs.observability.api import configure_observability, get_observability
from libs.observability.middleware import RequestContextMiddleware
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
        
        # Initialize cache manager (unified)
        self.cache_manager = CacheManager()
        
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

    # Request context propagation and standard span tags
    app.add_middleware(RequestContextMiddleware)
    
    # Routers - v2 only for clean API surface (v2-only default is true)
    from . import routers as r
    # Routers already include their own "/v2" prefixes; avoid double-prefixing
    app.include_router(r.curves)
    app.include_router(r.scenarios)
    app.include_router(r.catalog)
    app.include_router(r.market)
    app.include_router(r.admin)
    # Internal (non-production) utilities
    try:
        from .routers import internal as internal_routers
        app.include_router(internal_routers.router)
    except Exception:
        pass
    
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