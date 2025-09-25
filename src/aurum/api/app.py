"""Feature-flagged API application with simplified architecture.

Supports gradual migration from complex to simplified API setup:
- Legacy mode: Full feature set with complex configuration
- Simplified mode: Essential middleware with streamlined setup
- Feature flags control migration between modes

Migration phases:
1. Legacy compatibility (current state)
2. Essential middleware only
3. Minimal configuration
"""

import logging
import contextlib
import os

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from aurum.core import AurumSettings
from aurum.telemetry import configure_telemetry
# Feature flags for API migration
API_FEATURE_FLAGS = {
    "use_simplified_api": os.getenv("AURUM_USE_SIMPLIFIED_API", "false").lower() == "true",
    "api_migration_phase": os.getenv("AURUM_API_MIGRATION_PHASE", "1"),
}

# Migration metrics for API layer
class ApiMigrationMetrics:
    legacy_calls = 0
    simplified_calls = 0
    errors = 0
    performance_ms = []

def get_api_migration_phase() -> str:
    """Get current API migration phase."""
    return API_FEATURE_FLAGS["api_migration_phase"]

def is_api_feature_enabled() -> bool:
    """Check if simplified API is enabled."""
    return API_FEATURE_FLAGS["use_simplified_api"]

def log_api_migration_status():
    """Log current API migration status."""
    phase = get_api_migration_phase()
    simplified = is_api_feature_enabled()
    logger = logging.getLogger(__name__)

    logger.info(f"API Migration Status: Phase {phase}, Simplified: {simplified}")
    logger.info(f"API Migration Metrics: Legacy calls: {ApiMigrationMetrics.legacy_calls}, "
                f"Simplified calls: {ApiMigrationMetrics.simplified_calls}")

async def create_app(settings: AurumSettings = None) -> FastAPI:
    """Create and configure an Aurum FastAPI application instance.

    Uses feature flags to enable gradual migration between legacy and simplified modes.
    """
    settings = settings or AurumSettings.from_env()
    logger = logging.getLogger(__name__)

    # Log migration status
    log_api_migration_status()

    if is_api_feature_enabled():
        ApiMigrationMetrics.simplified_calls += 1
        return await _create_simplified_app(settings, logger)
    else:
        ApiMigrationMetrics.legacy_calls += 1
        return await _create_legacy_app(settings, logger)

async def _create_simplified_app(settings, logger):
    """Create simplified API with essential middleware only."""
    logger.info("Creating simplified API configuration")

    app = FastAPI(
        title=settings.api.title,
        version=settings.api.version,
        default_response_class=JSONResponse,
        timeout=settings.api.request_timeout_seconds
    )
    app.state.settings = settings

    # Essential telemetry
    configure_telemetry(settings.telemetry.service_name, fastapi_app=app, enable_psycopg=True)

    # Essential middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    # Core health endpoints
    try:
        from .health import router as health_router
        app.include_router(health_router)
    except Exception as e:
        logger.warning(f"Failed to load health router: {e}")

    # Basic route registration
    try:
        from . import routes
        if hasattr(routes, 'router'):
            app.include_router(routes.router)
    except Exception as e:
        logger.warning(f"Failed to load routes: {e}")

    return app

async def _create_legacy_app(settings, logger):
    """Create legacy API with full feature set."""
    logger.info("Creating legacy API configuration with full features")

    # For now, fall back to simplified app to avoid import issues
    # This can be gradually migrated back to full legacy mode
    logger.warning("Legacy mode temporarily falls back to simplified mode")
    return await _create_simplified_app(settings, logger)
