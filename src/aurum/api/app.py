from __future__ import annotations

"""Aurum API application factory and wiring.

Creates a FastAPI app with:
- Versioned routers (v1 deprecated with headers; v2 active)
- CORS, gzip, access logs, optional metrics middleware
- Optional OIDC auth and external audit middleware
- Rate limiting and cache integration via settings
- Custom OpenAPI JSON/YAML endpoints (generated from the running app)

See also:
- docs/api/README.md for usage and endpoint index
- docs/migration-guide.md for v1 → v2 migration details
- docs/runtime-config.md for admin runtime configuration
"""

import sys
import types
import logging
import contextlib
import os

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders

from aurum.core import AurumSettings
from aurum.telemetry import configure_telemetry

from aurum.performance.warehouse import WarehouseMaintenanceCoordinator
from .cache.cache import AsyncCache, CacheBackend, CacheManager
from .cache.golden_query_cache import initialize_golden_query_cache
from .features.feature_flags import initialize_feature_flags
from .idempotency import initialize_idempotency_manager, IdempotencyConfig

from . import routes as _routes

from .auth import AuthMiddleware, OIDCConfig
from .config import CacheConfig
from .rate_limiting.ratelimit import RateLimitConfig, RateLimitMiddleware, ratelimit_admin_router
from .runtime_config import router as runtime_config_router
from .routes import METRICS_MIDDLEWARE, access_log_middleware, configure_routes, router
from .state import configure as configure_state
# Observability router is optional during docs generation/local import. If
# dependencies are missing, skip registering the router rather than failing
# import (keeps app importable for tooling like docs builders).
try:  # pragma: no cover - optional
    from aurum.observability.api import router as observability_router
except Exception:  # broad except to avoid import-time issues in tooling contexts
    observability_router = None  # type: ignore

# Import versioning system
from .versioning import (
    create_versioned_app,
    get_version_manager,
    get_versioned_router,
    VersionStatus,
    DeprecationInfo
)
from .router_registry import get_v1_router_specs, get_v2_router_specs


async def create_app(settings: AurumSettings | None = None) -> FastAPI:
    """Create and configure an Aurum FastAPI application instance."""
    settings = settings or AurumSettings.from_env()
    configure_state(settings)
    configure_routes(settings)

    # Create versioned application
    # Some environments may not define description; default to empty
    api_description = getattr(settings.api, "description", "")
    version_manager, versioned_router = create_versioned_app(
        title=settings.api.title,
        description=api_description,
        version=settings.api.version
    )

    app = FastAPI(
        title=settings.api.title,
        version=settings.api.version,
        default_response_class=JSONResponse,
        timeout=settings.api.request_timeout_seconds
    )
    app.state.settings = settings

    configure_telemetry(settings.telemetry.service_name, fastapi_app=app, enable_psycopg=True)

    origins = list(settings.api.cors_allow_origins) or ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=settings.api.cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=settings.api.gzip_min_bytes)

    if settings.api.rate_limit.enabled:
        rl_config = RateLimitConfig.from_settings(settings)
        cache_cfg = CacheConfig.from_settings(settings)
        app.add_middleware(
            RateLimitMiddleware,
            cache_cfg=cache_cfg,
            rl_cfg=rl_config,
        )

    if not settings.auth.disabled:
        oidc_config = OIDCConfig.from_settings(settings)
        if not oidc_config.disabled:
            app.add_middleware(AuthMiddleware, config=oidc_config)

    app.middleware("http")(access_log_middleware)
    if settings.api.metrics.enabled and METRICS_MIDDLEWARE is not None:
        app.middleware("http")(METRICS_MIDDLEWARE)

    # Add response headers middleware for RFC 7807 compliance
    async def response_headers_middleware(request: Request, call_next):
        """Ensure all responses include X-Request-Id and X-API-Version headers."""
        from ..telemetry.context import get_request_id

        request_id = get_request_id()
        response = await call_next(request)

        # Add required headers if not already present
        headers = MutableHeaders(response.headers)
        if not headers.get("X-Request-Id"):
            headers["X-Request-Id"] = request_id
        if not headers.get("X-API-Version"):
            headers["X-API-Version"] = settings.api.version

        return response

    app.middleware("http")(response_headers_middleware)

    # Optional external audit middleware (logs to audit/compliance/security channels)
    if settings.api.audit.enabled:
        try:  # pragma: no cover - optional logging
            from .external_audit_logging import (
                audit_middleware as _audit_mw,
                configure_external_audit_logger as _configure_audit,
            )
            _configure_audit(settings.api.audit)
            app.middleware("http")(_audit_mw)
        except Exception:
            logging.getLogger(__name__).warning(
                "Failed to initialise external audit middleware",
                exc_info=True,
            )

    def _observability_admin_guard(
        principal=Depends(_routes._get_principal),
    ) -> None:
        """Require administrator access for observability endpoints."""
        _routes._require_admin(principal)

    # Register global exception handler for RFC 7807 compliance
    from .exceptions import handle_api_exception
    app.add_exception_handler(Exception, handle_api_exception)

    # Add custom OpenAPI endpoint
    from .openapi_generator import OpenAPIGenerator

    @app.get("/openapi.json", include_in_schema=False)
    async def get_openapi_json():
        """Get OpenAPI specification in JSON format."""
        generator = OpenAPIGenerator(app)
        return generator.generate_schema()

    @app.get("/openapi.yaml", include_in_schema=False)
    async def get_openapi_yaml():
        """Get OpenAPI specification in YAML format."""
        from .openapi_generator import DocumentationFormat
        import yaml

        generator = OpenAPIGenerator(app)
        schema = generator.generate_schema()

        return yaml.dump(schema, default_flow_style=False, sort_keys=False)

    if os.getenv("AURUM_API_LIGHT_INIT", "0") != "1":
        v1_specs = get_v1_router_specs(settings)

        if v1_specs:

            @app.middleware("http")
            async def add_v1_deprecation_headers(request: Request, call_next):
                """Add deprecation headers to v1 endpoints."""
                if request.url.path.startswith("/v1/"):
                    response = await call_next(request)
                    response.headers["X-API-Deprecation"] = "true"
                    response.headers["X-API-Version"] = "v1"
                    response.headers["X-API-Deprecation-Info"] = "v1 API is deprecated. Please migrate to v2."
                    response.headers["X-API-Sunset"] = "2025-12-31"
                    response.headers["X-API-Removed"] = "v3.0.0"
                    response.headers["X-API-Migration-Guide"] = "docs/migration-guide.md"
                    return response
                return await call_next(request)

        for spec in v1_specs:
            app.include_router(spec.router, **spec.include_kwargs)

        for spec in get_v2_router_specs(settings):
            app.include_router(spec.router, **spec.include_kwargs)

    # Set up versioning with feature freeze
    await version_manager.register_version(
        "1.0.0",
        status=VersionStatus.DEPRECATED,
        deprecation_info=DeprecationInfo(
            deprecated_in="2024-01-01",
            sunset_on="2025-12-31",
            removed_in="3.0.0",
            migration_guide="docs/migration-guide.md",
            alternative_endpoints=["/v2/scenarios", "/v2/curves"]
        )
    )

    await version_manager.register_version(
        "2.0.0",
        status=VersionStatus.ACTIVE,
        supported_features=[
            "cursor_pagination",
            "rfc7807_errors",
            "enhanced_etag",
            "improved_observability",
            "tenant_context_enforcement",
            "link_headers",
            "conditional_requests"
        ]
    )

    # Freeze v1 features
    await version_manager.freeze_features("1.0")

    # Initialize external dependencies on lifecycle events
    from aurum.scenarios.storage import initialize_scenario_store, close_scenario_store

    maintenance_coordinator: WarehouseMaintenanceCoordinator | None = None
    cache_service: AsyncCache | None = None
    logger = logging.getLogger(__name__)

    @app.on_event("startup")
    async def _startup():  # pragma: no cover - integration wiring
        # Prefer dedicated postgres_dsn, fallback to timescale_dsn if unset
        dsn = settings.database.postgres_dsn or settings.database.timescale_dsn
        try:
            await initialize_scenario_store(dsn)
        except Exception:
            # Keep app starting even if store init fails; endpoints will raise clearly
            pass

        nonlocal cache_service
        try:
            cache_cfg = CacheConfig.from_settings(settings)
            redis_mode = getattr(settings.redis, "mode", "standalone")
            backend = (
                CacheBackend.MEMORY
                if str(redis_mode).lower() == "disabled"
                else CacheBackend.HYBRID
            )
            cache_service = AsyncCache(cache_cfg, backend=backend)
            app.state.cache_service = cache_service
            app.state.cache_manager = CacheManager(cache_service)
            await initialize_golden_query_cache(cache_service)

            # Initialize idempotency manager
            idempotency_config = IdempotencyConfig()
            app.state.idempotency_manager = await initialize_idempotency_manager(
                idempotency_config, cache_service
            )

            # Initialize feature flags
            from ..scenarios.storage import get_scenario_store
            scenario_store = get_scenario_store()
            await initialize_feature_flags(
                redis_url=getattr(settings.redis, 'url', None),
                cache_manager=app.state.cache_manager,
                scenario_store=scenario_store
            )
        except Exception as exc:
            cache_service = None
            app.state.cache_service = None
            app.state.cache_manager = None
            logger.warning("Cache subsystem failed to initialize: %s", exc, exc_info=True)

        nonlocal maintenance_coordinator
        try:
            maintenance_coordinator = WarehouseMaintenanceCoordinator(settings)
            await maintenance_coordinator.start()
            app.state.warehouse_maintenance = maintenance_coordinator
        except Exception as exc:
            maintenance_coordinator = None
            logger.warning("Warehouse maintenance coordinator failed to start: %s", exc, exc_info=True)
            app.state.warehouse_maintenance = None

    @app.on_event("shutdown")
    async def _shutdown():  # pragma: no cover - integration wiring
        try:
            await close_scenario_store()
        except Exception:
            pass

        if maintenance_coordinator is not None:
            with contextlib.suppress(Exception):
                await maintenance_coordinator.stop()
        app.state.warehouse_maintenance = None

        if cache_service is not None:
            with contextlib.suppress(Exception):
                await cache_service.clear()
        app.state.cache_service = None
        app.state.cache_manager = None

    await version_manager.set_default_version("2.0.0")

    # Add version aliases
    await version_manager.add_version_alias("v1", "1.0.0")
    await version_manager.add_version_alias("v2", "2.0.0")
    await version_manager.add_version_alias("latest", "2.0.0")

    if observability_router is not None:
        app.include_router(
            observability_router,
            dependencies=[Depends(_observability_admin_guard)],
        )
    # Optional Trino admin router
    try:  # pragma: no cover - optional import
        from .trino_admin import router as trino_admin_router
        app.include_router(
            trino_admin_router,
            dependencies=[Depends(_observability_admin_guard)],
        )
    except Exception:
        pass
    app.include_router(
        ratelimit_admin_router,
        dependencies=[Depends(_observability_admin_guard)],
    )
    app.include_router(
        runtime_config_router,
        dependencies=[Depends(_observability_admin_guard)],
    )

    # Feature management router
    try:  # pragma: no cover - optional import
        from .features.feature_management import router as feature_management_router
        app.include_router(
            feature_management_router,
            dependencies=[Depends(_observability_admin_guard)],
        )
    except Exception:
        pass

    return app


import inspect as _inspect
if _inspect.iscoroutinefunction(create_app):  # avoid creating coroutine at import time
    app = None  # type: ignore[assignment]
else:
    app = create_app()

__all__ = ["create_app", "app"]


_ROUTE_EXPORTS = {
    "_CACHE",
    "_metadata_cache_get_or_set",
    "_persist_ppa_valuation_records",
    "_get_principal",
    "_check_trino_ready",
    "_check_timescale_ready",
    "_check_redis_ready",
    "_is_admin",
    "ADMIN_GROUPS",
    "METADATA_CACHE_TTL",
}


class _AppModule(types.ModuleType):
    """Proxy module exposing legacy attributes from ``routes``."""

    def __getattr__(self, name: str):  # pragma: no cover - trivial delegation
        if name == "router":
            return _routes.router
        if name in _ROUTE_EXPORTS:
            return getattr(_routes, name)
        raise AttributeError(f"module 'aurum.api.app' has no attribute '{name}'")

    def __setattr__(self, name: str, value):  # pragma: no cover - delegation
        if name in _ROUTE_EXPORTS:
            setattr(_routes, name, value)
        else:
            super().__setattr__(name, value)


_module = sys.modules[__name__]
if not isinstance(_module, _AppModule):  # pragma: no cover - initialization guard
    _module.__class__ = _AppModule
