from __future__ import annotations

"""Application factory for the Aurum API."""

import sys
import types

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from aurum.core import AurumSettings
from aurum.telemetry import configure_telemetry

from . import routes as _routes

from .auth import AuthMiddleware, OIDCConfig
from .config import CacheConfig
from .ratelimit import RateLimitConfig, RateLimitMiddleware, ratelimit_admin_router
from .runtime_config import router as runtime_config_router
from .routes import METRICS_MIDDLEWARE, access_log_middleware, configure_routes, router
from .state import configure as configure_state
from aurum.observability.api import router as observability_router


def create_app(settings: AurumSettings | None = None) -> FastAPI:
    """Create and configure an Aurum FastAPI application instance."""

    settings = settings or AurumSettings.from_env()
    configure_state(settings)
    configure_routes(settings)

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

    app.include_router(
        observability_router,
        dependencies=[Depends(_observability_admin_guard)],
    )
    app.include_router(
        ratelimit_admin_router,
        dependencies=[Depends(_observability_admin_guard)],
    )
    app.include_router(
        runtime_config_router,
        dependencies=[Depends(_observability_admin_guard)],
    )
    app.include_router(router)
    return app


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
    "SCENARIO_OUTPUT_CACHE_TTL",
    "SCENARIO_METRIC_CACHE_TTL",
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
