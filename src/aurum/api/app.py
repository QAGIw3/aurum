from __future__ import annotations

"""Application factory for the Aurum API."""

import sys
import types

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from aurum.core import AurumSettings
from aurum.telemetry import configure_telemetry

from . import routes as _routes

from .auth import AuthMiddleware, OIDCConfig
from .config import CacheConfig
from .ratelimit import RateLimitConfig, RateLimitMiddleware
from .routes import METRICS_MIDDLEWARE, access_log_middleware, configure_routes, router
from .state import configure as configure_state


def create_app(settings: AurumSettings | None = None) -> FastAPI:
    """Create and configure an Aurum FastAPI application instance."""

    settings = settings or AurumSettings.from_env()
    configure_state(settings)
    configure_routes(settings)

    app = FastAPI(title=settings.api.title, version=settings.api.version)
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
