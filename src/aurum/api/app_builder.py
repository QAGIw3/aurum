from __future__ import annotations

"""Shared builder for constructing Aurum FastAPI applications.

This module centralises the wiring that was previously duplicated between the
legacy and simplified application factory paths.  The builder owns the lifecycle
steps for initialising settings, dependency injection, middleware, telemetry,
feature flags, and router registration so that both code paths remain in sync.
"""

import asyncio
import inspect
import logging
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from aurum.core import AurumSettings

from .app_offload import build_offload_predicate
from .container import DependencyInjectionContainer, register_core_services
from .exceptions import handle_api_exception
from .middleware.manager import MiddlewareManager
from .router_registry import _register_versioned_routers
from .routes import configure_routes
from .telemetry import configure_telemetry
from .lifespan_manager import setup_lifespan
from .auth import OIDCConfig
from .container import get_app_context
from .app_offload import build_offload_predicate as _build_offload_predicate  # type: ignore[unused-import]


class ApplicationBuilder:
    """Reusable builder encapsulating FastAPI application composition."""

    def __init__(
        self,
        settings: AurumSettings,
        logger: logging.Logger,
        *,
        mode: str,
        strict_token_service: bool = False,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.mode = mode
        self.strict_token_service = strict_token_service
        self.app: Optional[FastAPI] = None
        self.container: Optional[DependencyInjectionContainer] = None
        self.tenant_manager = None
        self.tenant_context_options = None
        self.token_service = None

    def build(self) -> FastAPI:
        from .app import (  # Local import to avoid circular dependency
            _configure_admin_groups,
            _require_admin_groups,
            _initialize_tenant_manager,
            _initialize_token_service,
            _register_metrics_endpoint,
            _register_trino_lifecycle,
            _include_fallback_routes,
        )

        _configure_admin_groups(self.settings)
        _require_admin_groups(self.settings, self.logger)

        self.app = FastAPI(
            title=self.settings.api.api_title,
            version=self.settings.api.version,
            default_response_class=JSONResponse,
            timeout=self.settings.api.request_timeout_seconds,
            responses=self._global_error_responses(),
            lifespan=setup_lifespan(self.settings),
            docs_url="/docs" if getattr(self.settings, "debug", False) else None,
            redoc_url="/redoc" if getattr(self.settings, "debug", False) else None,
        )

        self.app.state.settings = self.settings

        self.container = DependencyInjectionContainer.from_settings(self.settings)
        register_core_services(self.container)
        self.app.state.container = self.container

        self.tenant_manager, self.tenant_context_options = _initialize_tenant_manager(
            self.settings, self.container
        )
        self.app.state.tenant_manager = self.tenant_manager

        self.token_service = self._init_token_service(_initialize_token_service)

        self._init_feature_flags()

        self.app.add_exception_handler(Exception, handle_api_exception)

        configure_routes(self.settings)

        self._configure_telemetry()

        manager = MiddlewareManager()
        manager.add_defaults(
            self.settings,
            tenant_manager=self.tenant_manager,
            tenant_context_options=self.tenant_context_options,
            token_service=self.token_service,
        )
        wrapped_app = manager.apply(self.app, self.settings)

        _register_trino_lifecycle(self.app)
        _register_metrics_endpoint(self.app, self.settings)

        self._include_health_router()
        self._include_auth_router()

        v2_only = True
        try:
            v2_only = bool(getattr(self.settings, "enable_v2_only", True))
        except Exception:
            v2_only = True

        if self._is_light_init():
            if not v2_only:
                _include_fallback_routes(self.app, self.logger)
        else:
            if not _register_versioned_routers(self.app, self.settings, self.logger):
                if not v2_only:
                    _include_fallback_routes(self.app, self.logger)

        return wrapped_app

    def _init_token_service(self, initializer):
        try:
            return initializer(self.app, self.settings)
        except Exception as exc:
            if self.strict_token_service:
                raise
            self.logger.warning("Failed to initialize token service", exc_info=exc)
            return None

    def _init_feature_flags(self) -> None:
        if not self.container:
            return
        try:
            from aurum.api.features import initialize_feature_flags
            from aurum.cache.cache import get_cache_manager

            cache_manager = get_cache_manager()
            if cache_manager is None and "cache_manager" in self.container:
                cache_manager = self.container.get("cache_manager")

            scenario_store = None
            if "scenario_store" in self.container:
                scenario_store = self.container.get("scenario_store")

            init_result = initialize_feature_flags(
                redis_url=getattr(self.settings, "redis_url", None),
                cache_manager=cache_manager,
                scenario_store=scenario_store,
            )
            feature_manager = (
                asyncio.run(init_result)
                if inspect.isawaitable(init_result)
                else init_result
            )
            self.app.state.feature_manager = feature_manager
            self.logger.info("Feature flag manager initialized successfully")
        except Exception as exc:
            self.logger.warning("Failed to initialize feature flag manager: %s", exc)

    def _configure_telemetry(self) -> None:
        service_name = self.settings.telemetry.service_name
        configure_telemetry(service_name, fastapi_app=self.app, enable_psycopg=True)

    def _include_health_router(self) -> None:
        try:
            from .health import router as health_router

            self.app.include_router(health_router)
        except Exception as exc:
            self.logger.warning("Failed to load health router: %s", exc)

    def _include_auth_router(self) -> None:
        if self.token_service is None:
            return
        try:
            from aurum.api.auth_endpoints import router as auth_router

            self.app.include_router(auth_router)
        except Exception as exc:
            self.logger.warning("Failed to load auth router", exc_info=exc)

    def _is_light_init(self) -> bool:
        return bool(getattr(self.settings, "api_light_init", False))

    @staticmethod
    def _global_error_responses():
        from .app import GLOBAL_ERROR_RESPONSES  # Local import to avoid cycle

        return GLOBAL_ERROR_RESPONSES


__all__ = ["ApplicationBuilder"]

