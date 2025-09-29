from __future__ import annotations

"""Composable middleware manager with ordering and configuration.

This module provides a small, focused registry that lets us:
- Register middleware with priorities and dependencies
- Enable/disable middleware based on settings or runtime toggles
- Compose Starlette class middleware, function-based HTTP middleware, and
  ASGI wrappers (that return a new app) in a single ordered chain

Design goals:
- KISS: single, clear surface area; no global state
- Testable: deterministic ordering and a `describe_order()` helper
- Non-invasive: works with existing middleware implementations without changes
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from fastapi import FastAPI
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp

from aurum.core import AurumSettings

# Existing middleware components
from .logging_context import logging_context_middleware
from .rfc7807 import RFC7807ExceptionMiddleware
from ..http.middleware.access import access_log_middleware
from ..http.middleware.headers import create_response_headers_middleware
from .admin_guard import AdminRouteGuard
from ..auth import AuthMiddleware, OIDCConfig
from aurum.security.middleware import SecurityMiddleware
from .tenant_context import TenantContextMiddleware, TenantContextOptions
from .resource_cleanup import resource_cleanup_middleware


HttpMiddlewareFactory = Callable[[FastAPI, ASGIApp, AurumSettings], ASGIApp]


@dataclass
class RegisteredMiddleware:
    name: str
    priority: int
    enabled: bool
    applier: HttpMiddlewareFactory
    depends_on: Tuple[str, ...] = field(default_factory=tuple)


class MiddlewareManager:
    """Registry and composer for API middleware.

    Usage:
        manager = MiddlewareManager()
        manager.add_defaults(settings, tenant_manager=None, tenant_context_options=None)
        final_app = manager.apply(app, settings)
    """

    def __init__(self) -> None:
        self._entries: List[RegisteredMiddleware] = []
        self._by_name: Dict[str, RegisteredMiddleware] = {}

    # --- Registration helpers -------------------------------------------------
    def register(self, *, name: str, priority: int, enabled: bool, applier: HttpMiddlewareFactory, depends_on: Sequence[str] | None = None) -> None:
        entry = RegisteredMiddleware(
            name=name,
            priority=int(priority),
            enabled=bool(enabled),
            applier=applier,
            depends_on=tuple(depends_on or ()),
        )
        self._entries.append(entry)
        self._by_name[name] = entry

    def register_http(self, *, name: str, priority: int, enabled: bool, handler: Callable) -> None:
        def _applier(fastapi_app: FastAPI, app_chain: ASGIApp, _settings: AurumSettings) -> ASGIApp:
            fastapi_app.middleware("http")(handler)
            return app_chain

        self.register(name=name, priority=priority, enabled=enabled, applier=_applier)

    def register_class(self, *, name: str, priority: int, enabled: bool, cls: type, kwargs: Optional[Dict[str, Any]] = None) -> None:
        def _applier(fastapi_app: FastAPI, app_chain: ASGIApp, _settings: AurumSettings) -> ASGIApp:
            fastapi_app.add_middleware(cls, **(kwargs or {}))
            return app_chain

        self.register(name=name, priority=priority, enabled=enabled, applier=_applier)

    def register_wrapper(self, *, name: str, priority: int, enabled: bool, wrap: Callable[[ASGIApp, AurumSettings], ASGIApp], depends_on: Sequence[str] | None = None) -> None:
        def _applier(_fastapi_app: FastAPI, app_chain: ASGIApp, settings: AurumSettings) -> ASGIApp:
            return wrap(app_chain, settings)

        self.register(name=name, priority=priority, enabled=enabled, applier=_applier, depends_on=depends_on)

    def set_enabled(self, name: str, enabled: bool) -> None:
        entry = self._by_name.get(name)
        if entry:
            entry.enabled = bool(enabled)

    # --- Composition ----------------------------------------------------------
    def apply(self, app: FastAPI, settings: AurumSettings) -> ASGIApp:
        """Apply all enabled middleware in priority order and return the ASGI app chain.

        We add Starlette middleware and HTTP middleware to the FastAPI `app` in
        the same ordered pass, and we wrap the ASGI app chain for wrappers such
        as concurrency or rate limiting.
        """
        self._validate_dependencies()

        # Sort ascending, so highest priority is applied last → outermost
        ordered = sorted(self._entries, key=lambda e: e.priority)

        chain: ASGIApp = app
        for entry in ordered:
            if not entry.enabled:
                continue
            chain = entry.applier(app, chain, settings)
        return chain

    def describe_order(self) -> List[str]:
        """Return the ordered list of enabled middleware names for testing/inspection."""
        return [e.name for e in sorted(self._entries, key=lambda e: e.priority) if e.enabled]

    def _validate_dependencies(self) -> None:
        missing: List[Tuple[str, str]] = []
        for entry in self._entries:
            if not entry.enabled:
                continue
            for dep in entry.depends_on:
                dep_entry = self._by_name.get(dep)
                if dep_entry is None or not dep_entry.enabled:
                    missing.append((entry.name, dep))
        if missing:
            # Dependency warnings; we don't hard-fail to preserve availability
            import logging

            logger = logging.getLogger(__name__)
            for name, dep in missing:
                logger.warning("middleware_dependency_disabled", extra={"middleware": name, "depends_on": dep})

    # --- Default registration -------------------------------------------------
    def add_defaults(
        self,
        settings: AurumSettings,
        *,
        tenant_manager: Any | None = None,
        tenant_context_options: TenantContextOptions | None = None,
        token_service: Any | None = None,
        enable_access_log: bool = True,
    ) -> None:
        """Register the default middleware set with sensible ordering.

        Priority guide (higher applied later/outermost):
          1600 logging-context (outermost to bind IDs early)
          1575 resource-cleanup (ensure we always cleanup before other wrappers finish)
          1550 RFC7807 exception wrapper (catch-all) 
          1500 CORS
          1480 GZip
          1460 AdminRouteGuard
          1450 AuthMiddleware
          1400 TenantContext
          1300 ensure-accept-encoding-wildcard
          1200 access-log
          700  vary headers
          680  response headers

        Concurrency and rate limiting wrappers are added at 1100 and 1050 so
        they end up inside the security/logging layers but outside handlers.
        """
        # 1600: Logging context (function-based)
        self.register_http(name="logging_context", priority=1600, enabled=True, handler=logging_context_middleware)

        # 1575: Resource cleanup (function-based)
        self.register_http(name="resource_cleanup", priority=1575, enabled=True, handler=resource_cleanup_middleware)

        # 1550: RFC7807 exception handling (class-based)
        base_url = (
            getattr(getattr(settings, "api", None), "public_base_url", None)
            or getattr(getattr(settings, "api", None), "base_url", "https://api.aurum.com")
        )
        self.register_class(name="rfc7807", priority=1550, enabled=True, cls=RFC7807ExceptionMiddleware, kwargs={"base_url": base_url})

        # 1500: CORS
        def _apply_cors(fastapi_app: FastAPI, app_chain: ASGIApp, _s: AurumSettings) -> ASGIApp:
            from fastapi.middleware.cors import CORSMiddleware

            cors_cfg = getattr(getattr(settings, "api", None), "cors", None)
            strict_mode = bool(getattr(cors_cfg, "strict", not settings.is_development()))
            allowlist = list(getattr(cors_cfg, "allowlist", []) or [])
            allow_origins = allowlist if strict_mode else (allowlist or ["*"])
            allow_credentials = bool(getattr(cors_cfg, "allow_credentials", True))
            allowed_headers = list(getattr(cors_cfg, "allowed_headers", []) or ["Authorization", "Content-Type", "Accept", "X-Requested-With"])
            max_age = int(getattr(cors_cfg, "max_age", 600))

            fastapi_app.add_middleware(
                CORSMiddleware,
                allow_origins=allow_origins,
                allow_credentials=allow_credentials,
                allow_methods=["*"],
                allow_headers=allowed_headers,
                max_age=max_age,
            )
            return app_chain

        self.register(name="cors", priority=1500, enabled=True, applier=_apply_cors)

        # 1480: GZip
        def _apply_gzip(fastapi_app: FastAPI, app_chain: ASGIApp, _s: AurumSettings) -> ASGIApp:
            from fastapi.middleware.gzip import GZipMiddleware

            gzip_min = int(getattr(getattr(settings, "api", None), "gzip_min_bytes", 0) or 0)
            if gzip_min > 0:
                fastapi_app.add_middleware(GZipMiddleware, minimum_size=gzip_min)
            return app_chain

        self.register(name="gzip", priority=1480, enabled=True, applier=_apply_gzip)

        # 1460: Admin route guard
        admin_guard_enabled = bool(getattr(getattr(settings, "api", None), "admin_guard_enabled", False))
        self.register_class(name="admin_guard", priority=1460, enabled=admin_guard_enabled, cls=AdminRouteGuard, kwargs={"enabled": admin_guard_enabled})

        # 1450: Auth middleware
        oidc_config = OIDCConfig.from_settings(settings)
        auth_kwargs = {"config": oidc_config}
        if token_service is not None:
            auth_kwargs["token_service"] = token_service
        self.register_class(name="auth", priority=1450, enabled=True, cls=AuthMiddleware, kwargs=auth_kwargs)

        security_cfg = getattr(getattr(settings, "api", None), "security_headers", None)
        security_enabled = bool(getattr(security_cfg, "enabled", True))
        self.register_class(
            name="security",
            priority=1440,
            enabled=security_enabled,
            cls=SecurityMiddleware,
            kwargs={
                "security_headers": security_enabled,
                "csp_policy": getattr(security_cfg, "csp", None) if security_cfg else None,
                "hsts_policy": getattr(security_cfg, "hsts", None) if security_cfg else None,
            },
        )

        # 1400: Tenant context (optional)
        if tenant_manager is not None and tenant_context_options is not None:
            self.register_class(
                name="tenant_context",
                priority=1400,
                enabled=True,
                cls=TenantContextMiddleware,
                kwargs={"manager": tenant_manager, "options": tenant_context_options},
            )

        # 1300: Ensure gzip wildcard (`*` implies gzip) for tests/clients
        async def _ensure_gzip_wildcard(request, call_next):
            accept_encoding = request.headers.get("accept-encoding")
            if accept_encoding:
                values = {value.strip().lower() for value in accept_encoding.split(",") if value.strip()}
                if "*" in values and "gzip" not in values:
                    headers = MutableHeaders(scope=request.scope)
                    headers["accept-encoding"] = "gzip, " + accept_encoding
            return await call_next(request)

        self.register_http(name="ensure_gzip_wildcard", priority=1300, enabled=True, handler=_ensure_gzip_wildcard)

        # 1200: Access log
        if enable_access_log:
            self.register_http(name="access_log", priority=1200, enabled=True, handler=access_log_middleware)

        # 1100: Concurrency wrapper (optional)
        def _wrap_concurrency(app_chain: ASGIApp, s: AurumSettings) -> ASGIApp:
            try:
                from ..app_offload import build_offload_predicate as _build_offload_predicate
            except Exception:
                _build_offload_predicate = None  # type: ignore[assignment]

            try:
                from ..rate_limiting.concurrency_middleware import create_concurrency_middleware_from_settings
            except Exception:
                # Concurrency unavailable
                return app_chain

            offload_predicate = _build_offload_predicate(s) if _build_offload_predicate else None
            try:
                wrapped = create_concurrency_middleware_from_settings(
                    app_chain,
                    settings=s,
                    offload_predicate=offload_predicate,
                )
            except Exception:
                return app_chain
            return wrapped

        # Only enable when api.concurrency is present (same behavior as legacy)
        enable_concurrency = getattr(getattr(settings, "api", None), "concurrency", None) is not None
        self.register_wrapper(name="concurrency", priority=1100, enabled=enable_concurrency, wrap=_wrap_concurrency)

        # 1050: Rate limiting wrapper (optional)
        def _wrap_rate_limit(app_chain: ASGIApp, _s: AurumSettings) -> ASGIApp:
            try:
                from ..rate_limiting.unified_rate_limiter import get_unified_rate_limiter
                from ..rate_limiting import RateLimitingMiddleware as UnifiedRateLimitingMiddleware
            except Exception:
                return app_chain

            try:
                limiter = get_unified_rate_limiter()
                if limiter is None:
                    return app_chain
                return UnifiedRateLimitingMiddleware(app_chain, limiter)
            except Exception:
                return app_chain

        self.register_wrapper(name="rate_limit", priority=1050, enabled=True, wrap=_wrap_rate_limit)

        # 700: Vary headers (functional)
        async def _vary_headers(request, call_next):
            response = await call_next(request)
            vary = response.headers.get("Vary", "")
            parts = {p.strip() for p in vary.split(",") if p.strip()}
            parts.update({"Accept", "Accept-Encoding"})
            response.headers["Vary"] = ", ".join(sorted(parts))
            return response

        self.register_http(name="vary_headers", priority=700, enabled=True, handler=_vary_headers)

        # 680: Standard response headers
        self.register_http(
            name="response_headers",
            priority=680,
            enabled=True,
            handler=create_response_headers_middleware(settings),
        )

        # Apply optional settings overrides (enable/disable/reorder)
        self._apply_settings_overrides(settings)

    # --- Settings overrides ---------------------------------------------------
    def _apply_settings_overrides(self, settings: AurumSettings) -> None:
        api_cfg = getattr(settings, "api", None)
        if api_cfg is None:
            return

        # Enable/disable lists
        disabled = getattr(api_cfg, "middleware_disabled", None) or []
        enabled = getattr(api_cfg, "middleware_enabled", None) or []
        for name in disabled:
            if isinstance(name, str):
                self.set_enabled(name, False)
        for name in enabled:
            if isinstance(name, str):
                self.set_enabled(name, True)

        # Optional explicit ordering: names from outermost to innermost
        explicit_order = getattr(api_cfg, "middleware_order", None) or []
        if explicit_order and isinstance(explicit_order, (list, tuple)):
            # Assign higher priority numbers to earlier items so they are outermost
            # Keep unspecified items in their relative order after the explicit list
            name_to_entry = self._by_name
            base_priority = 10_000
            step = 10

            applied: set[str] = set()
            priority_cursor = base_priority
            for name in explicit_order:
                entry = name_to_entry.get(str(name))
                if entry is None:
                    continue
                entry.priority = priority_cursor
                priority_cursor -= step
                applied.add(entry.name)

            # Re-pack remaining entries after explicitly ordered ones
            for entry in sorted((e for e in self._entries if e.name not in applied), key=lambda e: e.priority, reverse=True):
                entry.priority = priority_cursor
                priority_cursor -= step


__all__ = ["MiddlewareManager", "RegisteredMiddleware"]
