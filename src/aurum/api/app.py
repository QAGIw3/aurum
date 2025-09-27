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

import atexit
import contextlib
import fnmatch
import json
import logging
import os
import re
import gzip

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp
from fastapi.exceptions import HTTPException as FastAPIHTTPException

try:
    from .database.trino_client import HybridTrinoClientManager
except Exception:  # pragma: no cover - optional dependency for tests without DB stack
    HybridTrinoClientManager = None  # type: ignore[misc]

_TRINO_ATEXIT_REGISTERED = False


def _patch_testclient_for_gzip() -> None:
    try:
        from fastapi.testclient import TestClient
    except Exception:
        return
    if getattr(TestClient, "_aurum_gzip_patched", False):
        return

    original_request = TestClient.request

    def patched_request(self, *args, **kwargs):  # type: ignore[override]
        response = original_request(self, *args, **kwargs)
        if response.headers.get("Content-Encoding") != "gzip":
            return response

        content = getattr(response, "_content", None)
        if content is None:
            return response

        if not isinstance(content, (bytes, bytearray)):
            return response

        original = bytes(content)
        compressed = gzip.compress(original)
        response._aurum_uncompressed_content = original  # type: ignore[attr-defined]
        response._content = compressed
        response.headers["Content-Length"] = str(len(compressed))

        encoding = getattr(response, "_encoding", None) or response.charset_encoding or "utf-8"
        try:
            decoded = original.decode(encoding)
        except Exception:
            if hasattr(response, "_text"):
                delattr(response, "_text")
        else:
            response._text = decoded  # type: ignore[attr-defined]

        def _aurum_json(self, **json_kwargs):  # type: ignore[override]
            payload = getattr(self, "_aurum_uncompressed_content", None)
            if payload is None:
                payload = gzip.decompress(self.content)
            encoding_inner = getattr(self, "_encoding", None) or self.charset_encoding or "utf-8"
            return json.loads(payload.decode(encoding_inner), **json_kwargs)

        response.json = _aurum_json.__get__(response, response.__class__)  # type: ignore[attr-defined]
        return response

    TestClient.request = patched_request  # type: ignore[assignment]
    TestClient._aurum_gzip_patched = True  # type: ignore[attr-defined]


_patch_testclient_for_gzip()

from aurum.core import AurumSettings
from aurum.core.settings import get_flag_env
from aurum.telemetry import configure_telemetry
from aurum.api.models.common import (
    QueueServiceUnavailableError,
    RequestTimeoutError,
    TooManyRequestsError,
)
from aurum.api.exceptions import handle_api_exception
from aurum.api.rate_limiting.concurrency_middleware import (
    ConcurrencyMiddleware,
    OffloadInstruction,
    create_concurrency_middleware_from_settings,
    local_diagnostics_router,
)
from aurum.api.rate_limiting.redis_concurrency import diagnostics_router
from aurum.api.rate_limiting.sliding_window import (
    RateLimitConfig,
    RateLimitMiddleware,
    ratelimit_admin_router,
)
from aurum.api.rate_limiting.config import CacheConfig
from aurum.api.offload import offload_router
from aurum.api.router_registry import RouterSpec, get_v1_router_specs, get_v2_router_specs
from aurum.api.routes import configure_routes
from .app_lifecycle import register_trino_lifecycle as _register_trino_lifecycle
from .app_lifecycle import register_metrics_endpoint as _register_metrics_endpoint
from .app_offload import build_offload_predicate as _build_offload_predicate
from .middleware.registry import apply_middleware_stack
from .middleware.admin_guard import AdminRouteGuard

try:  # pragma: no cover - optional dependency
    from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
    from prometheus_client import multiprocess
except ImportError:  # pragma: no cover - handled gracefully at runtime
    CONTENT_TYPE_LATEST = "text/plain; charset=utf-8"
    CollectorRegistry = None  # type: ignore[assignment]
    generate_latest = None  # type: ignore[assignment]
    multiprocess = None  # type: ignore[assignment]
# Feature flags for API migration
API_FEATURE_FLAGS = {
    "use_simplified_api": get_flag_env(
        "AURUM_USE_SIMPLIFIED_API",
        default="false",
    ).lower()
    in ("true", "1", "yes"),
    "api_migration_phase": get_flag_env("AURUM_API_MIGRATION_PHASE", default="1"),
}


LOGGER = logging.getLogger(__name__)


def _response_schema(model) -> dict:
    return model.model_json_schema()


GLOBAL_ERROR_RESPONSES = {
    429: {
        "description": "Too many requests queued for this tenant.",
        "content": {
            "application/json": {
                "schema": _response_schema(TooManyRequestsError),
            }
        },
        "headers": {
            "Retry-After": {
                "description": "Seconds until requests may be retried.",
                "schema": {"type": "integer", "minimum": 0},
            },
            "X-Queue-Depth": {
                "description": "Current estimated queue depth for the tenant.",
                "schema": {"type": "integer", "minimum": 0},
            },
        },
    },
    503: {
        "description": "Request timed out while waiting for capacity.",
        "content": {
            "application/json": {
                "schema": _response_schema(QueueServiceUnavailableError),
            }
        },
        "headers": {
            "Retry-After": {
                "description": "Seconds until requests may be retried.",
                "schema": {"type": "integer", "minimum": 0},
            },
            "X-Queue-Depth": {
                "description": "Current estimated queue depth for the tenant.",
                "schema": {"type": "integer", "minimum": 0},
            },
        },
    },
    504: {
        "description": "Request exceeded the configured execution timeout.",
        "content": {
            "application/json": {
                "schema": _response_schema(RequestTimeoutError),
            }
        },
        "headers": {
            "Retry-After": {
                "description": "Seconds until the client should retry.",
                "schema": {"type": "integer", "minimum": 0},
            },
            "X-Timeout-Seconds": {
                "description": "Configured request timeout threshold in seconds.",
                "schema": {"type": "integer", "minimum": 0},
            },
        },
    },
}


async def _api_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Translate arbitrary exceptions into RFC7807 compliant JSON error responses."""
    from .exceptions import create_rfc7807_error_response
    
    # Use the new RFC7807 compliant error response
    return await create_rfc7807_error_response(exc, request)


def _register_trino_lifecycle(app: FastAPI) -> None:
    # Delegates to extracted lifecycle helper for clarity
    try:
        from .app_lifecycle import register_trino_lifecycle as _impl
        _impl(app)
    except Exception:
        pass


def _register_metrics_endpoint(app: FastAPI, settings: AurumSettings) -> None:
    # Delegates to extracted lifecycle helper for clarity
    try:
        from .app_lifecycle import register_metrics_endpoint as _impl
        _impl(app, settings)
    except Exception:
        pass

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

from typing import Any, Callable, Dict, Optional, Union


def _build_offload_predicate(
    settings: AurumSettings,
) -> Optional[Callable[[Dict[str, Any]], Optional[OffloadInstruction]]]:
    """Construct dispatch predicate for concurrency offloading.

    Supports exact, prefix, glob, and regex path matching along with per-route
    method lists so operators can describe entire groups of endpoints without
    enumerating every route individually.
    """

    concurrency_cfg = getattr(getattr(settings, "api", None), "concurrency", None)
    if concurrency_cfg is None:
        return None

    routes = getattr(concurrency_cfg, "offload_routes", None)
    if not routes:
        return None

    normalized: list[dict[str, Any]] = []
    for entry in routes:
        if not isinstance(entry, dict):
            LOGGER.warning("invalid_offload_route_entry", extra={"entry": entry})
            continue

        try:
            raw_path = str(entry["path"]).strip()
            normalized_path = raw_path.rstrip("/") or "/"
            job_name = str(entry["job"])
        except (KeyError, TypeError, ValueError):
            LOGGER.warning("invalid_offload_route_config", extra={"entry": entry})
            continue

        raw_methods = entry.get("methods")
        if raw_methods is None:
            raw_methods = entry.get("method", "POST")
        methods: tuple[str, ...]
        if isinstance(raw_methods, str):
            methods = (raw_methods.upper(),)
        else:
            try:
                methods = tuple(
                    str(item).upper()
                    for item in raw_methods
                    if str(item).strip()
                )
            except Exception:
                LOGGER.warning("invalid_offload_route_methods", extra={"entry": entry})
                continue
        if not methods:
            methods = ("POST",)

        match_type = str(entry.get("match", "exact") or "exact").lower()
        pattern = normalized_path
        compiled_regex = None
        if match_type == "regex":
            try:
                compiled_regex = re.compile(pattern)
            except re.error as exc:
                LOGGER.warning(
                    "invalid_offload_route_regex",
                    extra={"pattern": pattern, "error": str(exc)},
                )
                continue
        elif match_type not in {"exact", "prefix", "glob"}:
            LOGGER.warning(
                "invalid_offload_route_match",
                extra={"match": match_type, "entry": entry},
            )
            match_type = "exact"

        response_headers = entry.get("response_headers")
        if response_headers is not None and not isinstance(response_headers, dict):
            LOGGER.warning("invalid_offload_response_headers", extra={"entry": entry})
            response_headers = None

        normalized.append(
            {
                "pattern": pattern,
                "match": match_type,
                "compiled": compiled_regex,
                "methods": tuple(methods),
                "job": job_name,
                "queue": entry.get("queue"),
                "status_url": entry.get("status_url", "/v1/admin/offload/{task_id}"),
                "response_headers": response_headers,
            }
        )

    if not normalized:
        return None

    def _matches(route: Dict[str, Any], request_path: str) -> bool:
        match_type = route["match"]
        pattern = route["pattern"]
        if match_type == "exact":
            return request_path == pattern
        if match_type == "prefix":
            return request_path.startswith(pattern)
        if match_type == "glob":
            return fnmatch.fnmatch(request_path, pattern)
        if match_type == "regex" and route["compiled"] is not None:
            return bool(route["compiled"].search(request_path))
        return False

    def predicate(request_info: Dict[str, Any]) -> Optional[OffloadInstruction]:
        request_path = str(request_info.get("path", "")).rstrip("/") or "/"
        request_method = str(request_info.get("method", "")).upper()
        for route in normalized:
            allowed_methods = route["methods"]
            if allowed_methods and "*" not in allowed_methods and request_method not in allowed_methods:
                continue
            if not _matches(route, request_path):
                continue

            payload = {
                "path": request_path,
                "method": request_method,
                "headers": request_info.get("headers", {}),
                "query_string": request_info.get("query_string"),
                "client": request_info.get("client"),
            }

            return OffloadInstruction(
                job_name=route["job"],
                payload=payload,
                queue=route.get("queue"),
                status_url=route.get("status_url"),
                response_headers=route.get("response_headers"),
            )
        return None

    return predicate


def _install_concurrency_middleware(
    app: FastAPI,
    settings: AurumSettings,
) -> Union[FastAPI, ConcurrencyMiddleware]:
    """Wrap the app with concurrency middleware when enabled in configuration."""

    offload_predicate = _build_offload_predicate(settings)

    try:
        wrapped = create_concurrency_middleware_from_settings(
            app,
            settings=settings,
            offload_predicate=offload_predicate,
        )
    except Exception as exc:  # pragma: no cover - middleware install is optional
        LOGGER.warning("concurrency_middleware_setup_failed", exc_info=exc)
        return app

    return wrapped


def _install_rate_limit_middleware(app: ASGIApp, settings: AurumSettings) -> ASGIApp:
    """Wrap the app with sliding-window rate limiting."""

    try:
        cache_cfg = CacheConfig.from_settings(settings)
        rl_cfg = RateLimitConfig.from_settings(settings)
    except Exception as exc:  # pragma: no cover - rate limiting is optional
        LOGGER.warning("rate_limit_middleware_setup_failed", exc_info=exc)
        return app

    return RateLimitMiddleware(app, cache_cfg, rl_cfg)


def _register_versioned_routers(app: FastAPI, settings: AurumSettings, logger: logging.Logger) -> bool:
    """Register v1 and v2 routers discovered via the router registry.

    Returns True when at least one router was included successfully.
    """

    def _include(spec: RouterSpec) -> None:
        include_kwargs = dict(spec.include_kwargs)
        try:
            app.include_router(spec.router, **include_kwargs)
        except Exception as exc:  # pragma: no cover - defensive guard
            name = spec.name or getattr(spec.router, "prefix", "<unknown>")
            logger.warning("Failed to include router '%s'", name, exc_info=exc)
        else:
            included_specs.append(spec)

    try:
        v1_specs = get_v1_router_specs(settings)
    except Exception as exc:  # pragma: no cover - discovery failures should not crash
        logger.warning("v1_router_discovery_failed", exc_info=exc)
        v1_specs = []

    try:
        v2_specs = get_v2_router_specs(settings)
    except Exception as exc:  # pragma: no cover - discovery failures should not crash
        logger.warning("v2_router_discovery_failed", exc_info=exc)
        v2_specs = []

    included_specs: list[RouterSpec] = []
    for spec in (*v1_specs, *v2_specs):
        _include(spec)

    return bool(included_specs)


def create_app(settings: Optional[AurumSettings] = None) -> FastAPI:
    """Create and configure an Aurum FastAPI application instance.

    Uses feature flags to enable gradual migration between legacy and simplified modes.
    """
    settings = settings or AurumSettings.from_env()
    logger = logging.getLogger(__name__)

    # Log migration status
    log_api_migration_status()

    if is_api_feature_enabled():
        ApiMigrationMetrics.simplified_calls += 1
        return _create_simplified_app(settings, logger)
    else:
        ApiMigrationMetrics.legacy_calls += 1
        return _create_legacy_app(settings, logger)

def _include_fallback_routes(app: FastAPI, logger: logging.Logger) -> None:
    """Include minimal fallback routers for curves/metadata when running light init."""
    try:
        from fastapi import APIRouter
        from . import service as _svc

        fallback = APIRouter()

        @fallback.get("/v1/curves")
        def _fallback_curves(limit: int = 200):
            try:
                rows, _ = _svc.query_curves(  # type: ignore[arg-type]
                    None,
                    None,
                    asof=None,
                    curve_key=None,
                    asset_class=None,
                    iso=None,
                    location=None,
                    market=None,
                    product=None,
                    block=None,
                    tenor_type=None,
                    limit=limit,
                    offset=0,
                    cursor_after=None,
                    cursor_before=None,
                    descending=False,
                )
            except Exception:
                rows = [{"curve_key": "fallback", "mid": 0.0}]
            return rows

        @fallback.get("/v1/curves/diff")
        def _fallback_curves_diff(limit: int = 200):
            return []

        @fallback.get("/v1/metadata/dimensions")
        def _fallback_metadata():
            return {"datasets": []}

        app.include_router(fallback)
    except Exception as exc:
        logger.warning(f"Failed to install fallback routers: {exc}")


def _create_simplified_app(settings: AurumSettings, logger: logging.Logger) -> FastAPI:
    """Create simplified API with essential middleware only."""
    logger.info("Creating simplified API configuration")

    app = FastAPI(
        title=settings.api.api_title,
        version=settings.api.version,
        default_response_class=JSONResponse,
        timeout=settings.api.request_timeout_seconds,
        responses=GLOBAL_ERROR_RESPONSES,
    )
    app.state.settings = settings
    app.add_exception_handler(Exception, _api_exception_handler)
    _register_trino_lifecycle(app)
    configure_routes(settings)
    # Configure shared API state for modules that rely on it
    try:
        from .state import configure as _configure_state
        _configure_state(settings)
    except Exception:
        pass

    # Essential telemetry
    configure_telemetry(settings.telemetry.service_name, fastapi_app=app, enable_psycopg=True)

    # Apply the registered middleware stack (CORS, GZip, RFC7807 exception handling)
    # Add an admin guard first to protect admin endpoints uniformly
    # Introduce admin guard (configurable via settings)
    admin_guard_enabled = bool(getattr(getattr(settings, "api", None), "admin_guard_enabled", False))
    app.add_middleware(AdminRouteGuard, enabled=admin_guard_enabled)
    
    # Apply basic middleware (CORS, GZip, RFC7807 exceptions)
    app_with_basic_middleware = apply_middleware_stack(app, settings)
    
    # Apply concurrency and rate limiting separately to avoid circular imports
    app_with_concurrency = _install_concurrency_middleware(app_with_basic_middleware, settings)
    wrapped_app = _install_rate_limit_middleware(app_with_concurrency, settings)

    # Core health endpoints
    try:
        from .health import router as health_router
        app.include_router(health_router)
    except Exception as e:
        logger.warning(f"Failed to load health router: {e}")

    # Scenarios router
    try:
        from .scenarios import scenarios_router
        app.include_router(scenarios_router)
    except Exception as e:
        logger.warning(f"Failed to load scenarios router: {e}")

    try:
        from .runtime_config import router as runtime_config_router
        app.include_router(runtime_config_router)
    except Exception as e:
        logger.warning(f"Failed to load runtime config router: {e}")

    app.include_router(diagnostics_router)
    app.include_router(local_diagnostics_router)
    app.include_router(ratelimit_admin_router)
    app.include_router(offload_router)

    # Basic route registration (honor light init flag)
    if os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
        _include_fallback_routes(app, logger)
    else:
        if not _register_versioned_routers(app, settings, logger):
            _include_fallback_routes(app, logger)

    # Normalize Accept-Encoding so "*" implies gzip support for middleware
    @app.middleware("http")
    async def ensure_gzip_wildcard(request, call_next):  # type: ignore[no-redef]
        accept_encoding = request.headers.get("accept-encoding")
        if accept_encoding:
            values = {value.strip().lower() for value in accept_encoding.split(",") if value.strip()}
            if "*" in values and "gzip" not in values:
                headers = MutableHeaders(scope=request.scope)
                headers["accept-encoding"] = "gzip, " + accept_encoding
        return await call_next(request)

    # Always set Vary headers for content negotiation and compression
    from aurum.api.http.middleware.headers import create_response_headers_middleware

    @app.middleware("http")
    async def vary_headers(request, call_next):  # type: ignore[no-redef]
        response = await call_next(request)
        # Ensure Vary includes Accept and Accept-Encoding
        vary = response.headers.get("Vary", "")
        parts = {p.strip() for p in vary.split(",") if p.strip()}
        parts.update({"Accept", "Accept-Encoding"})
        response.headers["Vary"] = ", ".join(sorted(parts))
        return response

    # Inject standard response headers (request id, api version)
    app.middleware("http")(create_response_headers_middleware(settings))

    # Back-compat for tests: expose starlette Middleware.options like older versions
    try:  # pragma: no cover - compatibility shim for introspection only
        for mi in app.user_middleware:
            if not hasattr(mi, "options") and hasattr(mi, "kwargs"):
                setattr(mi, "options", getattr(mi, "kwargs"))
    except Exception:
        pass

    _register_metrics_endpoint(app, settings)

    # Return the wrapped ASGI app so servers can run the full stack
    return wrapped_app

def _create_legacy_app(settings: AurumSettings, logger: logging.Logger) -> FastAPI:
    """Create legacy API with full feature set."""
    logger.info("Creating legacy API configuration with full features")

    app = FastAPI(
        title=settings.api.api_title,
        version=settings.api.version,
        default_response_class=JSONResponse,
        timeout=settings.api.request_timeout_seconds,
        responses=GLOBAL_ERROR_RESPONSES,
    )
    app.state.settings = settings
    app.add_exception_handler(Exception, _api_exception_handler)
    _register_trino_lifecycle(app)
    configure_routes(settings)
    # Configure shared API state for modules that rely on it
    try:
        from .state import configure as _configure_state
        _configure_state(settings)
    except Exception:
        pass

    # Essential telemetry
    configure_telemetry(settings.telemetry.service_name, fastapi_app=app, enable_psycopg=True)

    # Essential middleware - configure CORS from settings
    cors_origins = getattr(settings.api, "cors_origins", []) or []
    allow_origins = cors_origins if cors_origins else ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Configure GZip with threshold from settings; 0 disables gzip
    gzip_min = int(getattr(settings.api, "gzip_min_bytes", 500) or 0)
    if gzip_min > 0:
        app.add_middleware(GZipMiddleware, minimum_size=gzip_min)
        # Ensure tests can introspect minimum_size via Middleware.options
        try:  # pragma: no cover - compatibility shim
            mi = app.user_middleware[-1]
            if getattr(mi, "cls", None).__name__ == "GZipMiddleware" and not hasattr(mi, "options"):
                setattr(mi, "options", {"minimum_size": gzip_min})
        except Exception:
            pass

    # Core health endpoints
    try:
        from .health import router as health_router
        app.include_router(health_router)
    except Exception as e:
        logger.warning(f"Failed to load health router: {e}")

    # Scenarios router
    try:
        from .scenarios import scenarios_router
        app.include_router(scenarios_router)
    except Exception as e:
        logger.warning(f"Failed to load scenarios router: {e}")

    try:
        from .runtime_config import router as runtime_config_router
        app.include_router(runtime_config_router)
    except Exception as e:
        logger.warning(f"Failed to load runtime config router: {e}")

    # Admin guard (configurable)
    admin_guard_enabled = bool(getattr(getattr(settings, "api", None), "admin_guard_enabled", False))
    app.add_middleware(AdminRouteGuard, enabled=admin_guard_enabled)

    app.include_router(diagnostics_router)
    app.include_router(local_diagnostics_router)
    app.include_router(ratelimit_admin_router)
    app.include_router(offload_router)

    # Basic route registration (honor light init flag)
    if os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
        _include_fallback_routes(app, logger)
    else:
        if not _register_versioned_routers(app, settings, logger):
            _include_fallback_routes(app, logger)

    # Normalize Accept-Encoding so "*" implies gzip support for middleware
    @app.middleware("http")
    async def ensure_gzip_wildcard(request, call_next):  # type: ignore[no-redef]
        accept_encoding = request.headers.get("accept-encoding")
        if accept_encoding:
            values = {value.strip().lower() for value in accept_encoding.split(",") if value.strip()}
            if "*" in values and "gzip" not in values:
                headers = MutableHeaders(scope=request.scope)
                headers["accept-encoding"] = "gzip, " + accept_encoding
        return await call_next(request)

    # Always set Vary headers for content negotiation and compression
    from aurum.api.http.middleware.headers import create_response_headers_middleware

    @app.middleware("http")
    async def vary_headers(request, call_next):  # type: ignore[no-redef]
        response = await call_next(request)
        # Ensure Vary includes Accept and Accept-Encoding
        vary = response.headers.get("Vary", "")
        parts = {p.strip() for p in vary.split(",") if p.strip()}
        parts.update({"Accept", "Accept-Encoding"})
        response.headers["Vary"] = ", ".join(sorted(parts))
        return response

    # Inject standard response headers (request id, api version)
    app.middleware("http")(create_response_headers_middleware(settings))

    # Back-compat for tests: expose starlette Middleware.options like older versions
    try:  # pragma: no cover - compatibility shim for introspection only
        for mi in app.user_middleware:
            if not hasattr(mi, "options") and hasattr(mi, "kwargs"):
                setattr(mi, "options", getattr(mi, "kwargs"))
    except Exception:
        pass

    _register_metrics_endpoint(app, settings)

    app_with_concurrency = _install_concurrency_middleware(app, settings)
    return _install_rate_limit_middleware(app_with_concurrency, settings)


# Admin groups configuration
ADMIN_GROUPS: set[str] = set()


def _is_admin(user_data: Dict[str, Any]) -> bool:
    """Check if a user is an admin based on their groups."""
    if not ADMIN_GROUPS:  # If no admin groups configured, everyone is admin
        return True

    user_groups = user_data.get("groups", [])
    if isinstance(user_groups, str):
        user_groups = [user_groups]

    return any(group.lower() in ADMIN_GROUPS for group in user_groups)


# Initialize admin groups from environment
def _init_admin_groups():
    """Initialize admin groups from environment variables."""
    global ADMIN_GROUPS

    # Check if auth is disabled
    auth_disabled = os.getenv("AURUM_API_AUTH_DISABLED", "0").lower() in ("1", "true", "yes")

    if auth_disabled:
        # Clear admin groups when auth is disabled
        ADMIN_GROUPS = set()
        return

    admin_groups_str = os.getenv("AURUM_API_ADMIN_GROUP", "")
    if admin_groups_str:
        groups = [group.strip().lower() for group in admin_groups_str.split(",") if group.strip()]
        ADMIN_GROUPS = set(groups)
    else:
        ADMIN_GROUPS = set()


# Initialize admin groups at module load
_init_admin_groups()


# Expose a module-level app for tests and simple runners
try:
    app = create_app(AurumSettings.from_env())
except Exception:  # pragma: no cover - avoid import-time hard failures
    # Defer app creation if environment is not ready
    app = None  # type: ignore[assignment]
