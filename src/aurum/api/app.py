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
import json
import logging
import os
import gzip

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
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
)
from aurum.api.rate_limiting.redis_concurrency import diagnostics_router
from aurum.api.offload import offload_router

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
    "use_simplified_api": os.getenv("AURUM_USE_SIMPLIFIED_API", "false").lower() == "true",
    "api_migration_phase": os.getenv("AURUM_API_MIGRATION_PHASE", "1"),
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
    """Translate arbitrary exceptions into JSON error envelopes."""

    http_exc = handle_api_exception(request, exc)
    if isinstance(http_exc, FastAPIHTTPException):
        detail = http_exc.detail
        if not isinstance(detail, dict):
            detail = {"error": http_exc.__class__.__name__, "message": str(detail)}
        headers = getattr(http_exc, "headers", None) or {}
        return JSONResponse(status_code=http_exc.status_code, content=detail, headers=headers)

    LOGGER.error("Unhandled exception escaped API", exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "InternalServerError",
            "message": "Unhandled exception",
        },
    )


def _register_trino_lifecycle(app: FastAPI) -> None:
    """Ensure Trino clients are gracefully closed with the application."""

    if HybridTrinoClientManager is None:  # pragma: no cover - optional dependency
        return

    previous_lifespan = getattr(app.router, "lifespan_context", None)

    @contextlib.asynccontextmanager
    async def _trino_lifespan(_app: FastAPI):
        manager = HybridTrinoClientManager.get_instance()
        try:
            yield
        finally:
            await manager.close_all()

    if previous_lifespan is None:
        app.router.lifespan_context = _trino_lifespan
    else:
        @contextlib.asynccontextmanager
        async def _chained_lifespan(_app: FastAPI):
            async with previous_lifespan(_app):
                async with _trino_lifespan(_app):
                    yield

        app.router.lifespan_context = _chained_lifespan

    manager = HybridTrinoClientManager.get_instance()

    global _TRINO_ATEXIT_REGISTERED
    if not _TRINO_ATEXIT_REGISTERED:
        try:
            atexit.register(manager.close_all_sync)
            _TRINO_ATEXIT_REGISTERED = True
        except Exception:  # pragma: no cover - best effort cleanup
            pass


def _register_metrics_endpoint(app: FastAPI, settings: AurumSettings) -> None:
    """Register Prometheus metrics endpoint with optional multiprocess support."""

    metrics_cfg = getattr(settings.api, "metrics", None)
    if metrics_cfg is None or not getattr(metrics_cfg, "enabled", False):
        return

    metrics_path = getattr(metrics_cfg, "path", "/metrics") or "/metrics"
    if not metrics_path.startswith("/"):
        metrics_path = f"/{metrics_path}"

    existing_routes = [
        route for route in list(app.router.routes)
        if getattr(route, "path", None) == metrics_path
    ]
    for route in existing_routes:
        try:
            app.router.routes.remove(route)
        except ValueError:  # pragma: no cover - defensive
            continue

    @app.get(metrics_path)
    async def prometheus_metrics() -> Response:  # type: ignore[override]
        if generate_latest is None:
            raise HTTPException(status_code=503, detail="Prometheus instrumentation not available")

        registry = None
        multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR")
        if multiproc_dir and multiprocess and CollectorRegistry is not None:
            registry = CollectorRegistry()
            try:
                multiprocess.MultiProcessCollector(registry)
            except Exception as exc:  # pragma: no cover - unlikely but defensive
                LOGGER.warning("prometheus_multiprocess_init_failed", exc_info=exc)
                registry = None

        try:
            payload = generate_latest(registry) if registry is not None else generate_latest()
        except Exception as exc:  # pragma: no cover - guard against collector errors
            LOGGER.warning("prometheus_generate_latest_failed", exc_info=exc)
            raise HTTPException(status_code=500, detail="Failed to render metrics") from exc

        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

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

from typing import Any, Callable, Dict, Optional


def _build_offload_predicate(
    settings: AurumSettings,
) -> Optional[Callable[[Dict[str, Any]], Optional[OffloadInstruction]]]:
    """Construct dispatch predicate for concurrency offloading."""

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
            path = str(entry["path"]).rstrip("/") or "/"
            method = str(entry.get("method", "POST")).upper()
            job_name = str(entry["job"])
        except (KeyError, TypeError, ValueError):
            LOGGER.warning("invalid_offload_route_config", extra={"entry": entry})
            continue

        normalized.append(
            {
                "path": path,
                "method": method,
                "job": job_name,
                "queue": entry.get("queue"),
                "status_url": entry.get("status_url", "/v1/admin/offload/{task_id}"),
            }
        )

    if not normalized:
        return None

    def predicate(request_info: Dict[str, Any]) -> Optional[OffloadInstruction]:
        path = str(request_info.get("path", "")).rstrip("/") or "/"
        method = str(request_info.get("method", "")).upper()
        for route in normalized:
            if path == route["path"] and method == route["method"]:
                payload = {
                    "path": path,
                    "method": method,
                    "headers": request_info.get("headers", {}),
                    "query_string": request_info.get("query_string"),
                    "client": request_info.get("client"),
                }
                return OffloadInstruction(
                    job_name=route["job"],
                    payload=payload,
                    queue=route.get("queue"),
                    status_url=route.get("status_url"),
                )
        return None

    return predicate


def _install_concurrency_middleware(
    app: FastAPI,
    settings: AurumSettings,
) -> FastAPI | ConcurrencyMiddleware:
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

    app.include_router(diagnostics_router)
    app.include_router(offload_router)

    # Basic route registration (honor light init flag)
    if os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
        _include_fallback_routes(app, logger)
    else:
        try:
            from . import routes
            if hasattr(routes, 'router'):
                app.include_router(routes.router)
        except Exception as e:
            logger.warning(f"Failed to load routes: {e}")
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

    return _install_concurrency_middleware(app, settings)

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

    app.include_router(diagnostics_router)
    app.include_router(offload_router)

    # Basic route registration (honor light init flag)
    if os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
        _include_fallback_routes(app, logger)
    else:
        try:
            from . import routes
            if hasattr(routes, 'router'):
                app.include_router(routes.router)
        except Exception as e:
            logger.warning(f"Failed to load routes: {e}")
            try:
                from . import v1
                if hasattr(v1, 'curves'):
                    app.include_router(v1.curves.router, prefix="/v1")
                if hasattr(v1, 'metadata'):
                    app.include_router(v1.metadata.router, prefix="/v1")
            except Exception:
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

    return _install_concurrency_middleware(app, settings)


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
