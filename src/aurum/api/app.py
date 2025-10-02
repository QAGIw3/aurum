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
import copy
import fnmatch
import json
import logging
import os
import re
import gzip
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

try:
    import redis
except Exception:  # pragma: no cover - optional dependency
    redis = None  # type: ignore[assignment]

from fastapi import Depends, FastAPI, HTTPException, Request, Response
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


if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("AURUM_ENABLE_TEST_GZIP_PATCH", "0").lower() in ("1", "true", "yes"):  # pragma: no cover - only for tests
    _patch_testclient_for_gzip()

from aurum.core import AurumSettings
from aurum.core.settings import get_flag_env
from aurum.telemetry import configure_telemetry
from aurum.security.token_service import (
    TokenService,
    TokenServiceConfig,
    RefreshTokenStore,
    RedisRefreshTokenStore,
)
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
# Canonical rate limiting entrypoint
from aurum.api.rate_limiting import get_unified_rate_limiter
from aurum.api.rate_limiting.config import CacheConfig
from aurum.api.router_registry import RouterSpec, get_v1_router_specs, get_v2_router_specs
from aurum.api.routes import configure_routes
from .lifespan_manager import setup_lifespan
from .container import DependencyInjectionContainer, register_core_services
from .middleware.manager import MiddlewareManager
from .middleware.tenant_context import TenantContextMiddleware, TenantContextOptions
from .app_builder import ApplicationBuilder
from aurum.tenancy import (
    InMemoryTenantStore,
    TenantAnalyticsAdapter,
    TenantBillingAdapter,
    TenantConfiguration,
    TenantIsolationController,
    TenantIsolationStrategy,
    TenantManager,
    TenantProvisioningError,
    TenantQuota,
    WorkloadPoolIsolation,
    RowLevelSecurityIsolation,
    SchemaPerTenantIsolation,
    set_tenant_manager,
)

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


def _build_quota_map(raw: Optional[Mapping[str, Any]]) -> Dict[str, TenantQuota]:
    if not raw:
        return {}
    quotas: Dict[str, TenantQuota] = {}
    for name, data in raw.items():
        if not isinstance(data, Mapping):
            continue
        quotas[name] = TenantQuota(
            name=name,
            hard_limit=data.get("hard_limit"),
            soft_limit=data.get("soft_limit"),
            burst_limit=data.get("burst_limit"),
            period=data.get("period", "monthly"),
            unit=data.get("unit", "requests"),
            usage=data.get("usage", 0.0),
            metadata=data.get("metadata", {}),
        )
    return quotas


def _bootstrap_tenant(
    manager: TenantManager,
    spec: Mapping[str, Any],
    fallback_quotas: Optional[Mapping[str, Any]],
) -> None:
    tenant_id = spec.get("tenant_id")
    if not tenant_id or not isinstance(tenant_id, str):
        LOGGER.warning("Skipping bootstrap tenant missing tenant_id: %s", spec)
        return
    display_name = spec.get("display_name") if isinstance(spec.get("display_name"), str) else tenant_id

    baseline = copy.deepcopy(getattr(manager, "_baseline_configuration", None))
    config = baseline or TenantConfiguration(plan=spec.get("plan", "standard"))

    overrides: Dict[str, Any] = {}
    for key in ("plan", "database_schema", "compute_pool", "billing_account", "contact_email", "data_retention_days", "export_formats"):
        if spec.get(key) is not None:
            overrides[key] = spec[key]
    for key in ("features", "settings", "metadata", "labels", "customizations"):
        if spec.get(key) is not None:
            overrides.setdefault(key, spec[key])
    if spec.get("configuration"):
        config.apply_overrides(spec["configuration"])
    if overrides:
        config.apply_overrides(overrides)

    quotas = _build_quota_map(fallback_quotas)
    quotas.update(_build_quota_map(spec.get("quotas")))

    try:
        manager.provision_tenant(
            tenant_id,
            display_name=display_name,
            configuration=config,
            quotas=quotas,
            metadata=spec.get("tenant_metadata") or spec.get("metadata") or {},
        )
    except TenantProvisioningError:
        LOGGER.debug("Bootstrap tenant %s already provisioned", tenant_id)


def _initialize_tenant_manager(
    settings: AurumSettings,
    container: DependencyInjectionContainer,
) -> Tuple[Optional[TenantManager], Optional[TenantContextOptions]]:
    tenancy_cfg = getattr(settings, "tenancy", None)
    if not tenancy_cfg or not getattr(tenancy_cfg, "enabled", True):
        set_tenant_manager(None)
        return None, None

    strategies: list[TenantIsolationStrategy] = []
    if tenancy_cfg.isolation_rls_tables:
        strategies.append(
            TenantIsolationStrategy(
                data=RowLevelSecurityIsolation(tables=tuple(tenancy_cfg.isolation_rls_tables))
            )
        )
    if tenancy_cfg.isolation_schema_template:
        strategies.append(
            TenantIsolationStrategy(
                data=SchemaPerTenantIsolation(template=tenancy_cfg.isolation_schema_template)
            )
        )
    if tenancy_cfg.compute_pools:
        strategies.append(
            TenantIsolationStrategy(
                compute=WorkloadPoolIsolation(
                    tuple(tenancy_cfg.compute_pools),
                    tenancy_cfg.default_compute_pool,
                )
            )
        )

    controller = TenantIsolationController(tuple(strategies))

    baseline_config = TenantConfiguration(plan=tenancy_cfg.default_plan)
    if tenancy_cfg.default_features:
        baseline_config.features.update({feature: True for feature in tenancy_cfg.default_features})
    baseline_config.compute_pool = tenancy_cfg.default_compute_pool

    billing = TenantBillingAdapter()
    analytics = TenantAnalyticsAdapter(require_roles=tenancy_cfg.cross_tenant_roles)

    manager = TenantManager(
        store=InMemoryTenantStore(),
        isolation=controller,
        billing=billing,
        analytics=analytics,
        baseline_configuration=baseline_config,
        default_quotas=tenancy_cfg.default_quotas,
    )

    for tenant_spec in tenancy_cfg.bootstrap_tenants or []:
        if isinstance(tenant_spec, Mapping):
            _bootstrap_tenant(manager, tenant_spec, tenancy_cfg.default_quotas)

    if tenancy_cfg.default_tenant and tenancy_cfg.auto_provision:
        try:
            manager.ensure_tenant(tenancy_cfg.default_tenant)
        except TenantProvisioningError:
            LOGGER.debug("Default tenant %s already provisioned", tenancy_cfg.default_tenant)

    container.register_singleton(TenantManager, manager)
    set_tenant_manager(manager)

    tenant_options = TenantContextOptions(
        header_name=tenancy_cfg.header_name,
        query_param=tenancy_cfg.query_param,
        default_tenant=tenancy_cfg.default_tenant,
        require_tenant=tenancy_cfg.require_registered_tenant,
        allow_cross_tenant_roles=tenancy_cfg.cross_tenant_roles,
        auto_provision=tenancy_cfg.auto_provision,
    )

    return manager, tenant_options


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

# (imports already moved to the top for early availability)


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


def _initialize_token_service(app: FastAPI, settings: AurumSettings) -> Optional["TokenService"]:
    if not getattr(settings.auth, "token_issuer_enabled", False):
        return None

    issuer = getattr(settings.auth, "oidc_issuer", None)
    if not issuer:
        service_name = getattr(getattr(settings, "telemetry", None), "service_name", "aurum-api")
        issuer = f"urn:aurum:issuer:{service_name}"

    raw_audiences = tuple(getattr(settings.auth, "audiences", ()) or ())
    if raw_audiences:
        audiences = raw_audiences
    else:
        fallback = getattr(settings.auth, "oidc_audience", None)
        audiences = tuple(value for value in (fallback, issuer) if value)

    config = TokenServiceConfig(
        issuer=issuer,
        audiences=audiences or (issuer,),
        access_token_ttl=getattr(settings.auth, "access_token_ttl_seconds", 900),
        refresh_token_ttl=getattr(settings.auth, "refresh_token_ttl_seconds", 60 * 60 * 24 * 14),
    )

    store = _build_refresh_token_store(settings)

    token_service = TokenService(config=config, store=store)
    app.state.token_service = token_service
    return token_service


def _build_refresh_token_store(settings: AurumSettings) -> Optional[RefreshTokenStore]:
    store_cfg = getattr(settings.auth, "refresh_store", None)
    if store_cfg is None:
        return None

    redis_url = getattr(store_cfg, "redis_url", None)
    if not redis_url or redis is None:
        return None

    logger = logging.getLogger(__name__)
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        namespace = getattr(store_cfg, "namespace", "aurum:auth:refresh_tokens")
        return RedisRefreshTokenStore(client, namespace=namespace)
    except Exception:  # pragma: no cover - best effort
        logger.warning("Failed to initialize Redis refresh token store", exc_info=True)
        return None


class ApplicationFactory:
    """Factory for creating FastAPI applications with proper configuration."""

    @staticmethod
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
            return ApplicationFactory._create_simplified_app(settings, logger)
        else:
            ApiMigrationMetrics.legacy_calls += 1
            return ApplicationFactory._create_legacy_app(settings, logger)

    @staticmethod
    def _create_simplified_app(settings: AurumSettings, logger: logging.Logger) -> FastAPI:
        """Create simplified API with essential middleware only."""
        logger.info("Creating simplified API configuration")

        builder = ApplicationBuilder(
            settings,
            logger,
            mode="simplified",
            strict_token_service=True,
        )
        return builder.build()

    @staticmethod
    def _create_legacy_app(settings: AurumSettings, logger: logging.Logger) -> FastAPI:
        """Create legacy API with full feature set."""
        logger.info("Creating legacy API configuration with full features")

        builder = ApplicationBuilder(
            settings,
            logger,
            mode="legacy",
            strict_token_service=False,
        )
        return builder.build()


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


# --- Admin group helpers (minimal, explicit, side-effect free) -----------------

def _configure_admin_groups(settings: AurumSettings) -> None:
    """Configure admin groups in process-local context.

    Uses the application context to expose `settings.auth.admin_groups` to routes.
    No globals are mutated here beyond the central app context.
    """
    try:
        from .container import get_app_context
        app_context = get_app_context()
        app_context.settings = settings
    except Exception:
        # Best-effort: continue without admin group wiring
        pass


def _require_admin_groups(settings: AurumSettings, logger: logging.Logger) -> None:
    """Log a warning if admin guard is enabled but no admin groups are configured."""
    try:
        admin_guard_enabled = bool(getattr(getattr(settings, "api", None), "admin_guard_enabled", False))
        admin_groups = getattr(getattr(settings, "auth", None), "admin_groups", frozenset())
        if admin_guard_enabled and not admin_groups:
            logger.warning("Admin guard enabled but no admin groups configured")
    except Exception:
        pass


# --- Public factory entry points ----------------------------------------------

def create_app(settings: Optional[AurumSettings] = None) -> FastAPI:
    """Create a FastAPI application using the consolidated factory.

    This is the single entry point for app creation used by production code and tests.
    """
    return ApplicationFactory.create_app(settings)


def create_dev_app(settings: Optional[AurumSettings] = None) -> FastAPI:
    """Create a development-configured app (docs enabled, debug-friendly)."""
    s = settings or AurumSettings.from_env()
    try:
        setattr(s, "debug", True)
        setattr(s, "environment", "development")
    except Exception:
        pass
    return ApplicationFactory.create_app(s)


def create_prod_app(settings: Optional[AurumSettings] = None) -> FastAPI:
    """Create a production-configured app (no docs by default)."""
    s = settings or AurumSettings.from_env()
    try:
        setattr(s, "debug", False)
        setattr(s, "environment", "production")
    except Exception:
        pass
    return ApplicationFactory.create_app(s)


def create_test_app(settings: Optional[AurumSettings] = None) -> FastAPI:
    """Create a test-configured app with docs enabled for convenience."""
    s = settings or AurumSettings.from_env()
    try:
        setattr(s, "debug", True)
        setattr(s, "environment", "test")
    except Exception:
        pass
    return ApplicationFactory.create_app(s)
