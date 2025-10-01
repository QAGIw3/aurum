"""Shared helpers for external API handlers."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar

from fastapi import HTTPException, Request, Response
from pydantic import ValidationError

from typing import TYPE_CHECKING
if TYPE_CHECKING:  # Avoid heavy imports at runtime; only needed for typing
    from ..cache.cache import CacheManager
    from ..models import Meta
if TYPE_CHECKING:
    from ..rate_limiting import QuotaTier, RateLimitManager
from ...observability.metrics import (
    EXTERNAL_API_LATENCY,
    EXTERNAL_API_REQUEST_COUNTER,
    EXTERNAL_CACHE_HIT_COUNTER,
    EXTERNAL_CACHE_MISS_COUNTER,
    EXTERNAL_DAO_QUERY_COUNTER,
    EXTERNAL_DAO_LATENCY,
)

CacheFetcher = Callable[[], Awaitable[Any]]
CacheHook = Callable[[], None]
TTLProvider = Optional[Callable[[Any], Optional[int]]]
ResponseT = TypeVar("ResponseT")
ResponseBuilder = Callable[[Any, "Meta"], ResponseT]


@dataclass
class ExternalRequestContext:
    """Context captured while preparing an external API request."""

    request_id: str
    endpoint: str
    start_time: float
    cache_hit_hook: CacheHook
    cache_miss_hook: CacheHook


async def enforce_rate_limit(
    *,
    rate_limit_mgr: "RateLimitManager",
    response: Response,
    identifier: str,
    tier: "QuotaTier",
    endpoint: str,
    request_tokens: int = 1,
    request_id: Optional[str] = None,
) -> None:
    """Check and enforce rate limits for an external endpoint."""
    rate_limit_result = await rate_limit_mgr.check_rate_limit(
        identifier=identifier,
        tier=tier,
        endpoint=endpoint,
        request_tokens=request_tokens,
    )

    if not rate_limit_result.allowed:
        retry_after = int(rate_limit_result.retry_after or 0)
        raise HTTPException(
            status_code=429,
            detail=create_error_response(
                429,
                "Too many requests",
                request_id=request_id,
                code="RATE_LIMIT_EXCEEDED",
                context={"retry_after_seconds": retry_after} if retry_after else None,
                retry_after=retry_after or None,
            ),
        )

    for key, value in rate_limit_result.to_headers().items():
        response.headers[key] = value


async def prepare_external_context(
    request: Request,
    *,
    response: Response,
    principal: Dict[str, Any],
    rate_limit_mgr: "RateLimitManager",
    endpoint: str,
    identifier_suffix: str,
    request_tokens: int = 1,
    request_id_header: str = "x-request-id",
) -> ExternalRequestContext:
    """Prepare shared context and enforce rate limits for an external endpoint."""

    request_id = str(request.headers.get(request_id_header, "unknown"))
    identifier = f"external:{identifier_suffix}:{principal.get('sub', 'anonymous')}"
    from ..rate_limiting import QuotaTier as _QuotaTier
    tier = _QuotaTier(principal.get("tier", "free").lower())

    await enforce_rate_limit(
        rate_limit_mgr=rate_limit_mgr,
        response=response,
        identifier=identifier,
        tier=tier,
        endpoint=endpoint,
        request_tokens=request_tokens,
        request_id=request_id,
    )

    start_time = perf_counter()
    cache_hit_hook, cache_miss_hook = cache_metrics_hooks(endpoint)

    return ExternalRequestContext(
        request_id=request_id,
        endpoint=endpoint,
        start_time=start_time,
        cache_hit_hook=cache_hit_hook,
        cache_miss_hook=cache_miss_hook,
    )


async def cached_fetch(
    cache_mgr: "CacheManager",
    *,
    cache_key: str,
    fetcher: CacheFetcher,
    ttl_seconds: Optional[int] = None,
    ttl_provider: TTLProvider = None,
    namespace: Optional[str] = None,
    on_cache_hit: Optional[CacheHook] = None,
    on_cache_miss: Optional[CacheHook] = None,
) -> Tuple[Any, bool]:
    """Retrieve a cached value or populate it using the provided fetcher."""
    cached_value = await cache_mgr.get_cache_entry(cache_key, namespace=namespace)
    if cached_value is not None:
        if on_cache_hit:
            on_cache_hit()
        return cached_value, True

    if on_cache_miss:
        on_cache_miss()

    value = await fetcher()
    ttl_to_use = ttl_seconds
    if ttl_provider is not None:
        ttl_override = ttl_provider(value)
        ttl_to_use = ttl_override if ttl_override is not None else ttl_seconds

    await cache_mgr.set_cache_entry(
        cache_key,
        value,
        ttl_seconds=ttl_to_use,
        namespace=namespace,
    )
    return value, False


def record_request_metrics(context: ExternalRequestContext, *, status: str = "200") -> None:
    """Record shared success metrics for an external endpoint."""

    if EXTERNAL_API_REQUEST_COUNTER:
        EXTERNAL_API_REQUEST_COUNTER.labels(endpoint=context.endpoint, status=status).inc()
    if EXTERNAL_API_LATENCY:
        EXTERNAL_API_LATENCY.labels(endpoint=context.endpoint).observe(perf_counter() - context.start_time)


def build_response_meta(context: ExternalRequestContext):
    """Construct the standard response meta payload."""
    # Deferred import to avoid heavy model dependency during import-time
    from ..http.responses import create_meta
    return create_meta(context.request_id, elapsed_milliseconds(context.start_time))


async def cached_endpoint_response(
    *,
    cache_mgr: "CacheManager",
    cache_key: str,
    fetcher: CacheFetcher,
    ttl_seconds: Optional[int],
    context: ExternalRequestContext,
    response_builder: ResponseBuilder,
    namespace: Optional[str] = None,
    on_cache_hit: Optional[CacheHook] = None,
    on_cache_miss: Optional[CacheHook] = None,
) -> ResponseT:
    """Resolve a cached API payload, record metrics, and build a response."""

    payload, _ = await cached_fetch(
        cache_mgr,
        cache_key=cache_key,
        fetcher=fetcher,
        ttl_seconds=ttl_seconds,
        ttl_provider=None,
        namespace=namespace,
        on_cache_hit=on_cache_hit,
        on_cache_miss=on_cache_miss,
    )

    record_request_metrics(context)
    meta = build_response_meta(context)
    return response_builder(payload, meta)


def build_external_cache_key(
    route: str,
    *,
    version: str = "v1",
    components: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate a stable cache key for external handlers."""
    payload = {k: v for k, v in (components or {}).items() if v is not None}
    return CacheManager.build_cache_key(
        f"external:{route}",
        payload or None,
        version=version,
    )


def cache_metrics_hooks(endpoint: str) -> Tuple[CacheHook, CacheHook]:
    """Return cache hit/miss hooks wired to the shared metrics."""

    def record_hit() -> None:
        if EXTERNAL_CACHE_HIT_COUNTER:
            EXTERNAL_CACHE_HIT_COUNTER.labels(endpoint=endpoint).inc()

    def record_miss() -> None:
        if EXTERNAL_CACHE_MISS_COUNTER:
            EXTERNAL_CACHE_MISS_COUNTER.labels(endpoint=endpoint).inc()

    return record_hit, record_miss


def elapsed_milliseconds(start_time: float) -> int:
    """Return elapsed milliseconds from a ``perf_counter`` start time."""
    return int((perf_counter() - start_time) * 1000)


async def dao_call_with_metrics(operation: str, fetcher: CacheFetcher) -> Any:
    """Execute a DAO call while recording shared DAO metrics."""

    start = perf_counter()
    if EXTERNAL_DAO_QUERY_COUNTER:
        EXTERNAL_DAO_QUERY_COUNTER.labels(operation=operation, status="start").inc()

    try:
        result = await fetcher()
        if EXTERNAL_DAO_QUERY_COUNTER:
            EXTERNAL_DAO_QUERY_COUNTER.labels(operation=operation, status="success").inc()
        if EXTERNAL_DAO_LATENCY:
            EXTERNAL_DAO_LATENCY.labels(operation=operation).observe(perf_counter() - start)
        return result
    except Exception:
        if EXTERNAL_DAO_QUERY_COUNTER:
            EXTERNAL_DAO_QUERY_COUNTER.labels(operation=operation, status="failure").inc()
        if EXTERNAL_DAO_LATENCY:
            EXTERNAL_DAO_LATENCY.labels(operation=operation).observe(perf_counter() - start)
        raise


def http_error(
    status_code: int,
    message: str,
    *,
    request_id: str,
    code: str,
    context: Optional[Dict[str, Any]] = None,
    field_errors: Optional[List[Dict[str, Any]]] = None,
    retry_after: Optional[int] = None,
) -> HTTPException:
    """Create a FastAPI HTTPException with the shared error envelope."""

    from ..http.responses import create_error_response
    return HTTPException(
        status_code=status_code,
        detail=create_error_response(
            status_code,
            message,
            request_id=request_id,
            code=code,
            context=context,
            field_errors=field_errors,
            retry_after=retry_after,
        ),
    )


def validation_error(
    exc: ValidationError,
    *,
    request_id: str,
    message: str = "Invalid request parameters",
    code: str = "VALIDATION_ERROR",
) -> HTTPException:
    """Normalize validation errors into the shared error response format."""

    field_errors = [
        {
            "field": (error.get("loc") or [None])[0],
            "message": error.get("msg"),
            "value": error.get("input"),
        }
        for error in exc.errors()
    ]

    return http_error(
        400,
        message,
        request_id=request_id,
        code=code,
        field_errors=field_errors or None,
    )


def validate_oidc_auth(request: Request) -> Dict[str, Any]:
    """Validate OIDC authentication for external endpoints.

    This is a simplified validator that ensures a Bearer token is present.
    In production, this should verify and decode the JWT using OIDC config.
    """

    from ..http.responses import create_error_response
    request_id = request.headers.get("x-request-id")
    auth_header = request.headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail=create_error_response(
                401,
                "Valid Bearer token required",
                request_id=request_id,
                code="AUTH_MISSING_TOKEN",
                context={"header_name": "Authorization"},
            ),
        )

    token = auth_header.split(" ", 1)[1]
    if not token:
        raise HTTPException(
            status_code=401,
            detail=create_error_response(
                401,
                "Invalid or expired token",
                request_id=request_id,
                code="AUTH_INVALID_TOKEN",
                context={"header_name": "Authorization"},
            ),
        )

    return {"sub": "user123", "tier": "premium"}


# -------- Response builders (ETag and non-ETag) --------

def providers_response_builder(request: Request, response: Response) -> ResponseBuilder:
    from ..http import respond_with_etag
    from ..models import ExternalProvidersResponse
    return lambda payload, meta: respond_with_etag(
        ExternalProvidersResponse(data=payload, meta=meta), request, response
    )


def series_response_builder(request: Request, response: Response) -> ResponseBuilder:
    from ..http import respond_with_etag
    from ..models import ExternalSeriesResponse
    return lambda payload, meta: respond_with_etag(
        ExternalSeriesResponse(data=payload, meta=meta), request, response
    )


def observations_response_builder() -> ResponseBuilder:
    from ..models import ExternalObservationsResponse
    return lambda payload, meta: ExternalObservationsResponse(data=payload, meta=meta)


def metadata_response_builder() -> ResponseBuilder:
    from ..models import ExternalMetadataResponse
    return lambda payload, meta: ExternalMetadataResponse(**payload, meta=meta)


# -------- Cache-key component helpers --------

def providers_cache_components(limit: int, offset: Optional[int], cursor: Optional[str]) -> Dict[str, Any]:
    return {"limit": limit, "offset": offset, "cursor": cursor or ""}


def series_cache_components(params: Any) -> Dict[str, Any]:
    return {
        "provider": getattr(params, "provider", None),
        "frequency": getattr(params, "frequency", None),
        "asof": getattr(params, "asof", None),
        "limit": getattr(params, "limit", None),
        "offset": getattr(params, "offset", None),
        "cursor": getattr(params, "cursor", None),
    }


def observations_cache_components(series_id: str, params: Any) -> Dict[str, Any]:
    return {
        "series_id": series_id,
        "start_date": getattr(params, "start_date", None),
        "end_date": getattr(params, "end_date", None),
        "frequency": getattr(params, "frequency", None),
        "asof": getattr(params, "asof", None),
        "limit": getattr(params, "limit", None),
        "offset": getattr(params, "offset", None),
    }


def metadata_cache_components(provider: Optional[str], include_counts: bool) -> Dict[str, Any]:
    return {"provider": provider, "include_counts": include_counts}
