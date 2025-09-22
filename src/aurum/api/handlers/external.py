"""External data API handlers with input validation, error handling, and OIDC guards."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from ..config import CacheConfig, TrinoConfig
from ..cache import CacheManager, AsyncCache, CacheBackend
from ..exceptions import (
    AurumAPIException,
    ValidationException,
)
from ..http import respond_with_etag
    NotFoundException,
    ServiceUnavailableException,
    DataProcessingException,
)
from ..models import (
    ExternalProvider,
    ExternalSeries,
    ExternalObservation,
    ExternalProvidersResponse,
    ExternalSeriesResponse,
    ExternalObservationsResponse,
    ExternalMetadataResponse,
    ExternalSeriesQueryParams,
    ExternalObservationsQueryParams,
    Meta,
)
from ..auth import AuthMiddleware, OIDCConfig
from ..container import get_service
from ..rate_limiting import RateLimitManager, QuotaTier
from ..trino_client import TrinoClient
from ...data.external_dao import ExternalDAO
from ...observability.metrics import (
    EXTERNAL_API_REQUEST_COUNTER,
    EXTERNAL_API_LATENCY,
    EXTERNAL_CACHE_HIT_COUNTER,
    EXTERNAL_CACHE_MISS_COUNTER,
    EXTERNAL_DAO_QUERY_COUNTER,
    EXTERNAL_DAO_LATENCY,
    EXTERNAL_CURVE_MAPPING_COUNTER,
)

# Create router
router = APIRouter()

# Constants
EXTERNAL_CACHE_TTL = 300  # 5 minutes
EXTERNAL_OBSERVATIONS_CACHE_TTL = 600  # 10 minutes
EXTERNAL_MAX_LIMIT = 10000
EXTERNAL_SERIES_MAX_LIMIT = 1000
EXTERNAL_METADATA_CACHE_TTL = 1800  # 30 minutes


def create_cache_key(prefix: str, **components) -> str:
    """Create a cache key with hash for the given components."""
    # Sort components to ensure consistent hashing
    sorted_components = sorted(components.items())

    # Create hash from all components
    key_parts = [f"{k}:{v}" for k, v in sorted_components if v is not None]
    key_string = "|".join(key_parts)

    # Create hash
    hash_obj = hashlib.sha256(key_string.encode('utf-8'))
    hash_digest = hash_obj.hexdigest()[:16]  # Use first 16 chars of hash

    # Return formatted key
    return f"{prefix}:{hash_digest}"


async def get_external_dao() -> ExternalDAO:
    """Get ExternalDAO instance."""
    return ExternalDAO()


async def get_cache_manager() -> CacheManager:
    """Get cache manager instance."""
    return CacheManager()


async def get_rate_limit_manager() -> RateLimitManager:
    """Get rate limit manager instance."""
    # This would be configured in the main app
    from ..rate_limiting import create_rate_limit_manager
    return create_rate_limit_manager()


async def get_trino_client() -> TrinoClient:
    """Get Trino client instance."""
    from ..trino_client import get_trino_client
    return get_trino_client()


async def _check_curve_mapping(series_id: str) -> bool:
    """Check if series has a curve mapping in market.series_curve_map."""
    dao = ExternalDAO()
    client = await dao.get_trino_client()

    query = """
        SELECT 1 as exists_check
        FROM market.series_curve_map
        WHERE external_series_id = :series_id
        LIMIT 1
    """

    params = {"series_id": series_id}

    try:
        result = await client.execute_query(query, params)
        return len(result) > 0
    except Exception:
        # If query fails, assume no mapping exists
        return False


async def _proxy_to_curves_endpoint(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    frequency: Optional[str] = None,
    asof: Optional[str] = None,
    limit: int = 500,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Proxy request to curves endpoint for mapped series."""
    dao = ExternalDAO()
    client = await dao.get_trino_client()

    # Get curve mapping details
    mapping_query = """
        SELECT curve_key, asset_class, iso, location, market, product, block
        FROM market.series_curve_map
        WHERE external_series_id = :series_id
    """

    params = {"series_id": series_id}
    mapping_result = await client.execute_query(mapping_query, params)

    if not mapping_result:
        raise HTTPException(
            status_code=500,
            detail="Series mapping not found despite mapping check"
        )

    mapping = mapping_result[0]

    # Build curves query
    curves_query = """
        SELECT
            curve_key,
            tenor_label,
            mid as value,
            asof_date
        FROM market_curves
        WHERE curve_key = :curve_key
    """

    curves_params = {
        "curve_key": mapping["curve_key"]
    }

    # Add filters based on mapping and request parameters
    conditions = []

    if mapping["asset_class"]:
        conditions.append("asset_class = :asset_class")
        curves_params["asset_class"] = mapping["asset_class"]

    if mapping["iso"]:
        conditions.append("iso = :iso")
        curves_params["iso"] = mapping["iso"]

    if mapping["location"]:
        conditions.append("location = :location")
        curves_params["location"] = mapping["location"]

    if mapping["market"]:
        conditions.append("market = :market")
        curves_params["market"] = mapping["market"]

    if mapping["product"]:
        conditions.append("product = :product")
        curves_params["product"] = mapping["product"]

    if mapping["block"]:
        conditions.append("block = :block")
        curves_params["block"] = mapping["block"]

    if start_date:
        conditions.append("asof_date >= :start_date")
        curves_params["start_date"] = start_date

    if end_date:
        conditions.append("asof_date <= :end_date")
        curves_params["end_date"] = end_date

    if asof:
        conditions.append("asof_date <= :asof")
        curves_params["asof"] = asof

    if conditions:
        curves_query += " AND " + " AND ".join(conditions)

    # Add ordering and limits
    curves_query += """
        ORDER BY asof_date
        LIMIT :limit
        OFFSET :offset
    """
    curves_params["limit"] = limit
    curves_params["offset"] = offset

    result = await client.execute_query(curves_query, curves_params)
    return result


async def _convert_curve_to_external_observations(
    curve_observations: List[Dict[str, Any]],
    series_id: str
) -> List[Dict[str, Any]]:
    """Convert curve observations to external observations format."""
    external_observations = []

    for obs in curve_observations:
        external_obs = {
            "series_id": series_id,
            "date": obs["asof_date"],
            "value": obs["value"],
            "metadata": {
                "curve_key": obs["curve_key"],
                "tenor_label": obs.get("tenor_label"),
                "source": "curve_mapping"
            }
        }
        external_observations.append(external_obs)

    return external_observations


def validate_oidc_auth(request: Request) -> Dict[str, Any]:
    """Validate OIDC authentication."""
    # Check for Authorization header
    auth_header = request.headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Authentication required",
                "message": "Valid Bearer token required",
                "code": "AUTH_MISSING_TOKEN",
                "field": "authorization",
                "context": {"header_name": "Authorization"},
            }
        )

    # Extract token
    token = auth_header.split(" ", 1)[1]

    # Validate token (simplified - would use OIDC validation in production)
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Authentication failed",
                "message": "Invalid or expired token",
                "code": "AUTH_INVALID_TOKEN",
                "field": "authorization",
            }
        )

    # Return principal (simplified - would decode JWT in production)
    return {"sub": "user123", "tier": "premium"}


def create_external_meta(request_id: str, query_time_ms: int) -> Meta:
    """Create metadata for external API responses."""
    return Meta(
        request_id=request_id,
        query_time_ms=query_time_ms,
        has_more=False,
        count=0,
        total=None,
        offset=0,
        limit=None,
    )


@router.get("/v1/external/providers", response_model=ExternalProvidersResponse)
async def list_external_providers(
    request: Request,
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of providers to return"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    *,
    response: Response,
    # Dependencies
    principal: Dict[str, Any] = Depends(validate_oidc_auth),
    dao: ExternalDAO = Depends(get_external_dao),
    cache_mgr: CacheManager = Depends(get_cache_manager),
    rate_limit_mgr: RateLimitManager = Depends(get_rate_limit_manager),
) -> ExternalProvidersResponse:
    """
    List external data providers.

    Returns a paginated list of external data providers with their metadata.
    """
    request_id = str(request.headers.get("x-request-id", "unknown"))

    # Rate limiting
    rate_limit_result = await rate_limit_mgr.check_rate_limit(
        identifier=f"external:providers:{principal.get('sub', 'anonymous')}",
        tier=QuotaTier(principal.get('tier', 'free').lower()),
        endpoint="/v1/external/providers"
    )
    if not rate_limit_result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": "Too many requests",
                "retry_after": rate_limit_result.retry_after,
                "code": "RATE_LIMIT_EXCEEDED",
                "context": {"retry_after_seconds": rate_limit_result.retry_after},
            }
        )

    # Add rate limit headers
    headers = rate_limit_result.to_headers()
    for key, value in headers.items():
        response.headers[key] = value

    start_time = time.time()

    # Record request metrics
    if EXTERNAL_API_REQUEST_COUNTER and EXTERNAL_API_LATENCY:
        EXTERNAL_API_REQUEST_COUNTER.labels(endpoint="/v1/external/providers", status="200").inc()
        EXTERNAL_API_LATENCY.labels(endpoint="/v1/external/providers").observe(time.time() - start_time)

    try:
        # Create cache key with hash
        cache_key = create_cache_key(
            "external:providers",
            limit=limit,
            offset=offset,
            cursor=cursor or ''
        )

        # Try cache first
        cached_result = await cache_mgr.get(cache_key)
        if cached_result is not None:
            if EXTERNAL_CACHE_HIT_COUNTER:
                EXTERNAL_CACHE_HIT_COUNTER.labels(endpoint="/v1/external/providers").inc()
            query_time_ms = int((time.time() - start_time) * 1000)
            meta = create_external_meta(request_id, query_time_ms)
            model = ExternalProvidersResponse(data=cached_result, meta=meta)
            return _respond_with_etag(model, request, response)

        if EXTERNAL_CACHE_MISS_COUNTER:
            EXTERNAL_CACHE_MISS_COUNTER.labels(endpoint="/v1/external/providers").inc()

        # Query providers with DAO metrics
        dao_start = time.time()
        if EXTERNAL_DAO_QUERY_COUNTER and EXTERNAL_DAO_LATENCY:
            EXTERNAL_DAO_QUERY_COUNTER.labels(operation="get_providers", status="start").inc()

        providers = await dao.get_providers(limit=limit, offset=offset, cursor=cursor)

        if EXTERNAL_DAO_QUERY_COUNTER and EXTERNAL_DAO_LATENCY:
            EXTERNAL_DAO_QUERY_COUNTER.labels(operation="get_providers", status="success").inc()
            EXTERNAL_DAO_LATENCY.labels(operation="get_providers").observe(time.time() - dao_start)

        # Cache result
        await cache_mgr.set(
            cache_key,
            providers,
            ttl=EXTERNAL_CACHE_TTL
        )

        query_time_ms = int((time.time() - start_time) * 1000)
        meta = create_external_meta(request_id, query_time_ms)

        model = ExternalProvidersResponse(data=providers, meta=meta)
        return _respond_with_etag(model, request, response)

    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Validation Error",
                "message": "Invalid request parameters",
                "field_errors": [
                    {
                        "field": error["loc"][0],
                        "message": error["msg"],
                        "value": error["input"],
                    }
                    for error in exc.errors()
                ],
                "code": "VALIDATION_ERROR",
                "request_id": request_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "Failed to retrieve external providers",
                "code": "EXTERNAL_PROVIDERS_ERROR",
                "context": {"error_type": exc.__class__.__name__},
                "request_id": request_id,
            }
        )


@router.get("/v1/external/series", response_model=ExternalSeriesResponse)
async def list_external_series(
    request: Request,
    params: ExternalSeriesQueryParams = Depends(),
    *,
    response: Response,
    principal: Dict[str, Any] = Depends(validate_oidc_auth),
    dao: ExternalDAO = Depends(get_external_dao),
    cache_mgr: CacheManager = Depends(get_cache_manager),
    rate_limit_mgr: RateLimitManager = Depends(get_rate_limit_manager),
) -> ExternalSeriesResponse:
    """
    List external series with optional filtering.

    Returns a paginated list of external data series filtered by provider, frequency, and as-of date.
    """
    request_id = str(request.headers.get("x-request-id", "unknown"))

    # Rate limiting
    rate_limit_result = await rate_limit_mgr.check_rate_limit(
        identifier=f"external:series:{principal.get('sub', 'anonymous')}",
        tier=QuotaTier(principal.get('tier', 'free').lower()),
        endpoint="/v1/external/series"
    )
    if not rate_limit_result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": "Too many requests",
                "retry_after": rate_limit_result.retry_after,
                "code": "RATE_LIMIT_EXCEEDED",
                "context": {"retry_after_seconds": rate_limit_result.retry_after},
            }
        )

    # Add rate limit headers
    headers = rate_limit_result.to_headers()
    for key, value in headers.items():
        response.headers[key] = value

    start_time = time.time()

    try:
        # Create cache key with hash
        cache_key = create_cache_key(
            "external:series",
            provider=params.provider,
            frequency=params.frequency,
            asof=params.asof,
            limit=params.limit,
            offset=params.offset,
            cursor=params.cursor
        )

        # Try cache first
        cached_result = await cache_mgr.get(cache_key)
        if cached_result is not None:
            query_time_ms = int((time.time() - start_time) * 1000)
            meta = create_external_meta(request_id, query_time_ms)
            model = ExternalSeriesResponse(data=cached_result, meta=meta)
            return _respond_with_etag(model, request, response)

        # Query series
        series = await dao.get_series(
            provider=params.provider,
            frequency=params.frequency,
            asof=params.asof,
            limit=params.limit,
            offset=params.offset,
            cursor=params.cursor
        )

        # Cache result
        await cache_mgr.set(
            cache_key,
            series,
            ttl=EXTERNAL_CACHE_TTL
        )

        query_time_ms = int((time.time() - start_time) * 1000)
        meta = create_external_meta(request_id, query_time_ms)

        model = ExternalSeriesResponse(data=series, meta=meta)
        return _respond_with_etag(model, request, response)

    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Validation Error",
                "message": "Invalid request parameters",
                "field_errors": [
                    {
                        "field": error["loc"][0],
                        "message": error["msg"],
                        "value": error["input"],
                    }
                    for error in exc.errors()
                ],
                "code": "VALIDATION_ERROR",
                "request_id": request_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "Failed to retrieve external series",
                "code": "EXTERNAL_SERIES_ERROR",
                "context": {"error_type": exc.__class__.__name__},
                "request_id": request_id,
            }
        )


@router.get("/v1/external/series/{series_id}/observations", response_model=ExternalObservationsResponse)
async def get_external_series_observations(
    request: Request,
    series_id: str,
    params: ExternalObservationsQueryParams = Depends(),
    *,
    response: Response,
    principal: Dict[str, Any] = Depends(validate_oidc_auth),
    dao: ExternalDAO = Depends(get_external_dao),
    cache_mgr: CacheManager = Depends(get_cache_manager),
    rate_limit_mgr: RateLimitManager = Depends(get_rate_limit_manager),
) -> ExternalObservationsResponse:
    """
    Get observations for a specific external series.

    Returns observations for the specified series with optional date range filtering and frequency conversion.
    """
    request_id = str(request.headers.get("x-request-id", "unknown"))

    # Rate limiting (higher limits for observations endpoint)
    rate_limit_result = await rate_limit_mgr.check_rate_limit(
        identifier=f"external:observations:{principal.get('sub', 'anonymous')}",
        tier=QuotaTier(principal.get('tier', 'free').lower()),
        endpoint="/v1/external/series/{series_id}/observations",
        request_tokens=2  # Higher cost for observations
    )
    if not rate_limit_result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": "Too many requests",
                "retry_after": rate_limit_result.retry_after,
                "code": "RATE_LIMIT_EXCEEDED",
                "context": {"retry_after_seconds": rate_limit_result.retry_after},
            }
        )

    # Add rate limit headers
    headers = rate_limit_result.to_headers()
    for key, value in headers.items():
        response.headers[key] = value

    start_time = time.time()

    try:
        # Create cache key with hash
        cache_key = create_cache_key(
            "external:observations",
            series_id=series_id,
            start_date=params.start_date,
            end_date=params.end_date,
            freq=params.frequency,
            asof=params.asof,
            limit=params.limit,
            offset=params.offset
        )

        # Try cache first
        cached_result = await cache_mgr.get(cache_key)
        if cached_result is not None:
            query_time_ms = int((time.time() - start_time) * 1000)
            meta = create_external_meta(request_id, query_time_ms)
            return ExternalObservationsResponse(data=cached_result, meta=meta)

        # Check for curve mapping passthrough with metrics
        mapping_start = time.time()
        has_curve_mapping = await _check_curve_mapping(series_id)

        if has_curve_mapping:
            if EXTERNAL_CURVE_MAPPING_COUNTER:
                EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="curve_proxy").inc()

            # Proxy to curves endpoint
            curve_observations = await _proxy_to_curves_endpoint(
                series_id=series_id,
                start_date=params.start_date,
                end_date=params.end_date,
                frequency=params.frequency,
                asof=params.asof,
                limit=params.limit,
                offset=params.offset
            )
            # Convert curve observations to external observations format
            observations = await _convert_curve_to_external_observations(curve_observations, series_id)

            if EXTERNAL_CURVE_MAPPING_COUNTER:
                EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="curve_conversion").inc()
        else:
            if EXTERNAL_CURVE_MAPPING_COUNTER:
                EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="no_mapping").inc()

            # Query observations from external DAO with DAO metrics
            dao_start = time.time()
            if EXTERNAL_DAO_QUERY_COUNTER and EXTERNAL_DAO_LATENCY:
                EXTERNAL_DAO_QUERY_COUNTER.labels(operation="get_observations", status="start").inc()

            observations = await dao.get_observations(
                series_id=series_id,
                start_date=params.start_date,
                end_date=params.end_date,
                frequency=params.frequency,
                asof=params.asof,
                limit=params.limit,
                offset=params.offset,
                cursor=params.cursor
            )

            if EXTERNAL_DAO_QUERY_COUNTER and EXTERNAL_DAO_LATENCY:
                EXTERNAL_DAO_QUERY_COUNTER.labels(operation="get_observations", status="success").inc()
                EXTERNAL_DAO_LATENCY.labels(operation="get_observations").observe(time.time() - dao_start)

        # Cache result
        await cache_mgr.set(
            cache_key,
            observations,
            ttl=EXTERNAL_OBSERVATIONS_CACHE_TTL
        )

        query_time_ms = int((time.time() - start_time) * 1000)
        meta = create_external_meta(request_id, query_time_ms)

        return ExternalObservationsResponse(data=observations, meta=meta)

    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Validation Error",
                "message": "Invalid request parameters",
                "field_errors": [
                    {
                        "field": error["loc"][0],
                        "message": error["msg"],
                        "value": error["input"],
                    }
                    for error in exc.errors()
                ],
                "code": "VALIDATION_ERROR",
                "request_id": request_id,
            }
        )
    except NotFoundException as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Not Found",
                "message": "Series not found",
                "code": "SERIES_NOT_FOUND",
                "context": {"series_id": series_id},
                "request_id": request_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "Failed to retrieve series observations",
                "code": "EXTERNAL_OBSERVATIONS_ERROR",
                "context": {"error_type": exc.__class__.__name__, "series_id": series_id},
                "request_id": request_id,
            }
        )


@router.get("/v1/metadata/external", response_model=ExternalMetadataResponse)
async def get_external_metadata(
    request: Request,
    provider: Optional[str] = Query(None, description="Filter by provider"),
    include_counts: bool = Query(False, description="Include series counts in response"),
    *,
    response: Response,
    principal: Dict[str, Any] = Depends(validate_oidc_auth),
    dao: ExternalDAO = Depends(get_external_dao),
    cache_mgr: CacheManager = Depends(get_cache_manager),
    rate_limit_mgr: RateLimitManager = Depends(get_rate_limit_manager),
) -> ExternalMetadataResponse:
    """
    Get external data metadata.

    Returns metadata about external data providers and their series.
    """
    request_id = str(request.headers.get("x-request-id", "unknown"))

    # Rate limiting (metadata endpoint)
    rate_limit_result = await rate_limit_mgr.check_rate_limit(
        identifier=f"external:metadata:{principal.get('sub', 'anonymous')}",
        tier=QuotaTier(principal.get('tier', 'free').lower()),
        endpoint="/v1/metadata/external"
    )
    if not rate_limit_result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": "Too many requests",
                "retry_after": rate_limit_result.retry_after,
                "code": "RATE_LIMIT_EXCEEDED",
                "context": {"retry_after_seconds": rate_limit_result.retry_after},
            }
        )

    # Add rate limit headers
    headers = rate_limit_result.to_headers()
    for key, value in headers.items():
        response.headers[key] = value

    start_time = time.time()

    try:
        # Create cache key with hash
        cache_key = create_cache_key(
            "external:metadata",
            provider=provider,
            include_counts=include_counts
        )

        # Try cache first
        cached_result = await cache_mgr.get(cache_key)
        if cached_result is not None:
            query_time_ms = int((time.time() - start_time) * 1000)
            meta = create_external_meta(request_id, query_time_ms)
            return ExternalMetadataResponse(**cached_result, meta=meta)

        # Query metadata
        metadata = await dao.get_metadata(
            provider=provider,
            include_counts=include_counts
        )

        # Cache result
        await cache_mgr.set(
            cache_key,
            metadata,
            ttl=EXTERNAL_METADATA_CACHE_TTL
        )

        query_time_ms = int((time.time() - start_time) * 1000)
        meta = create_external_meta(request_id, query_time_ms)

        return ExternalMetadataResponse(**metadata, meta=meta)

    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Validation Error",
                "message": "Invalid request parameters",
                "field_errors": [
                    {
                        "field": error["loc"][0],
                        "message": error["msg"],
                        "value": error["input"],
                    }
                    for error in exc.errors()
                ],
                "code": "VALIDATION_ERROR",
                "request_id": request_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "Failed to retrieve external metadata",
                "code": "EXTERNAL_METADATA_ERROR",
                "context": {"error_type": exc.__class__.__name__},
                "request_id": request_id,
            }
        )
