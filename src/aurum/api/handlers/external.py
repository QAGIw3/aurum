"""External data API handlers with input validation, error handling, and OIDC guards."""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from ..config import CacheConfig, TrinoConfig
from ..cache.cache import CacheManager, AsyncCache, CacheBackend
from ..exceptions import (
    AurumAPIException,
    ValidationException,
    NotFoundException,
    ServiceUnavailableException,
    DataProcessingException,
)
from ..http import respond_with_etag
from ..http.responses import create_error_response
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
)
from ..auth import AuthMiddleware, OIDCConfig
from ..container import provide_service
from ..rate_limiting import RateLimitManager
from .external_support import (
    build_external_cache_key,
    cached_endpoint_response,
    prepare_external_context,
    validation_error,
    http_error,
    validate_oidc_auth,
    dao_call_with_metrics,
    providers_response_builder,
    series_response_builder,
    observations_response_builder,
    metadata_response_builder,
    providers_cache_components,
    series_cache_components,
    observations_cache_components,
    metadata_cache_components,
)
from ..trino_client import TrinoClient
from ...data.external_dao import ExternalDAO
from ...observability.metrics import EXTERNAL_CURVE_MAPPING_COUNTER

# Create router
router = APIRouter()

# Constants
EXTERNAL_CACHE_TTL = 300  # 5 minutes
EXTERNAL_OBSERVATIONS_CACHE_TTL = 600  # 10 minutes
EXTERNAL_MAX_LIMIT = 10000
EXTERNAL_SERIES_MAX_LIMIT = 1000
EXTERNAL_METADATA_CACHE_TTL = 1800  # 30 minutes


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
            "observation_date": obs["asof_date"],
            "value": obs["value"],
            "metadata": {
                "curve_key": obs["curve_key"],
                "tenor_label": obs.get("tenor_label"),
                "source": "curve_mapping"
            }
        }
        external_observations.append(external_obs)

    return external_observations


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
    endpoint = "/v1/external/providers"
    context = await prepare_external_context(
        request,
        response=response,
        principal=principal,
        rate_limit_mgr=rate_limit_mgr,
        endpoint=endpoint,
        identifier_suffix="providers",
    )

    async def _fetch_providers() -> List[Dict[str, Any]]:
        return await dao_call_with_metrics(
            "get_providers",
            lambda: dao.get_providers(limit=limit, offset=offset, cursor=cursor),
        )

    try:
        cache_key = build_external_cache_key("providers", components=providers_cache_components(limit, offset, cursor))

        return await cached_endpoint_response(
            cache_mgr=cache_mgr,
            cache_key=cache_key,
            fetcher=_fetch_providers,
            ttl_seconds=EXTERNAL_CACHE_TTL,
            context=context,
            on_cache_hit=context.cache_hit_hook,
            on_cache_miss=context.cache_miss_hook,
            response_builder=providers_response_builder(request, response),
        )

    except ValidationError as exc:
        raise validation_error(exc, request_id=context.request_id)
    except Exception as exc:
        raise http_error(
            500,
            "Failed to retrieve external providers",
            request_id=context.request_id,
            code="EXTERNAL_PROVIDERS_ERROR",
            context={"error_type": exc.__class__.__name__},
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
    endpoint = "/v1/external/series"
    context = await prepare_external_context(
        request,
        response=response,
        principal=principal,
        rate_limit_mgr=rate_limit_mgr,
        endpoint=endpoint,
        identifier_suffix="series",
    )

    async def _fetch_series() -> List[Dict[str, Any]]:
        return await dao_call_with_metrics(
            "get_series",
            lambda: dao.get_series(
                provider=params.provider,
                frequency=params.frequency,
                asof=params.asof,
                limit=params.limit,
                offset=params.offset,
                cursor=params.cursor,
            ),
        )

    try:
        cache_key = build_external_cache_key("series", components=series_cache_components(params))

        return await cached_endpoint_response(
            cache_mgr=cache_mgr,
            cache_key=cache_key,
            fetcher=_fetch_series,
            ttl_seconds=EXTERNAL_CACHE_TTL,
            context=context,
            on_cache_hit=context.cache_hit_hook,
            on_cache_miss=context.cache_miss_hook,
            response_builder=series_response_builder(request, response),
        )

    except ValidationError as exc:
        raise validation_error(exc, request_id=context.request_id)
    except Exception as exc:
        raise http_error(
            500,
            "Failed to retrieve external series",
            request_id=context.request_id,
            code="EXTERNAL_SERIES_ERROR",
            context={"error_type": exc.__class__.__name__},
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
    """Return observations for a specific external series."""

    endpoint = "/v1/external/series/{series_id}/observations"
    context = await prepare_external_context(
        request,
        response=response,
        principal=principal,
        rate_limit_mgr=rate_limit_mgr,
        endpoint=endpoint,
        identifier_suffix="observations",
        request_tokens=2,
    )

    async def _fetch_observations() -> List[Dict[str, Any]]:
        has_curve_mapping = await _check_curve_mapping(series_id)

        if has_curve_mapping:
            if EXTERNAL_CURVE_MAPPING_COUNTER:
                EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="curve_proxy").inc()

            curve_observations = await _proxy_to_curves_endpoint(
                series_id=series_id,
                start_date=params.start_date,
                end_date=params.end_date,
                frequency=params.frequency,
                asof=params.asof,
                limit=params.limit,
                offset=params.offset,
            )

            observations_result = await _convert_curve_to_external_observations(curve_observations, series_id)

            if EXTERNAL_CURVE_MAPPING_COUNTER:
                EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="curve_conversion").inc()

            return observations_result

        if EXTERNAL_CURVE_MAPPING_COUNTER:
            EXTERNAL_CURVE_MAPPING_COUNTER.labels(mapping_type="no_mapping").inc()

        return await dao_call_with_metrics(
            "get_observations",
            lambda: dao.get_observations(
                series_id=series_id,
                start_date=params.start_date,
                end_date=params.end_date,
                frequency=params.frequency,
                asof=params.asof,
                limit=params.limit,
                offset=params.offset,
                cursor=params.cursor,
            ),
        )

    try:
        cache_key = build_external_cache_key("observations", components=observations_cache_components(series_id, params))

        return await cached_endpoint_response(
            cache_mgr=cache_mgr,
            cache_key=cache_key,
            fetcher=_fetch_observations,
            ttl_seconds=EXTERNAL_OBSERVATIONS_CACHE_TTL,
            context=context,
            on_cache_hit=context.cache_hit_hook,
            on_cache_miss=context.cache_miss_hook,
            response_builder=observations_response_builder(),
        )

    except ValidationError as exc:
        raise validation_error(exc, request_id=context.request_id)
    except NotFoundException as exc:
        raise http_error(
            404,
            "Series not found",
            request_id=context.request_id,
            code="SERIES_NOT_FOUND",
            context={"series_id": series_id},
        )
    except Exception as exc:
        raise http_error(
            500,
            "Failed to retrieve series observations",
            request_id=context.request_id,
            code="EXTERNAL_OBSERVATIONS_ERROR",
            context={"error_type": exc.__class__.__name__, "series_id": series_id},
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
    endpoint = "/v1/metadata/external"
    context = await prepare_external_context(
        request,
        response=response,
        principal=principal,
        rate_limit_mgr=rate_limit_mgr,
        endpoint=endpoint,
        identifier_suffix="metadata",
    )

    async def _fetch_metadata() -> Dict[str, Any]:
        return await dao_call_with_metrics(
            "get_metadata",
            lambda: dao.get_metadata(
                provider=provider,
                include_counts=include_counts,
            ),
        )

    try:
        cache_key = build_external_cache_key("metadata", components=metadata_cache_components(provider, include_counts))

        return await cached_endpoint_response(
            cache_mgr=cache_mgr,
            cache_key=cache_key,
            fetcher=_fetch_metadata,
            ttl_seconds=EXTERNAL_METADATA_CACHE_TTL,
            context=context,
            on_cache_hit=context.cache_hit_hook,
            on_cache_miss=context.cache_miss_hook,
            response_builder=metadata_response_builder(),
        )

    except ValidationError as exc:
        raise validation_error(exc, request_id=context.request_id)
    except Exception as exc:
        raise http_error(
            500,
            "Failed to retrieve external metadata",
            request_id=context.request_id,
            code="EXTERNAL_METADATA_ERROR",
            context={"error_type": exc.__class__.__name__},
        )
