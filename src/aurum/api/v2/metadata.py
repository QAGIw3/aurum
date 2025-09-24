"""v2 Metadata API with enhanced features.

This module provides the v2 implementation of the metadata API with:
- Cursor-only pagination (offset deprecated)
- Consistent error shapes using RFC 7807
- Enhanced ETag support
- Improved validation and error handling
- Better observability
- Link headers for navigation
- Tenant context enforcement

Notes:
- Base path: `/v2/*` (see app wiring in src/aurum/api/app.py)
- Migration guidance from v1 endpoints: docs/migration-guide.md
"""

from __future__ import annotations

import time
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag
from .pagination import (
    resolve_pagination,
    build_next_cursor,
    build_pagination_envelope,
)
from ...telemetry.context import get_request_id
from ...scenarios.series_curve_mapping import get_database_mapper

router = APIRouter(prefix="/v2", tags=["metadata"])


class MetadataListResponse(BaseModel):
    """Response for listing metadata with v2 enhancements."""
    data: List[dict] = Field(..., description="List of metadata items")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class DimensionResponse(BaseModel):
    """Response for dimension data with v2 enhancements."""
    dimension: str = Field(..., description="Dimension name")
    values: List[str] = Field(..., description="Dimension values")
    asof: str = Field(..., description="As-of timestamp")
    meta: dict = Field(..., description="Metadata")


class DimensionsResponse(BaseModel):
    """Response for dimensions data with v2 enhancements."""
    data: List[DimensionResponse] = Field(..., description="List of dimensions")
    meta: dict = Field(..., description="Pagination and metadata")


class SeriesCurveMappingRequest(BaseModel):
    """Request model for creating/updating series-curve mappings."""
    external_provider: str = Field(..., description="External data provider (FRED, EIA, etc.)")
    external_series_id: str = Field(..., description="Provider-specific series identifier")
    curve_key: str = Field(..., description="Internal curve key")
    mapping_confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Mapping confidence score")
    mapping_method: str = Field(default="manual", description="Mapping method (manual, automated, heuristic)")
    mapping_notes: Optional[str] = Field(None, description="Mapping rationale")
    is_active: bool = Field(default=True, description="Whether this mapping is active")


class SeriesCurveMappingResponse(BaseModel):
    """Response model for series-curve mapping operations."""
    data: dict = Field(..., description="Mapping data")
    meta: dict = Field(..., description="Operation metadata")


class SeriesMappingSuggestion(BaseModel):
    """Model for series mapping suggestions."""
    curve_key: str = Field(..., description="Suggested curve key")
    similarity_score: float = Field(..., description="Similarity score")
    confidence_score: float = Field(..., description="Confidence score")
    reasoning: dict = Field(..., description="Reasoning for suggestion")


class SeriesMappingSuggestionsResponse(BaseModel):
    """Response for series mapping suggestions."""
    data: List[SeriesMappingSuggestion] = Field(..., description="List of mapping suggestions")
    meta: dict = Field(..., description="Request metadata")
    links: dict = Field(..., description="Pagination links")


class IsoLocationResponse(BaseModel):
    """Response for ISO location data with v2 enhancements."""
    iso: str = Field(..., description="ISO identifier")
    location_id: str = Field(..., description="Location identifier")
    name: str = Field(..., description="Location name")
    latitude: Optional[float] = Field(None, description="Latitude")
    longitude: Optional[float] = Field(None, description="Longitude")
    meta: dict = Field(..., description="Metadata")


class IsoLocationsResponse(BaseModel):
    """Response for ISO locations data with v2 enhancements."""
    data: List[IsoLocationResponse] = Field(..., description="List of locations")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class UnitsCanonicalResponse(BaseModel):
    """Response for canonical units data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of canonical units")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class UnitsMappingResponse(BaseModel):
    """Response for units mapping data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of unit mappings")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class CalendarsResponse(BaseModel):
    """Response for calendars data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of calendars")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class CalendarBlocksResponse(BaseModel):
    """Response for calendar blocks data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of calendar blocks")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class CalendarHoursResponse(BaseModel):
    """Response for calendar hours data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of calendar hours")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/metadata/dimensions", response_model=DimensionsResponse)
async def list_dimensions_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    asof: Optional[str] = Query(None, description="As-of date filter"),
) -> DimensionsResponse:
    """List dimensions with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"asof": asof},
        )

        from ..metadata_v2_service import get_metadata_service
        svc = await get_metadata_service()
        paginated_data, total_count = await svc.list_dimensions(asof=asof, offset=offset, limit=effective_limit)

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=end_idx < total_count,
            filters={"asof": asof},
        )
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        dimensions = []
        for dim_data in paginated_data:
            dimensions.append(DimensionResponse(
                dimension=dim_data["dimension"],
                values=dim_data["values"],
                asof=dim_data.get("asof", "latest"),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(dimensions),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = DimensionsResponse(
            data=dimensions,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list dimensions: {str(exc)}",
                "instance": "/v2/metadata/dimensions",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/metadata/locations", response_model=IsoLocationsResponse)
async def list_locations_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    iso: str = Query(..., description="ISO identifier"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> IsoLocationsResponse:
    """List ISO locations with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"iso": iso},
        )

        from ..metadata_v2_service import get_metadata_service
        svc = await get_metadata_service()
        paginated_data, total_count = await svc.list_locations(iso=iso, offset=offset, limit=effective_limit)

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=end_idx < total_count,
            filters={"iso": iso},
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"iso": iso})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        locations = []
        for loc_data in paginated_data:
            locations.append(IsoLocationResponse(
                iso=iso,
                location_id=loc_data["location_id"],
                name=loc_data["name"],
                latitude=loc_data.get("latitude"),
                longitude=loc_data.get("longitude"),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "iso": iso,
            "returned_count": len(locations),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = IsoLocationsResponse(
            data=locations,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list locations: {str(exc)}",
                "instance": "/v2/metadata/locations",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/metadata/units", response_model=UnitsCanonicalResponse)
async def list_units_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> UnitsCanonicalResponse:
    """List canonical units with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters=None,
        )

        from ..metadata_v2_service import get_metadata_service
        svc = await get_metadata_service()
        paginated_data, total_count = await svc.list_units(offset=offset, limit=effective_limit)

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=end_idx < total_count,
            filters=None,
        )
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters=None)
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(paginated_data),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = UnitsCanonicalResponse(
            data=paginated_data,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list units: {str(exc)}",
                "instance": "/v2/metadata/units",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/metadata/calendars", response_model=CalendarsResponse)
async def list_calendars_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> CalendarsResponse:
    """List calendars with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters=None,
        )

        from ..metadata_v2_service import get_metadata_service
        svc = await get_metadata_service()
        paginated_data, total_count = await svc.list_calendars(offset=offset, limit=effective_limit)

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=end_idx < total_count,
            filters=None,
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters=None)
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(paginated_data),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = CalendarsResponse(
            data=paginated_data,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list calendars: {str(exc)}",
                "instance": "/v2/metadata/calendars",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


# Series-Curve Mapping Endpoints
@router.post("/series-curve-mappings", response_model=SeriesCurveMappingResponse)
async def create_series_curve_mapping(
    request: Request,
    response: Response,
    mapping: SeriesCurveMappingRequest,
) -> SeriesCurveMappingResponse:
    """Create a new series-curve mapping."""
    start_time = time.perf_counter()

    try:
        # Get database mapper
        mapper = get_database_mapper()
        await mapper.initialize()
        mapping_id = await mapper.create_mapping(
            external_provider=mapping.external_provider,
            external_series_id=mapping.external_series_id,
            curve_key=mapping.curve_key,
            mapping_confidence=mapping.mapping_confidence,
            mapping_method=mapping.mapping_method,
            mapping_notes=mapping.mapping_notes,
            is_active=mapping.is_active,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = SeriesCurveMappingResponse(
            data={
                "id": mapping_id,
                "external_provider": mapping.external_provider,
                "external_series_id": mapping.external_series_id,
                "curve_key": mapping.curve_key,
                "mapping_confidence": mapping.mapping_confidence,
                "mapping_method": mapping.mapping_method,
                "mapping_notes": mapping.mapping_notes,
                "is_active": mapping.is_active,
            },
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create series-curve mapping: {str(exc)}"
        ) from exc


@router.get("/series-curve-mappings", response_model=SeriesCurveMappingResponse)
async def list_series_curve_mappings(
    request: Request,
    response: Response,
    provider: Optional[str] = Query(None, description="Filter by external provider"),
    series_id: Optional[str] = Query(None, description="Filter by external series ID"),
    curve_key: Optional[str] = Query(None, description="Filter by curve key"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
) -> SeriesCurveMappingResponse:
    """List series-curve mappings with optional filtering."""
    start_time = time.perf_counter()

    try:
        # Get database mapper
        mapper = get_database_mapper()
        await mapper.initialize()
        mappings, total = await mapper.list_mappings(
            external_provider=provider,
            external_series_id=series_id,
            curve_key=curve_key,
            is_active=is_active,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = SeriesCurveMappingResponse(
            data=mappings,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total": total,
                "limit": limit,
                "offset": offset,
            },
            links={
                "self": str(request.url),
                "next": f"{request.url.replace(query=f'offset={offset + limit}')}" if offset + limit < total else None,
                "prev": f"{request.url.replace(query=f'offset={max(0, offset - limit)}')}" if offset > 0 else None,
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list series-curve mappings: {str(exc)}"
        ) from exc


@router.put("/series-curve-mappings/{mapping_id}", response_model=SeriesCurveMappingResponse)
async def update_series_curve_mapping(
    request: Request,
    response: Response,
    mapping_id: str,
    mapping: SeriesCurveMappingRequest,
) -> SeriesCurveMappingResponse:
    """Update an existing series-curve mapping."""
    start_time = time.perf_counter()

    try:
        # Get database mapper
        mapper = get_database_mapper()
        await mapper.initialize()
        updated_mapping = await mapper.update_mapping(
            mapping_id=mapping_id,
            external_provider=mapping.external_provider,
            external_series_id=mapping.external_series_id,
            curve_key=mapping.curve_key,
            mapping_confidence=mapping.mapping_confidence,
            mapping_method=mapping.mapping_method,
            mapping_notes=mapping.mapping_notes,
            is_active=mapping.is_active,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = SeriesCurveMappingResponse(
            data=updated_mapping,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update series-curve mapping: {str(exc)}"
        ) from exc


@router.delete("/series-curve-mappings/{mapping_id}")
async def delete_series_curve_mapping(
    request: Request,
    response: Response,
    mapping_id: str,
) -> dict:
    """Delete a series-curve mapping."""
    start_time = time.perf_counter()

    try:
        # Get database mapper
        mapper = get_database_mapper()
        await mapper.initialize()
        deleted = await mapper.delete_mapping(mapping_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "data": {"deleted": deleted},
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete series-curve mapping: {str(exc)}"
        ) from exc


@router.get("/series-mapping-suggestions", response_model=SeriesMappingSuggestionsResponse)
async def get_series_mapping_suggestions(
    request: Request,
    response: Response,
    provider: str = Query(..., description="External data provider"),
    series_id: str = Query(..., description="External series ID"),
    series_name: Optional[str] = Query(None, description="Series name for similarity matching"),
    description: Optional[str] = Query(None, description="Series description"),
    units: Optional[str] = Query(None, description="Series units"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of suggestions"),
) -> SeriesMappingSuggestionsResponse:
    """Get automated suggestions for mapping an external series to internal curves."""
    start_time = time.perf_counter()

    try:
        # Get database mapper
        mapper = get_database_mapper()
        await mapper.initialize()
        suggestions = await mapper.get_mapping_suggestions(
            provider=provider,
            series_id=series_id,
            series_name=series_name,
            description=description,
            units=units,
            limit=limit,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = SeriesMappingSuggestionsResponse(
            data=suggestions,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "provider": provider,
                "series_id": series_id,
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get mapping suggestions: {str(exc)}"
        ) from exc


# Staleness Monitoring Endpoints
class StalenessStatusResponse(BaseModel):
    """Response for staleness status."""
    dataset: str = Field(..., description="Dataset identifier")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")
    staleness_minutes: float = Field(..., description="Minutes since last update")
    staleness_level: str = Field(..., description="Staleness level (fresh/stale/critical)")
    slo_status: str = Field(..., description="SLO compliance status")
    expected_frequency_minutes: int = Field(..., description="Expected update frequency")
    is_breached: bool = Field(..., description="Whether SLO is breached")


class StalenessListResponse(BaseModel):
    """Response for listing staleness statuses."""
    data: List[StalenessStatusResponse] = Field(..., description="List of staleness statuses")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class StalenessAlertResponse(BaseModel):
    """Response for staleness alerts."""
    dataset: str = Field(..., description="Dataset identifier")
    alert_type: str = Field(..., description="Type of alert")
    severity: str = Field(..., description="Alert severity")
    message: str = Field(..., description="Alert message")
    timestamp: str = Field(..., description="Alert timestamp")
    remediation_suggestion: Optional[str] = Field(None, description="Suggested remediation")


class StalenessAlertsResponse(BaseModel):
    """Response for staleness alerts."""
    data: List[StalenessAlertResponse] = Field(..., description="List of active alerts")
    meta: dict = Field(..., description="Metadata")
    links: dict = Field(..., description="Links")


@router.get("/staleness", response_model=StalenessListResponse)
async def get_staleness_status(
    request: Request,
    response: Response,
    dataset: Optional[str] = Query(None, description="Filter by dataset name"),
    staleness_level: Optional[str] = Query(None, description="Filter by staleness level"),
    slo_status: Optional[str] = Query(None, description="Filter by SLO status"),
    is_breached: Optional[bool] = Query(None, description="Filter by SLO breach status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
) -> StalenessListResponse:
    """Get staleness status for datasets."""
    start_time = time.perf_counter()

    try:
        # Get staleness monitor
        from ..staleness.staleness_monitor import StalenessMonitor
        monitor = await StalenessMonitor.get_instance()

        # Get staleness statuses
        statuses = await monitor.get_staleness_status(
            dataset_filter=dataset,
            staleness_level_filter=staleness_level,
            slo_status_filter=slo_status,
            breached_filter=is_breached,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = StalenessListResponse(
            data=statuses,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total": len(statuses),
                "limit": limit,
                "offset": offset,
            },
            links={
                "self": str(request.url),
                "next": f"{request.url.replace(query=f'offset={offset + limit}')}" if offset + limit < len(statuses) else None,
                "prev": f"{request.url.replace(query=f'offset={max(0, offset - limit)}')}" if offset > 0 else None,
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get staleness status: {str(exc)}"
        ) from exc


@router.get("/staleness/alerts", response_model=StalenessAlertsResponse)
async def get_staleness_alerts(
    request: Request,
    response: Response,
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    resolved: Optional[bool] = Query(None, description="Filter by resolution status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
) -> StalenessAlertsResponse:
    """Get active staleness alerts."""
    start_time = time.perf_counter()

    try:
        # Get staleness monitor
        from ..staleness.staleness_monitor import StalenessMonitor
        monitor = await StalenessMonitor.get_instance()

        # Get alerts
        alerts = await monitor.get_active_alerts(
            severity_filter=severity,
            resolved_filter=resolved,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = StalenessAlertsResponse(
            data=alerts,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total": len(alerts),
                "limit": limit,
                "offset": offset,
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get staleness alerts: {str(exc)}"
        ) from exc


@router.post("/staleness/check")
async def trigger_staleness_check(
    request: Request,
    response: Response,
) -> dict:
    """Trigger a manual staleness check."""
    start_time = time.perf_counter()

    try:
        # Get staleness monitor
        from ..staleness.staleness_monitor import StalenessMonitor
        monitor = await StalenessMonitor.get_instance()

        # Trigger check
        await monitor.check_staleness()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "data": {"message": "Staleness check triggered successfully"},
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger staleness check: {str(exc)}"
        ) from exc


@router.get("/staleness/metrics", response_model=StalenessListResponse)
async def get_staleness_metrics(
    request: Request,
    response: Response,
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    metric_type: Optional[str] = Query(None, description="Filter by metric type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
) -> StalenessListResponse:
    """Get staleness metrics and performance indicators."""
    start_time = time.perf_counter()

    try:
        # Get staleness monitor
        from ..staleness.staleness_monitor import StalenessMonitor
        monitor = await StalenessMonitor.get_instance()

        # Get metrics
        metrics = await monitor.get_metrics(
            dataset_filter=dataset,
            metric_type_filter=metric_type,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = StalenessListResponse(
            data=metrics,
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total": len(metrics),
                "limit": limit,
                "offset": offset,
            },
        )

        return respond_with_etag(result, request, response)

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get staleness metrics: {str(exc)}"
        ) from exc
