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
from ...telemetry.context import get_request_id

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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/metadata/dimensions"
                    }
                )

        # Get dimensions service (placeholder - would integrate with actual service)
        from ..routes import _dimensions_data
        dimensions_data = _dimensions_data(asof)

        # Apply pagination
        total_count = len(dimensions_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = dimensions_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

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
        result = DimensionsResponse(
            data=dimensions,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "total_count": total_count,
                "returned_count": len(dimensions),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/metadata/locations"
                    }
                )

        # Get locations service (placeholder - would integrate with actual service)
        from ..routes import _locations_data
        locations_data = _locations_data(iso)

        # Apply pagination
        total_count = len(locations_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = locations_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

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
        result = IsoLocationsResponse(
            data=locations,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "iso": iso,
                "total_count": total_count,
                "returned_count": len(locations),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/metadata/units"
                    }
                )

        # Get units service (placeholder - would integrate with actual service)
        from ..routes import _units_canonical_data
        units_data = _units_canonical_data()

        # Apply pagination
        total_count = len(units_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = units_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        result = UnitsCanonicalResponse(
            data=paginated_data,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "total_count": total_count,
                "returned_count": len(paginated_data),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/metadata/calendars"
                    }
                )

        # Get calendars service (placeholder - would integrate with actual service)
        from ..routes import _calendars_data
        calendars_data = _calendars_data()

        # Apply pagination
        total_count = len(calendars_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = calendars_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        result = CalendarsResponse(
            data=paginated_data,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "total_count": total_count,
                "returned_count": len(paginated_data),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

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
