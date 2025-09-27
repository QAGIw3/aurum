"""v1 Metadata API with backward compatibility.

This module provides the v1 implementation of the metadata API with:
- Backward compatible response formats
- Legacy pagination support (offset-based)
- Simplified error handling
- Basic observability

Notes:
- Base path: `/v1/*` (see app wiring in src/aurum/api/app.py)
- Migration guidance to v2 endpoints: docs/migration-guide.md
"""

from __future__ import annotations

import time
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response

from ..telemetry.context import get_request_id
from ..services import MetadataService
from ..deps import get_settings
from aurum.core import AurumSettings

router = APIRouter(prefix="/v1", tags=["metadata"])


@router.get("/metadata/dimensions", response_model=dict)
async def get_dimensions_v1(
    request: Request,
    response: Response,
    asof: Optional[str] = None,
    include_counts: bool = Query(False, description="Include dimension value counts"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get available dimensions and values for filtering (v1 compatibility)."""
    start_time = time.perf_counter()

    try:
        service = MetadataService()
        dimensions, counts = await service.get_dimensions(
            asof=asof,
            include_counts=include_counts,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(dimensions),
                "total": len(dimensions),
                "offset": offset,
                "limit": limit,
            },
            "data": dimensions,
            "counts": counts if include_counts else None,
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get dimensions: {str(exc)}"
        )


@router.get("/metadata/locations", response_model=dict)
async def get_locations_v1(
    request: Request,
    response: Response,
    tenant_id: Optional[str] = Query(None, description="Filter by tenant"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get ISO locations for filtering (v1 compatibility)."""
    start_time = time.perf_counter()

    try:
        service = MetadataService()
        locations = await service.get_locations(
            tenant_id=tenant_id,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(locations),
                "total": len(locations),
                "offset": offset,
                "limit": limit,
            },
            "data": locations,
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get locations: {str(exc)}"
        )


@router.get("/metadata/units", response_model=dict)
async def get_units_v1(
    request: Request,
    response: Response,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get units and canonical mappings (v1 compatibility)."""
    start_time = time.perf_counter()

    try:
        service = MetadataService()
        units = await service.get_units(limit=limit, offset=offset)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(units),
                "total": len(units),
                "offset": offset,
                "limit": limit,
            },
            "data": units,
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get units: {str(exc)}"
        )


@router.get("/metadata/calendars", response_model=dict)
async def get_calendars_v1(
    request: Request,
    response: Response,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get calendar definitions (v1 compatibility)."""
    start_time = time.perf_counter()

    try:
        service = MetadataService()
        calendars = await service.get_calendars(limit=limit, offset=offset)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(calendars),
                "total": len(calendars),
                "offset": offset,
                "limit": limit,
            },
            "data": calendars,
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get calendars: {str(exc)}"
        )
