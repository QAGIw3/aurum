"""Metadata and reference data endpoints."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..telemetry.context import get_request_id
from .models import (
    DimensionsResponse,
    DimensionsData,
    DimensionsCountData,
    IsoLocationsResponse,
    IsoLocationOut,
    UnitsCanonicalResponse,
    UnitsCanonical,
    UnitsMappingResponse,
    UnitMappingOut,
    CalendarsResponse,
    CalendarOut,
    CalendarBlocksResponse,
    CalendarHoursResponse,
)
from .service import (
    fetch_metadata_dimensions,
    fetch_iso_locations,
    fetch_units_canonical,
    fetch_units_mapping,
    fetch_calendars,
    fetch_calendar_blocks,
    fetch_calendar_hours,
)

router = APIRouter()


@router.get("/v1/metadata/dimensions", response_model=DimensionsResponse, tags=["Metadata"])
async def get_dimensions(
    asof: Optional[str] = None,
    include_counts: bool = Query(False, description="Include dimension value counts"),
) -> DimensionsResponse:
    """Get available dimensions and values for filtering."""
    start_time = time.perf_counter()

    try:
        dimensions, counts = await fetch_metadata_dimensions(
            asof=asof,
            include_counts=include_counts,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return DimensionsResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=dimensions,
            counts=counts,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch metadata dimensions: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/locations", response_model=IsoLocationsResponse)
async def get_iso_locations(
    iso: Optional[str] = Query(None, description="Filter by ISO code"),
    prefix: Optional[str] = Query(None, description="Filter by location ID/name prefix"),
) -> IsoLocationsResponse:
    """Get ISO location registry for LMP enrichment."""
    start_time = time.perf_counter()

    try:
        locations = await fetch_iso_locations(iso=iso, prefix=prefix)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return IsoLocationsResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=locations,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ISO locations: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/locations/{iso}/{location_id}", response_model=IsoLocationOut)
async def get_iso_location(
    iso: str,
    location_id: str,
) -> IsoLocationOut:
    """Get specific ISO location details."""
    start_time = time.perf_counter()

    try:
        location = await fetch_iso_locations(iso=iso, location_ids=[location_id])

        if not location:
            raise HTTPException(
                status_code=404,
                detail=f"Location {location_id} not found for ISO {iso}"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return IsoLocationOut(
            **location[0].dict(),
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ISO location: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/units", response_model=UnitsCanonicalResponse)
async def get_units_canonical() -> UnitsCanonicalResponse:
    """Get canonical units and currencies."""
    start_time = time.perf_counter()

    try:
        units = await fetch_units_canonical()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return UnitsCanonicalResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=units,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch canonical units: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/units/mapping", response_model=UnitsMappingResponse)
async def get_units_mapping(
    prefix: Optional[str] = Query(None, description="Filter by raw unit string prefix"),
) -> UnitsMappingResponse:
    """Get unit mappings from raw strings to canonical currency/unit."""
    start_time = time.perf_counter()

    try:
        mappings = await fetch_units_mapping(prefix=prefix)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return UnitsMappingResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=mappings,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch units mapping: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/calendars", response_model=CalendarsResponse)
async def get_calendars() -> CalendarsResponse:
    """Get available calendar definitions."""
    start_time = time.perf_counter()

    try:
        calendars = await fetch_calendars()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CalendarsResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=calendars,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch calendars: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/calendars/{name}/blocks", response_model=CalendarBlocksResponse)
async def get_calendar_blocks(name: str) -> CalendarBlocksResponse:
    """Get block definitions for a calendar."""
    start_time = time.perf_counter()

    try:
        blocks = await fetch_calendar_blocks(name)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CalendarBlocksResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=blocks,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch calendar blocks: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/calendars/{name}/hours", response_model=CalendarHoursResponse)
async def get_calendar_hours(
    name: str,
    block: str = Query(..., description="Block name"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
) -> CalendarHoursResponse:
    """Get hours for a specific date and block."""
    start_time = time.perf_counter()

    try:
        hours = await fetch_calendar_hours(name, block, date)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CalendarHoursResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=hours,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch calendar hours: {str(exc)}"
        ) from exc


@router.get("/v1/metadata/calendars/{name}/expand", response_model=CalendarHoursResponse)
async def expand_calendar_block(
    name: str,
    block: str = Query(..., description="Block name"),
    start: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end: str = Query(..., description="End date in YYYY-MM-DD format"),
) -> CalendarHoursResponse:
    """Expand a block over a date range."""
    start_time = time.perf_counter()

    try:
        hours = await fetch_calendar_hours(name, block, start, end)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CalendarHoursResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=hours,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to expand calendar block: {str(exc)}"
        ) from exc
