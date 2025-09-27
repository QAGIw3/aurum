"""v1 EIA endpoints split from the monolith for maintainability.

This router mirrors existing v1 behavior for:
- EIA dataset listing and detail
- EIA series listing (JSON/CSV)
- EIA series dimensions

The implementation delegates data access to ``aurum.api.service`` and
uses the reference catalog for dataset metadata.

Enable inclusion via AURUM_API_V1_SPLIT_EIA=1 in app wiring.
"""

from __future__ import annotations

import time
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from ..http import respond_with_etag, csv_response
from ..models import (
    Meta,
    EiaDatasetBriefOut,
    EiaDatasetsResponse,
    EiaDatasetDetailOut,
    EiaDatasetResponse,
    EiaSeriesPoint,
    EiaSeriesResponse,
    EiaSeriesDimensionsData,
    EiaSeriesDimensionsResponse,
)
from ..service import (
    query_eia_series,
    query_eia_series_dimensions,
)
from ..config import TrinoConfig, CacheConfig
from ..state import get_settings
from ...reference import eia_catalog as ref_eia
from ...telemetry.context import get_request_id


router = APIRouter()


@router.get("/v1/metadata/eia/datasets", response_model=EiaDatasetsResponse)
def list_eia_datasets(
    request: Request,
    response: Response,
    prefix: Optional[str] = Query(None, description="Startswith filter on path or name"),
) -> EiaDatasetsResponse:
    request_id = get_request_id() or "unknown"

    def _all() -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for ds in ref_eia.iter_datasets():
            items.append(
                {
                    "path": ds.path,
                    "name": ds.name,
                    "description": ds.description,
                    "default_frequency": ds.default_frequency,
                    "start_period": ds.start_period,
                    "end_period": ds.end_period,
                }
            )
        items.sort(key=lambda d: d.get("path", ""))
        return items

    base = _all()
    if prefix:
        pfx = prefix.lower()
        base = [
            d
            for d in base
            if str(d.get("path", "")).lower().startswith(pfx)
            or str(d.get("name") or "").lower().startswith(pfx)
        ]

    model = EiaDatasetsResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=[EiaDatasetBriefOut(**d) for d in base],
    )
    return respond_with_etag(model, request, response)


@router.get("/v1/metadata/eia/datasets/{dataset_path:path}", response_model=EiaDatasetResponse)
def get_eia_dataset(
    request: Request,
    response: Response,
    dataset_path: str,
) -> EiaDatasetResponse:
    request_id = get_request_id() or "unknown"
    try:
        ds = ref_eia.get_dataset(dataset_path)
    except ref_eia.DatasetNotFoundError as exc:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Dataset not found") from exc

    detail = EiaDatasetDetailOut(
        path=ds.path,
        name=ds.name,
        description=ds.description,
        frequencies=list(ds.frequencies),
        facets=list(ds.facets),
        data_columns=list(ds.data_columns),
        start_period=ds.start_period,
        end_period=ds.end_period,
        default_frequency=ds.default_frequency,
        default_date_format=ds.default_date_format,
    )

    model = EiaDatasetResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=detail)
    return respond_with_etag(model, request, response)


@router.get("/v1/ref/eia/series", response_model=EiaSeriesResponse)
def list_eia_series(
    request: Request,
    response: Response,
    series_id: Optional[str] = Query(None, description="Exact series identifier"),
    frequency: Optional[str] = Query(None, description="Frequency (ANNUAL, MONTHLY, etc.)"),
    area: Optional[str] = Query(None, description="Area/region filter"),
    sector: Optional[str] = Query(None, description="Sector/category filter"),
    dataset: Optional[str] = Query(None, description="Dataset identifier"),
    unit: Optional[str] = Query(None, description="Unit filter"),
    canonical_unit: Optional[str] = Query(None, description="Normalized unit filter"),
    canonical_currency: Optional[str] = Query(None, description="Canonical currency filter"),
    source: Optional[str] = Query(None, description="Source filter"),
    start: Optional[str] = Query(None, description="period_start >= this timestamp (ISO 8601)"),
    end: Optional[str] = Query(None, description="period_start <= this timestamp (ISO 8601)"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None, description="Cursor (offset-based token)"),
    format: Optional[str] = Query(None, description="csv for CSV response"),
) -> EiaSeriesResponse | StreamingResponse:
    request_id = get_request_id() or "unknown"
    start_time = time.perf_counter()

    # Parse start/end
    from datetime import datetime as _dt
    start_ts = None
    end_ts = None
    try:
        if start:
            start_ts = _dt.fromisoformat(start)
        if end:
            end_ts = _dt.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid timestamp format for start/end")

    settings = get_settings()
    trino_cfg = TrinoConfig.from_settings(settings)
    cache_cfg = CacheConfig.from_settings(settings)

    rows, elapsed_ms = query_eia_series(
        trino_cfg=trino_cfg,
        cache_cfg=cache_cfg,
        series_id=series_id,
        frequency=frequency,
        area=area,
        sector=sector,
        dataset=dataset,
        unit=unit,
        canonical_unit=canonical_unit,
        canonical_currency=canonical_currency,
        source=source,
        start=start_ts,
        end=end_ts,
        limit=limit,
        offset=offset,
        cursor_after=None,
        cursor_before=None,
        descending=False,
    )

    if (format or "").lower() == "csv":
        # Stream CSV with a standard field order
        fieldnames = [
            "series_id",
            "period",
            "period_start",
            "period_end",
            "frequency",
            "value",
            "raw_value",
            "unit",
            "canonical_unit",
            "canonical_currency",
            "canonical_value",
            "conversion_factor",
            "area",
            "sector",
            "seasonal_adjustment",
            "description",
            "source",
            "dataset",
            "metadata",
            "ingest_ts",
        ]

        async def _rows() -> AsyncIterator[Mapping[str, Any]]:
            for r in rows:
                yield r

        filename = "eia_series.csv"
        return csv_response(request_id, _rows(), fieldnames, filename)

    items: List[EiaSeriesPoint] = []
    for row in rows:
        # Basic passthrough; upstream may supply canonical fields
        items.append(EiaSeriesPoint(**row))

    model = EiaSeriesResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(round(elapsed_ms or ((time.perf_counter() - start_time) * 1000), 2))),
        data=items,
    )
    return respond_with_etag(model, request, response)


@router.get("/v1/ref/eia/series/dimensions", response_model=EiaSeriesDimensionsResponse)
def list_eia_series_dimensions(
    request: Request,
    response: Response,
    series_id: Optional[str] = Query(None),
    frequency: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    dataset: Optional[str] = Query(None),
    unit: Optional[str] = Query(None),
    canonical_unit: Optional[str] = Query(None),
    canonical_currency: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
) -> EiaSeriesDimensionsResponse:
    request_id = get_request_id() or "unknown"
    settings = get_settings()
    trino_cfg = TrinoConfig.from_settings(settings)
    cache_cfg = CacheConfig.from_settings(settings)
    try:
        values, elapsed_ms = query_eia_series_dimensions(
            trino_cfg=trino_cfg,
            cache_cfg=cache_cfg,
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    data = EiaSeriesDimensionsData(
        dataset=values.get("dataset"),
        area=values.get("area"),
        sector=values.get("sector"),
        unit=values.get("unit"),
        canonical_unit=values.get("canonical_unit"),
        canonical_currency=values.get("canonical_currency"),
        frequency=values.get("frequency"),
        source=values.get("source"),
    )
    model = EiaSeriesDimensionsResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(round(elapsed_ms, 2))),
        data=data,
    )
    return respond_with_etag(model, request, response)
