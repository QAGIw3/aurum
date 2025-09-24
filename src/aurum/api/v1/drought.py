"""v1 Drought endpoints split from the monolith for maintainability.

Endpoints:
- /v1/drought/tiles/{dataset}/{index}/{timescale}/{z}/{x}/{y}.png
- /v1/drought/info/{dataset}/{index}/{timescale}

Enable via AURUM_API_V1_SPLIT_DROUGHT=1 in app wiring.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Response

from ..http.clients import request as http_request
from ..models import (
    Meta,
    DroughtInfoResponse,
    DroughtDimensionsResponse,
    DroughtDimensions,
    DroughtIndexResponse,
    DroughtIndexPoint,
    DroughtUsdmResponse,
    DroughtVectorResponse,
    DroughtVectorEventPoint,
)
from ..service import query_drought_indices, query_drought_usdm
from ...telemetry.context import get_request_id

# Reuse helpers and metrics from the monolith to avoid duplication
from ..routes import (  # type: ignore
    _drought_catalog,
    _tile_cache,
    _record_tile_cache_metric,
    _observe_tile_fetch,
    _TILE_CACHE_CFG,
    _parse_region_param,
)

router = APIRouter()


@router.get("/v1/drought/tiles/{dataset}/{index}/{timescale}/{z}/{x}/{y}.png", response_class=Response)
def proxy_drought_tile(
    dataset: str,
    index: str,
    timescale: str,
    z: int,
    x: int,
    y: int,
    valid_date: date = Query(..., description="Valid date for the tile (YYYY-MM-DD)."),
) -> Response:
    catalog = _drought_catalog()
    try:
        raster = catalog.raster_by_name(dataset.lower())
    except KeyError:
        raise HTTPException(status_code=404, detail="unknown_dataset")
    slug = valid_date.isoformat()
    try:
        tile_url = raster.xyz_url(index.upper(), timescale.upper(), slug, z, x, y)
    except Exception:
        raise HTTPException(status_code=404, detail="tile_not_available")

    cache_client = _tile_cache()
    cache_key = f"drought:tile:{dataset.lower()}:{index.upper()}:{timescale.upper()}:{slug}:{z}:{x}:{y}"
    if cache_client is not None:
        try:
            cached = cache_client.get(cache_key)
        except Exception:
            cached = None
            _record_tile_cache_metric("tile", "error")
        if cached:
            _record_tile_cache_metric("tile", "hit")
            return Response(content=cached, media_type="image/png")
        _record_tile_cache_metric("tile", "miss")
    else:
        _record_tile_cache_metric("tile", "bypass")

    start_fetch = time.perf_counter()
    try:
        resp = http_request("GET", tile_url, timeout=20.0)
    except Exception as exc:
        _observe_tile_fetch("tile", "error", time.perf_counter() - start_fetch)
        status = getattr(getattr(exc, "response", None), "status_code", 502) or 502
        raise HTTPException(status_code=status, detail="tile_fetch_failed") from exc

    duration = time.perf_counter() - start_fetch
    _observe_tile_fetch("tile", "success", duration)

    content = resp.content
    if cache_client is not None and _TILE_CACHE_CFG is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, content)
        except Exception:
            pass
    media_type = resp.headers.get("Content-Type", "image/png")
    return Response(content=content, media_type=media_type)


@router.get("/v1/drought/info/{dataset}/{index}/{timescale}", response_model=DroughtInfoResponse)
def get_drought_tile_metadata(
    dataset: str,
    index: str,
    timescale: str,
    valid_date: date = Query(..., description="Valid date for the raster (YYYY-MM-DD)."),
) -> DroughtInfoResponse:
    catalog = _drought_catalog()
    try:
        raster = catalog.raster_by_name(dataset.lower())
    except KeyError:
        raise HTTPException(status_code=404, detail="unknown_dataset")
    slug = valid_date.isoformat()
    try:
        info_url = raster.info_url(index.upper(), timescale.upper(), slug)
    except Exception:
        raise HTTPException(status_code=404, detail="info_not_available")

    cache_client = _tile_cache()
    cache_key = f"drought:info:{dataset.lower()}:{index.upper()}:{timescale.upper()}:{slug}"
    if cache_client is not None:
        try:
            cached = cache_client.get(cache_key)
        except Exception:
            cached = None
            _record_tile_cache_metric("info", "error")
        if cached:
            _record_tile_cache_metric("info", "hit")
            try:
                payload = json.loads(cached)
                meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=0)
                return DroughtInfoResponse(meta=meta, data=payload)
            except json.JSONDecodeError:
                pass
        else:
            _record_tile_cache_metric("info", "miss")
    else:
        _record_tile_cache_metric("info", "bypass")

    start_fetch = time.perf_counter()
    try:
        resp = http_request("GET", info_url, timeout=15.0)
    except Exception as exc:
        _observe_tile_fetch("info", "error", time.perf_counter() - start_fetch)
        status = getattr(getattr(exc, "response", None), "status_code", 502) or 502
        raise HTTPException(status_code=status, detail="info_fetch_failed") from exc

    duration = time.perf_counter() - start_fetch
    _observe_tile_fetch("info", "success", duration)

    payload = resp.json()
    if cache_client is not None and _TILE_CACHE_CFG is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, resp.text)
        except Exception:
            pass

    meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=0)
    return DroughtInfoResponse(meta=meta, data=payload)


@router.get("/v1/drought/dimensions", response_model=DroughtDimensionsResponse)
def get_drought_dimensions() -> DroughtDimensionsResponse:
    catalog = _drought_catalog()
    datasets = sorted({item.dataset for item in catalog.raster_indices})
    indices = sorted({idx for item in catalog.raster_indices for idx in item.indices})
    timescales = sorted({ts for item in catalog.raster_indices for ts in item.timescales})
    layers = sorted({layer.layer for layer in catalog.vector_layers if layer.layer != 'usdm'})
    region_types = sorted({region.region_type for region in catalog.region_sets})
    meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=0)
    data = DroughtDimensions(
        datasets=datasets,
        indices=indices,
        timescales=timescales,
        layers=layers,
        region_types=region_types,
    )
    return DroughtDimensionsResponse(meta=meta, data=data)


@router.get("/v1/drought/indices", response_model=DroughtIndexResponse)
def get_drought_indices(
    dataset: Optional[str] = Query(None, description="Source dataset (nclimgrid, acis, cpc, prism, eddi)."),
    index: Optional[str] = Query(None, description="Index name (SPI, SPEI, EDDI, PDSI, QuickDRI, VHI)."),
    timescale: Optional[str] = Query(None, description="Accumulation window such as 1M, 3M, 6M."),
    region: Optional[str] = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: Optional[str] = Query(None),
    region_id: Optional[str] = Query(None),
    start: Optional[date] = Query(None, description="Inclusive start date (YYYY-MM-DD)."),
    end: Optional[date] = Query(None, description="Inclusive end date (YYYY-MM-DD)."),
    limit: int = Query(250, ge=1, le=1000),
) -> DroughtIndexResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)

    # Delegate to service
    from ..config import TrinoConfig
    from ..state import get_settings
    trino_cfg = TrinoConfig.from_settings(get_settings())
    rows, elapsed = query_drought_indices(
        trino_cfg,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        dataset=dataset,
        index_id=index,
        timescale=timescale,
        start_date=start,
        end_date=end,
        limit=limit,
    )
    payload: List[DroughtIndexPoint] = []
    for row in rows:
        payload.append(
            DroughtIndexPoint(
                series_id=row["series_id"],
                dataset=row["dataset"],
                index=row["index"],
                timescale=row["timescale"],
                valid_date=row["valid_date"],
                as_of=row.get("as_of"),
                value=row.get("value"),
                unit=row.get("unit"),
                poc=row.get("poc"),
                region_type=row.get("region_type"),
                region_id=row.get("region_id"),
                region_name=row.get("region_name"),
                parent_region_id=row.get("parent_region_id"),
                source_url=row.get("source_url"),
                metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else None,
            )
        )
    meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed))
    return DroughtIndexResponse(meta=meta, data=payload)


@router.get("/v1/drought/usdm", response_model=DroughtUsdmResponse)
def get_drought_usdm(
    region: Optional[str] = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: Optional[str] = Query(None),
    region_id: Optional[str] = Query(None),
    start: Optional[date] = Query(None, description="Inclusive start date (YYYY-MM-DD)."),
    end: Optional[date] = Query(None, description="Inclusive end date (YYYY-MM-DD)."),
    limit: int = Query(250, ge=1, le=1000),
) -> DroughtUsdmResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)

    from ..config import TrinoConfig
    from ..state import get_settings
    trino_cfg = TrinoConfig.from_settings(get_settings())
    rows, elapsed = query_drought_usdm(
        trino_cfg,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        start_date=start,
        end_date=end,
        limit=limit,
    )
    payload = []
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None
        payload.append(
            {
                "region_type": row.get("region_type"),
                "region_id": row.get("region_id"),
                "region_name": row.get("region_name"),
                "parent_region_id": row.get("parent_region_id"),
                "valid_date": row.get("valid_date"),
                "as_of": row.get("as_of"),
                "d0_frac": row.get("d0_frac"),
                "d1_frac": row.get("d1_frac"),
                "d2_frac": row.get("d2_frac"),
                "d3_frac": row.get("d3_frac"),
                "d4_frac": row.get("d4_frac"),
                "source_url": row.get("source_url"),
                "metadata": metadata if isinstance(metadata, dict) else None,
            }
        )
    meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed))
    return DroughtUsdmResponse(meta=meta, data=payload)


@router.get("/v1/drought/layers", response_model=DroughtVectorResponse, include_in_schema=False)
def get_drought_layers(
    layer: Optional[str] = Query(None, description="Layer key (qpf, ahps_flood, aqi, wildfire)."),
    region: Optional[str] = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: Optional[str] = Query(None),
    region_id: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(250, ge=1, le=1000),
) -> DroughtVectorResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    from ..config import TrinoConfig
    from ..state import get_settings
    trino_cfg = TrinoConfig.from_settings(get_settings())
    from ..service import query_drought_vector_events
    rows, elapsed = query_drought_vector_events(
        trino_cfg,
        layer=layer.lower() if layer else None,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        start_time=start,
        end_time=end,
        limit=limit,
    )
    payload: List[DroughtVectorEventPoint] = []
    for row in rows:
        props = row.get("properties")
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except json.JSONDecodeError:
                props = None
        payload.append(
            DroughtVectorEventPoint(
                layer=row["layer"],
                event_id=row["event_id"],
                region_type=row.get("region_type"),
                region_id=row.get("region_id"),
                region_name=row.get("region_name"),
                parent_region_id=row.get("parent_region_id"),
                valid_start=row.get("valid_start"),
                valid_end=row.get("valid_end"),
                value=row.get("value"),
                unit=row.get("unit"),
                category=row.get("category"),
                severity=row.get("severity"),
                source_url=row.get("source_url"),
                geometry_wkt=row.get("geometry_wkt"),
                properties=props if isinstance(props, dict) else None,
            )
        )
    meta = Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed))
    return DroughtVectorResponse(meta=meta, data=payload)
