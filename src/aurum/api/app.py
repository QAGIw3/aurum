from __future__ import annotations

"""FastAPI application exposing curve endpoints."""

import base64
import json
import uuid
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

from .config import CacheConfig, TrinoConfig
from . import service
from .models import CurveDiffPoint, CurveDiffResponse, CurvePoint, CurveResponse, DimensionsData, DimensionsResponse, Meta
from .ratelimit import RateLimitConfig, RateLimitMiddleware
from .auth import AuthMiddleware, OIDCConfig


app = FastAPI(title="Aurum API", version="0.1.0")

# CORS (configurable via env AURUM_API_CORS_ORIGINS, comma-separated)
import os
origins_raw = os.getenv("AURUM_API_CORS_ORIGINS", "")
origins = [o.strip() for o in origins_raw.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Attach simple rate limiting middleware (Redis-backed when configured)
app.add_middleware(
    RateLimitMiddleware,
    cache_cfg=CacheConfig.from_env(),
    rl_cfg=RateLimitConfig.from_env(),
)

# Prometheus metrics
try:  # pragma: no cover
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    REQUEST_COUNTER = Counter(
        "aurum_api_requests_total", "Total API requests", ["method", "path", "status"]
    )
    REQUEST_LATENCY = Histogram(
        "aurum_api_request_duration_seconds", "API request duration in seconds", ["method", "path"]
    )

    @app.middleware("http")
    async def _metrics_middleware(request, call_next):  # type: ignore[no-redef]
        method = request.method
        path = request.url.path
        with REQUEST_LATENCY.labels(method=method, path=path).time():
            response = await call_next(request)
        REQUEST_COUNTER.labels(method=method, path=path, status=str(response.status_code)).inc()
        return response

    @app.get("/metrics")
    def metrics():
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)
except Exception:  # pragma: no cover
    pass


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

# Register auth middleware after health/metrics route definitions
app.add_middleware(AuthMiddleware, config=OIDCConfig.from_env())


def _encode_cursor(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(token: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc


@app.get("/v1/curves", response_model=CurveResponse)
def list_curves(
    asof: Optional[date] = Query(None, description="As-of date filter (YYYY-MM-DD)"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
) -> CurveResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    # Cursor overrides offset
    if cursor:
        payload = _decode_cursor(cursor)
        offset = int(payload.get("offset", 0))

    try:
        # Fetch one extra row to determine if there is a next page
        rows, elapsed_ms = service.query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=limit + 1,
            offset=offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_cursor({"offset": offset + limit})

    return CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )


@app.get("/v1/curves/diff", response_model=CurveDiffResponse)
def list_curves_diff(
    asof_a: date = Query(..., description="First as-of date"),
    asof_b: date = Query(..., description="Second as-of date"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
) -> CurveDiffResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    if cursor:
        payload = _decode_cursor(cursor)
        offset = int(payload.get("offset", 0))

    try:
        rows, elapsed_ms = service.query_curves_diff(
            trino_cfg,
            cache_cfg,
            asof_a=asof_a,
            asof_b=asof_b,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=limit + 1,
            offset=offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_cursor({"offset": offset + limit})

    return CurveDiffResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurveDiffPoint(**row) for row in rows],
    )


@app.get("/v1/metadata/dimensions", response_model=DimensionsResponse)
def list_dimensions(
    asof: Optional[date] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    per_dim_limit: int = Query(1000, ge=1, le=5000),
    prefix: Optional[str] = Query(None, description="Optional case-insensitive startswith filter applied to each dimension list"),
) -> DimensionsResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    try:
        results = service.query_dimensions(
            trino_cfg,
            cache_cfg,
            asof=asof,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            per_dim_limit=per_dim_limit,
        )
    except RuntimeError as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if prefix:
        pfx = prefix.lower()
        for k, vals in list(results.items()):
            results[k] = [v for v in vals if isinstance(v, str) and v.lower().startswith(pfx)]

    return DimensionsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=DimensionsData(**results))


@app.get("/v1/curves/strips", response_model=CurveResponse)
def list_strips(
    asof: Optional[date] = Query(None),
    type: str = Query(..., pattern="^(CALENDAR|SEASON|QUARTER)$", description="Strip type"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
) -> CurveResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    if cursor:
        payload = _decode_cursor(cursor)
        offset = int(payload.get("offset", 0))

    try:
        rows, elapsed_ms = service.query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=type,
            limit=limit + 1,
            offset=offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_cursor({"offset": offset + limit})

    return CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )


__all__ = ["app"]
