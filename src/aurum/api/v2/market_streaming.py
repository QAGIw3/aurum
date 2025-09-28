"""v2 API endpoints for real-time market streaming and diagnostics."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from ..websocket.market_feeds import get_market_data_service
from ...streaming import MarketDataEvent


router = APIRouter(prefix="/v2/market", tags=["Market"])


class IngestRequest(BaseModel):
    tenor: str
    price: float
    timestamp: Optional[datetime] = None
    vendor: Optional[str] = None
    volume: Optional[float] = None
    metadata: Mapping[str, Any] = Field(default_factory=dict)


class HistoricalPoint(BaseModel):
    tenor: str
    price: float
    timestamp: Optional[datetime] = None
    volume: Optional[float] = None


class HistoricalSeedRequest(BaseModel):
    points: List[HistoricalPoint]


@router.get("/curves/{curve_id}/snapshot")
async def get_snapshot(curve_id: str) -> Mapping[str, Any]:
    service = get_market_data_service()
    snapshot = await service.engine.get_snapshot(curve_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No snapshot available for curve")
    return snapshot.to_dict()


@router.get("/curves/{curve_id}/reconciliation")
async def get_reconciliation(curve_id: str) -> Mapping[str, Any]:
    service = get_market_data_service()
    reconciliation = await service.engine.get_reconciliation(curve_id)
    if reconciliation is None:
        raise HTTPException(status_code=404, detail="No reconciliation available for curve")
    return reconciliation.to_dict()


@router.post("/curves/{curve_id}/ingest")
async def post_ingest(curve_id: str, body: IngestRequest = Body(...)) -> Mapping[str, Any]:
    service = get_market_data_service()
    timestamp = body.timestamp or datetime.now(timezone.utc)
    event = MarketDataEvent(
        curve_id=curve_id,
        tenor=body.tenor,
        price=body.price,
        timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc),
        vendor=body.vendor,
        volume=body.volume,
        metadata=dict(body.metadata or {}),
    )
    report = await service.ingest_event(event)
    return {
        "status": "ingested",
        "report": report.to_payload(),
        "metrics": {
            "events_ingested": service.metrics.events_ingested,
            "alerts_emitted": service.metrics.alerts_emitted,
        },
    }


@router.post("/curves/{curve_id}/historical")
async def post_historical(curve_id: str, body: HistoricalSeedRequest = Body(...)) -> Mapping[str, Any]:
    service = get_market_data_service()
    payload = [
        {
            "tenor": p.tenor,
            "price": p.price,
            "timestamp": (p.timestamp or datetime.now(timezone.utc)).isoformat(),
            "volume": p.volume,
        }
        for p in (body.points or [])
    ]
    await service.engine.add_historical_curve(curve_id, payload)
    reconciliation = await service.engine.get_reconciliation(curve_id)
    return {
        "status": "seeded",
        "curve_id": curve_id,
        "historical_points": len(payload),
        "reconciliation": reconciliation.to_dict() if reconciliation else None,
    }


@router.get("/metrics")
async def get_metrics() -> Mapping[str, Any]:
    service = get_market_data_service()
    kafka = service.kafka
    return {
        "service": {
            "events_ingested": service.metrics.events_ingested,
            "alerts_emitted": service.metrics.alerts_emitted,
            "last_ingested_at": service.metrics.last_ingested_at.isoformat() if service.metrics.last_ingested_at else None,
            "last_curve_id": service.metrics.last_curve_id,
            "last_reconciliation_delta": service.metrics.last_reconciliation_delta,
        },
        "kafka": {
            "processed": kafka.metrics.processed,
            "failed": kafka.metrics.failed,
            "backpressure_events": kafka.metrics.backpressure_events,
            "circuit_open_events": kafka.metrics.circuit_open_events,
            "topic_counts": dict(kafka.metrics.topic_counts),
            "last_error": kafka.metrics.last_error,
        },
    }


__all__ = ["router"]

