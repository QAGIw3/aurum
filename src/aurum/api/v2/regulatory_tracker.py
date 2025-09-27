"""v2 Regulatory Tracker API for policy ingestion and compliance monitoring.

This module provides REST endpoints for:
- Regulatory artifact ingestion from RSS/API sources
- Policy metadata and compliance tracking
- NLP-based tagging of affected markets/instruments
- Regulatory impact analysis for portfolios
- Alert management and compliance monitoring
- Integration with forecasting and risk management
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.regulatory_tracker_service import (
    get_regulatory_tracker_service,
    RegulatoryArtifact,
    RegulatoryAlert,
    PolicyImpactLevel
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/regulatory-tracker", tags=["regulatory-tracker"])


class RegulatoryArtifactResponse(BaseModel):
    """Response containing regulatory artifact information."""

    artifact_id: str
    source: str
    title: str
    summary: str
    publication_date: datetime
    effective_date: Optional[datetime]
    expiry_date: Optional[datetime]
    url: str
    document_type: str
    status: str
    affected_markets: List[str]
    affected_instruments: List[str]
    nlp_tags: Dict[str, List[str]]
    impact_level: str
    compliance_deadline: Optional[datetime]
    metadata: Dict[str, any]
    created_at: datetime


class RegulatoryArtifactListResponse(BaseModel):
    """Response for listing regulatory artifacts."""

    data: List[RegulatoryArtifactResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class RegulatoryAlertResponse(BaseModel):
    """Response containing regulatory alert information."""

    alert_id: str
    artifact_id: str
    alert_type: str
    severity: str
    title: str
    message: str
    affected_portfolios: List[str]
    affected_assets: List[str]
    action_required: str
    deadline: Optional[datetime]
    created_at: datetime


class RegulatoryAlertListResponse(BaseModel):
    """Response for listing regulatory alerts."""

    data: List[RegulatoryAlertResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class PortfolioImpactResponse(BaseModel):
    """Response containing portfolio regulatory impact."""

    portfolio_id: str
    total_artifacts: int
    high_impact_artifacts: int
    critical_impact_artifacts: int
    risk_score: float
    compliance_deadlines: List[datetime]
    affected_markets: List[str]
    affected_instruments: List[str]


class MarketSummaryResponse(BaseModel):
    """Response containing market regulatory summary."""

    market: str
    total_artifacts: int
    recent_artifacts: int
    high_impact_artifacts: int
    critical_impact_artifacts: int
    affected_instruments: List[str]
    compliance_deadlines: List[datetime]


@router.post("/ingest", response_model=Dict[str, any], status_code=202)
async def ingest_regulatory_updates(
    request: Request
) -> Dict[str, any]:
    """Trigger regulatory data ingestion from all sources."""
    start_time = time.perf_counter()

    try:
        from ..services.regulatory_tracker_service import ingest_regulatory_updates

        # Ingest regulatory updates
        artifacts = await ingest_regulatory_updates()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="ingest_regulatory_updates",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="ingest_regulatory_updates",
                query_time_ms=query_time_ms
            ),
            "data": {
                "artifacts_ingested": len(artifacts),
                "sources_processed": 5,  # Mock count
                "message": "Regulatory ingestion completed successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="ingest_regulatory_updates",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest regulatory updates: {str(exc)}"
        )


@router.get("/artifacts", response_model=RegulatoryArtifactListResponse)
async def list_regulatory_artifacts(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source: Optional[str] = Query(None, description="Filter by source"),
    market: Optional[str] = Query(None, description="Filter by affected market"),
    instrument: Optional[str] = Query(None, description="Filter by affected instrument"),
    impact_level: Optional[str] = Query(None, description="Filter by impact level")
) -> RegulatoryArtifactListResponse:
    """List regulatory artifacts with filtering."""
    start_time = time.perf_counter()

    try:
        service = get_regulatory_tracker_service()

        # Get artifacts with filters (mock implementation)
        artifacts = list(service._artifacts.values())

        # Apply filters
        if source:
            artifacts = [a for a in artifacts if a.source.value == source]
        if market:
            artifacts = [a for a in artifacts if market in a.affected_markets]
        if instrument:
            artifacts = [a for a in artifacts if instrument in a.affected_instruments]
        if impact_level:
            artifacts = [a for a in artifacts if a.impact_level.value == impact_level]

        # Apply pagination
        paginated_artifacts = artifacts[offset:offset + limit]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        artifact_responses = [
            RegulatoryArtifactResponse(
                artifact_id=artifact.artifact_id,
                source=artifact.source.value,
                title=artifact.title,
                summary=artifact.summary,
                publication_date=artifact.publication_date,
                effective_date=artifact.effective_date,
                expiry_date=artifact.expiry_date,
                url=artifact.url,
                document_type=artifact.document_type,
                status=artifact.status,
                affected_markets=artifact.affected_markets,
                affected_instruments=artifact.affected_instruments,
                nlp_tags=artifact.nlp_tags,
                impact_level=artifact.impact_level.value,
                compliance_deadline=artifact.compliance_deadline,
                metadata=artifact.metadata,
                created_at=artifact.created_at
            )
            for artifact in paginated_artifacts
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_regulatory_artifacts",
            query_time_ms=query_time_ms,
            record_count=len(artifact_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return RegulatoryArtifactListResponse(
            data=artifact_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_regulatory_artifacts",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list regulatory artifacts: {str(exc)}"
        )


@router.get("/artifacts/{artifact_id}", response_model=RegulatoryArtifactResponse)
async def get_regulatory_artifact(
    request: Request,
    artifact_id: str
) -> RegulatoryArtifactResponse:
    """Get a specific regulatory artifact."""
    start_time = time.perf_counter()

    try:
        service = get_regulatory_tracker_service()
        artifact = service._artifacts.get(artifact_id)

        if not artifact:
            raise HTTPException(status_code=404, detail="Regulatory artifact not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_regulatory_artifact",
            query_time_ms=query_time_ms
        )

        return RegulatoryArtifactResponse(
            artifact_id=artifact.artifact_id,
            source=artifact.source.value,
            title=artifact.title,
            summary=artifact.summary,
            publication_date=artifact.publication_date,
            effective_date=artifact.effective_date,
            expiry_date=artifact.expiry_date,
            url=artifact.url,
            document_type=artifact.document_type,
            status=artifact.status,
            affected_markets=artifact.affected_markets,
            affected_instruments=artifact.affected_instruments,
            nlp_tags=artifact.nlp_tags,
            impact_level=artifact.impact_level.value,
            compliance_deadline=artifact.compliance_deadline,
            metadata=artifact.metadata,
            created_at=artifact.created_at
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_regulatory_artifact",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get regulatory artifact: {str(exc)}"
        )


@router.get("/markets/{market}/summary", response_model=MarketSummaryResponse)
async def get_market_regulatory_summary(
    request: Request,
    market: str
) -> MarketSummaryResponse:
    """Get regulatory summary for a specific market."""
    start_time = time.perf_counter()

    try:
        from ..services.regulatory_tracker_service import get_market_regulatory_summary

        # Get market summary
        summary = await get_market_regulatory_summary(market)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_market_regulatory_summary",
            query_time_ms=query_time_ms
        )

        return MarketSummaryResponse(
            market=summary["market"],
            total_artifacts=summary["total_artifacts"],
            recent_artifacts=summary["recent_artifacts"],
            high_impact_artifacts=summary["high_impact_artifacts"],
            critical_impact_artifacts=summary["critical_impact_artifacts"],
            affected_instruments=summary["affected_instruments"],
            compliance_deadlines=summary["compliance_deadlines"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_market_regulatory_summary",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get market regulatory summary: {str(exc)}"
        )


@router.get("/portfolios/{portfolio_id}/impact", response_model=PortfolioImpactResponse)
async def get_portfolio_regulatory_impact(
    request: Request,
    portfolio_id: str
) -> PortfolioImpactResponse:
    """Get regulatory impact analysis for a portfolio."""
    start_time = time.perf_counter()

    try:
        from ..services.regulatory_tracker_service import get_regulatory_impact_for_portfolio

        # Get portfolio impact
        impact = await get_regulatory_impact_for_portfolio(portfolio_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_portfolio_regulatory_impact",
            query_time_ms=query_time_ms
        )

        return PortfolioImpactResponse(
            portfolio_id=impact["portfolio_id"],
            total_artifacts=impact["total_artifacts"],
            high_impact_artifacts=impact["high_impact_artifacts"],
            critical_impact_artifacts=impact["critical_impact_artifacts"],
            risk_score=impact["risk_score"],
            compliance_deadlines=impact["compliance_deadlines"],
            affected_markets=impact["affected_markets"],
            affected_instruments=impact["affected_instruments"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_portfolio_regulatory_impact",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get portfolio regulatory impact: {str(exc)}"
        )


@router.get("/alerts", response_model=RegulatoryAlertListResponse)
async def list_regulatory_alerts(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    severity: Optional[str] = Query(None, description="Filter by severity")
) -> RegulatoryAlertListResponse:
    """List regulatory alerts with filtering."""
    start_time = time.perf_counter()

    try:
        service = get_regulatory_tracker_service()

        # Get alerts
        alerts = await service.get_regulatory_alerts(limit + offset)

        # Apply client-side filtering
        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        # Apply pagination
        paginated_alerts = alerts[offset:offset + limit]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        alert_responses = [
            RegulatoryAlertResponse(
                alert_id=alert.alert_id,
                artifact_id=alert.artifact_id,
                alert_type=alert.alert_type,
                severity=alert.severity,
                title=alert.title,
                message=alert.message,
                affected_portfolios=alert.affected_portfolios,
                affected_assets=alert.affected_assets,
                action_required=alert.action_required,
                deadline=alert.deadline,
                created_at=alert.created_at
            )
            for alert in paginated_alerts
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_regulatory_alerts",
            query_time_ms=query_time_ms,
            record_count=len(alert_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return RegulatoryAlertListResponse(
            data=alert_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_regulatory_alerts",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list regulatory alerts: {str(exc)}"
        )


@router.get("/health")
async def get_regulatory_tracker_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get regulatory tracker service health status."""
    start_time = time.perf_counter()

    try:
        service = get_regulatory_tracker_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_regulatory_tracker_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_regulatory_tracker_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get regulatory tracker health: {str(exc)}"
        )
