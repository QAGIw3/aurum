"""v2 Explainability API for SHAP/feature attribution and model interpretability.

This module provides REST endpoints for:
- Retrieving forecast explanations and feature attributions
- Downloading explanation visualizations and plots
- Getting top drivers summaries for model predictions
- Managing explanation artifacts and metadata
- Integration with forecasting and model registry
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.explainability_service import (
    get_explainability_service,
    ExplanationArtifact,
    ExplanationSummary,
    ExplanationVisualization,
    FeatureAttribution,
    ExplanationConfig
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/explainability", tags=["explainability"])


class ExplanationRequest(BaseModel):
    """Request for model explanation generation."""

    forecast_id: str = Field(..., description="Forecast ID to explain")
    model_version_id: str = Field(..., description="Model version to use for explanation")
    explanation_method: str = Field("shap", description="Explanation method (shap, lime, integrated_gradients)")
    include_visualizations: bool = Field(True, description="Generate explanation visualizations")
    max_features: int = Field(20, description="Maximum number of top features to include")


class ExplanationResponse(BaseModel):
    """Response containing explanation information."""

    explanation_id: str
    forecast_id: str
    model_version_id: str
    explanation_method: str
    feature_attributions: List[Dict[str, any]]
    top_drivers: List[Dict[str, any]]
    summary_text: str
    confidence_score: float
    created_at: datetime
    visualizations: List[Dict[str, any]]


class ExplanationListResponse(BaseModel):
    """Response for listing explanations."""

    data: List[ExplanationResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class TopDriversResponse(BaseModel):
    """Response containing top drivers for a forecast."""

    forecast_id: str
    model_version_id: str
    top_drivers: List[Dict[str, any]]
    summary_text: str
    confidence_score: float
    key_insights: List[str]
    risk_factors: List[str]
    recommendations: List[str]


class VisualizationResponse(BaseModel):
    """Response containing visualization information."""

    visualization_id: str
    explanation_id: str
    visualization_type: str
    file_path: str
    file_size: int
    mime_type: str
    created_at: datetime
    metadata: Dict[str, any]


class VisualizationListResponse(BaseModel):
    """Response for listing visualizations."""

    data: List[VisualizationResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


@router.post("/forecasts/{forecast_id}/explain", response_model=ExplanationResponse, status_code=201)
async def explain_forecast_prediction(
    request: Request,
    forecast_id: str,
    explanation_data: ExplanationRequest
) -> ExplanationResponse:
    """Generate explanation for a forecast prediction."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Generate explanation
        explanation = await service.explain_forecast(
            forecast_id=forecast_id,
            model_version_id=explanation_data.model_version_id,
            explanation_method=explanation_data.explanation_method,
            include_visualizations=explanation_data.include_visualizations,
            max_features=explanation_data.max_features
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="explain_forecast_prediction",
            query_time_ms=query_time_ms
        )

        return ExplanationResponse(
            explanation_id=explanation.artifact_id,
            forecast_id=explanation.forecast_id,
            model_version_id=explanation.model_version_id,
            explanation_method=explanation.explanation_method,
            feature_attributions=[
                {
                    "feature_name": attr.feature_name,
                    "attribution_score": attr.attribution_score,
                    "absolute_score": attr.absolute_score,
                    "rank": attr.rank,
                    "percentile": attr.percentile,
                    "feature_type": attr.feature_type,
                    "description": attr.description,
                    "importance_category": attr.importance_category
                }
                for attr in explanation.feature_attributions
            ],
            top_drivers=[],  # Would be populated from summary
            summary_text="",  # Would be populated from summary
            confidence_score=0.8,  # Mock value
            created_at=explanation.created_at,
            visualizations=[]  # Would be populated from visualizations
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="explain_forecast_prediction",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to explain forecast prediction: {str(exc)}"
        )


@router.get("/forecasts/{forecast_id}/explanations", response_model=ExplanationListResponse)
async def list_forecast_explanations(
    request: Request,
    response: Response,
    forecast_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
) -> ExplanationListResponse:
    """List explanations for a forecast."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get explanations (mock implementation)
        explanations = await service.get_forecast_explanations(
            forecast_id=forecast_id,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        explanation_responses = [
            ExplanationResponse(
                explanation_id=exp.artifact_id,
                forecast_id=exp.forecast_id,
                model_version_id=exp.model_version_id,
                explanation_method=exp.explanation_method,
                feature_attributions=[
                    {
                        "feature_name": attr.feature_name,
                        "attribution_score": attr.attribution_score,
                        "absolute_score": attr.absolute_score,
                        "rank": attr.rank,
                        "percentile": attr.percentile,
                        "feature_type": attr.feature_type,
                        "description": attr.description,
                        "importance_category": attr.importance_category
                    }
                    for attr in exp.feature_attributions
                ],
                top_drivers=[],  # Would be populated from summary
                summary_text="",  # Would be populated from summary
                confidence_score=0.8,  # Mock value
                created_at=exp.created_at,
                visualizations=[]  # Would be populated from visualizations
            )
            for exp in explanations
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_forecast_explanations",
            query_time_ms=query_time_ms,
            record_count=len(explanation_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return ExplanationListResponse(
            data=explanation_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_forecast_explanations",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list forecast explanations: {str(exc)}"
        )


@router.get("/forecasts/{forecast_id}/top-drivers", response_model=TopDriversResponse)
async def get_forecast_top_drivers(
    request: Request,
    forecast_id: str,
    model_version_id: Optional[str] = Query(None, description="Specific model version")
) -> TopDriversResponse:
    """Get top drivers for a forecast prediction."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get top drivers (mock implementation)
        top_drivers = await service.get_top_drivers(
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            limit=10
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_forecast_top_drivers",
            query_time_ms=query_time_ms
        )

        return TopDriversResponse(
            forecast_id=forecast_id,
            model_version_id=model_version_id or "latest",
            top_drivers=[
                {
                    "feature_name": driver.feature_name,
                    "attribution_score": driver.attribution_score,
                    "absolute_score": driver.absolute_score,
                    "rank": driver.rank,
                    "percentile": driver.percentile,
                    "feature_type": driver.feature_type,
                    "description": driver.description,
                    "importance_category": driver.importance_category
                }
                for driver in top_drivers
            ],
            summary_text="Top drivers analysis shows weather and load factors as primary influences.",  # Mock
            confidence_score=0.85,  # Mock
            key_insights=["Temperature strongly influences load", "Price volatility is high"],  # Mock
            risk_factors=["Weather dependency", "Market volatility"],  # Mock
            recommendations=["Monitor weather patterns", "Implement hedging strategies"]  # Mock
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_forecast_top_drivers",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get forecast top drivers: {str(exc)}"
        )


@router.get("/explanations/{explanation_id}/summary", response_model=TopDriversResponse)
async def get_explanation_summary(
    request: Request,
    explanation_id: str
) -> TopDriversResponse:
    """Get explanation summary with insights and recommendations."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get explanation summary (mock implementation)
        summary = await service.get_explanation_summary(explanation_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_explanation_summary",
            query_time_ms=query_time_ms
        )

        return TopDriversResponse(
            forecast_id=summary.forecast_id,
            model_version_id=summary.model_version_id,
            top_drivers=[
                {
                    "feature_name": driver.feature_name,
                    "attribution_score": driver.attribution_score,
                    "absolute_score": driver.absolute_score,
                    "rank": driver.rank,
                    "percentile": driver.percentile,
                    "feature_type": driver.feature_type,
                    "description": driver.description,
                    "importance_category": driver.importance_category
                }
                for driver in summary.top_drivers
            ],
            summary_text=summary.summary_text,
            confidence_score=summary.confidence_score,
            key_insights=[],  # Would be populated
            risk_factors=[],  # Would be populated
            recommendations=[]  # Would be populated
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_explanation_summary",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get explanation summary: {str(exc)}"
        )


@router.get("/explanations/{explanation_id}/visualizations", response_model=VisualizationListResponse)
async def list_explanation_visualizations(
    request: Request,
    response: Response,
    explanation_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    visualization_type: Optional[str] = Query(None, description="Filter by visualization type")
) -> VisualizationListResponse:
    """List explanation visualizations."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get visualizations (mock implementation)
        visualizations = await service.get_visualizations(
            explanation_id=explanation_id,
            visualization_type=visualization_type,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        visualization_responses = [
            VisualizationResponse(
                visualization_id=vis.visualization_id,
                explanation_id=vis.explanation_id,
                visualization_type=vis.visualization_type,
                file_path=vis.file_path,
                file_size=vis.file_size,
                mime_type=vis.mime_type,
                created_at=vis.created_at,
                metadata=vis.metadata
            )
            for vis in visualizations
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_explanation_visualizations",
            query_time_ms=query_time_ms,
            record_count=len(visualization_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return VisualizationListResponse(
            data=visualization_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_explanation_visualizations",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list explanation visualizations: {str(exc)}"
        )


@router.get("/visualizations/{visualization_id}/download")
async def download_explanation_visualization(
    request: Request,
    visualization_id: str
) -> StreamingResponse:
    """Download explanation visualization file."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get visualization (mock implementation)
        visualization = await service.get_visualization(visualization_id)

        if not visualization:
            raise HTTPException(status_code=404, detail="Visualization not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="download_explanation_visualization",
            query_time_ms=query_time_ms
        )

        # Return file as streaming response
        def file_generator():
            # Mock file content - in reality would read from storage
            yield b"mock_visualization_content"

        return StreamingResponse(
            file_generator(),
            media_type=visualization.mime_type,
            headers={
                "Content-Disposition": f"attachment; filename={visualization.file_path.split('/')[-1]}"
            }
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="download_explanation_visualization",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to download explanation visualization: {str(exc)}"
        )


@router.get("/models/{model_name}/feature-importance", response_model=Dict[str, any])
async def get_model_feature_importance(
    request: Request,
    model_name: str,
    model_version: Optional[str] = Query(None, description="Specific model version")
) -> Dict[str, any]:
    """Get feature importance for a model."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()

        # Get feature importance (mock implementation)
        feature_importance = await service.get_model_feature_importance(
            model_name=model_name,
            model_version=model_version
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_model_feature_importance",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": feature_importance
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_model_feature_importance",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get model feature importance: {str(exc)}"
        )


@router.get("/health")
async def get_explainability_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get explainability service health status."""
    start_time = time.perf_counter()

    try:
        service = get_explainability_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_explainability_health",
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
            operation="get_explainability_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get explainability health: {str(exc)}"
        )
