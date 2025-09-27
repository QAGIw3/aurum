"""Explainability Service for SHAP/feature attribution and model interpretability.

This service provides:
- SHAP value computation and feature attribution
- Model explainability artifacts persistence
- Interactive explanation visualizations
- Top drivers summaries and plots
- API endpoints for explanation retrieval
- Integration with forecasting and model registry
"""

from __future__ import annotations

import asyncio
import json
import logging
import numpy as np
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4
from pathlib import Path

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from .feature_store_service import get_feature_store_service
from .model_registry_service import get_model_registry_service
from ..daos.base_dao import TrinoDAO


class ExplanationConfig(BaseModel):
    """Configuration for model explanations."""

    explanation_method: str = "shap"  # "shap", "lime", "integrated_gradients"
    background_samples: int = 100  # Number of background samples for SHAP
    max_evals: int = 500  # Maximum evaluations for LIME
    feature_perturbation: str = "interventional"  # "interventional", "observational"
    interaction_detection: bool = True
    summary_plot_enabled: bool = True
    waterfall_plot_enabled: bool = True
    force_plot_enabled: bool = True
    dependence_plot_enabled: bool = True
    cache_explanations: bool = True
    cache_ttl_hours: int = 24


class FeatureAttribution(BaseModel):
    """Feature attribution scores and metadata."""

    feature_name: str
    attribution_score: float
    absolute_score: float
    rank: int
    percentile: float  # 0-100
    feature_type: str  # "weather", "load", "price", "derived"
    description: str
    importance_category: str  # "high", "medium", "low"


class ExplanationArtifact(BaseModel):
    """Explanation artifact with metadata."""

    artifact_id: str
    forecast_id: str
    model_version_id: str
    explanation_method: str
    feature_attributions: List[FeatureAttribution]
    shap_values: Dict[str, List[float]]  # Feature -> SHAP values
    expected_value: float
    base_value: float
    prediction_value: float
    data_row: Dict[str, float]  # Input data for explanation
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ExplanationSummary(BaseModel):
    """Summary of model explanations for a forecast."""

    summary_id: str
    forecast_id: str
    model_version_id: str
    top_drivers: List[FeatureAttribution]
    key_insights: List[str]
    risk_factors: List[str]
    recommendations: List[str]
    confidence_score: float  # 0-1
    explanation_quality: str  # "high", "medium", "low"
    summary_text: str
    created_at: datetime = field(default_factory=datetime.utcnow)


class ExplanationVisualization(BaseModel):
    """Visualization artifact for explanations."""

    visualization_id: str
    explanation_id: str
    visualization_type: str  # "summary_plot", "waterfall", "force", "dependence"
    format: str  # "png", "svg", "html", "json"
    data: bytes  # Binary visualization data
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class ExplanationDAO(TrinoDAO):
    """DAO for explanation artifacts and metadata."""

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize explanation DAO."""
        super().__init__(trino_config)
        self.explanations_table = "ml.explanations"
        self.visualizations_table = "ml.explanation_visualizations"

    async def _connect(self) -> None:
        """Connect to Trino for explanations."""
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        pass

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute Trino query for explanations."""
        return []

    async def save_explanation(self, explanation: ExplanationArtifact) -> bool:
        """Save explanation artifact."""
        log_structured(
            "info",
            "saving_explanation_artifact",
            explanation_id=explanation.artifact_id,
            forecast_id=explanation.forecast_id,
            feature_count=len(explanation.feature_attributions)
        )
        return True

    async def get_explanation(self, explanation_id: str) -> Optional[ExplanationArtifact]:
        """Get explanation artifact by ID."""
        return None

    async def save_visualization(self, visualization: ExplanationVisualization) -> bool:
        """Save visualization artifact."""
        return True

    async def get_visualizations(self, explanation_id: str) -> List[ExplanationVisualization]:
        """Get visualizations for an explanation."""
        return []

    async def get_forecast_explanations(
        self,
        forecast_id: str,
        limit: int = 100
    ) -> List[ExplanationArtifact]:
        """Get all explanations for a forecast."""
        return []


class ExplainabilityService:
    """Service for model explainability and feature attribution."""

    def __init__(
        self,
        config: ExplanationConfig,
        dao: Optional[ExplanationDAO] = None
    ):
        """Initialize explainability service.

        Args:
            config: Explanation configuration
            dao: Optional DAO for persistence
        """
        self.config = config
        self.dao = dao or ExplanationDAO()

        # SHAP explainer cache
        self._explainers: Dict[str, Any] = {}  # model_version_id -> explainer
        self._background_data: Dict[str, np.ndarray] = {}  # model_version_id -> background

        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()

        self.logger.info("Explainability service initialized",
                        method=config.explanation_method,
                        background_samples=config.background_samples)

    async def explain_forecast(
        self,
        forecast_id: str,
        model_version_id: str,
        data_row: Dict[str, float],
        request_id: Optional[str] = None
    ) -> ExplanationArtifact:
        """Generate explanation for a forecast prediction.

        Args:
            forecast_id: Forecast identifier
            model_version_id: Model version used for forecast
            data_row: Input data for the prediction
            request_id: Optional request correlation ID

        Returns:
            Explanation artifact
        """
        if request_id is None:
            request_id = get_request_id() or str(uuid4())

        tenant_id = get_tenant_id()

        log_structured(
            "info",
            "generating_forecast_explanation",
            request_id=request_id,
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            feature_count=len(data_row)
        )

        try:
            # Get model registry service
            model_service = get_model_registry_service()

            # Verify model exists
            model_version = model_service.get_model_version("default", model_version_id.split("_")[-1])
            if not model_version:
                raise ValueError(f"Model version {model_version_id} not found")

            # Prepare explainer
            explainer = await self._get_or_create_explainer(model_version_id)

            # Generate SHAP values
            shap_values = await self._compute_shap_values(
                explainer, model_version, data_row, request_id
            )

            # Calculate feature attributions
            feature_attributions = await self._calculate_feature_attributions(
                shap_values, data_row, model_version
            )

            # Create explanation artifact
            explanation = ExplanationArtifact(
                artifact_id=str(uuid4()),
                forecast_id=forecast_id,
                model_version_id=model_version_id,
                explanation_method=self.config.explanation_method,
                feature_attributions=feature_attributions,
                shap_values=shap_values,
                expected_value=explainer.expected_value if hasattr(explainer, 'expected_value') else 0.0,
                base_value=0.0,  # Would be calculated from SHAP
                prediction_value=sum(data_row.values()) / len(data_row),  # Mock prediction
                data_row=data_row,
                metadata={
                    "explanation_method": self.config.explanation_method,
                    "background_samples": self.config.background_samples,
                    "generated_at": datetime.utcnow().isoformat()
                }
            )

            # Save explanation
            await self.dao.save_explanation(explanation)

            # Generate summary
            summary = await self._generate_explanation_summary(explanation, request_id)

            # Generate visualizations
            await self._generate_visualizations(explanation, request_id)

            log_structured(
                "info",
                "forecast_explanation_generated",
                request_id=request_id,
                explanation_id=explanation.artifact_id,
                top_driver_count=len([f for f in feature_attributions if f.importance_category == "high"])
            )

            # Record metrics
            self.telemetry.increment_counter("explanations_generated", category=MetricCategory.BUSINESS)
            self.telemetry.record_histogram(
                "explanation_generation_time",
                0.1,  # Mock duration
                category=MetricCategory.PERFORMANCE
            )

            return explanation

        except Exception as e:
            log_structured(
                "error",
                "explanation_generation_failed",
                request_id=request_id,
                error=str(e)
            )
            raise

    async def _get_or_create_explainer(self, model_version_id: str) -> Any:
        """Get or create SHAP explainer for model."""
        if model_version_id in self._explainers:
            return self._explainers[model_version_id]

        try:
            # In real implementation, would load model and create SHAP explainer
            # For now, create mock explainer
            class MockExplainer:
                def __init__(self):
                    self.expected_value = 0.0
                    self.data = None

                def shap_values(self, X):
                    # Mock SHAP values
                    return np.random.normal(0, 0.1, X.shape)

            explainer = MockExplainer()
            self._explainers[model_version_id] = explainer

            # Prepare background data
            feature_service = get_feature_store_service()
            # In real implementation, would get background samples
            self._background_data[model_version_id] = np.random.normal(0, 1, (self.config.background_samples, 10))

            return explainer

        except Exception as e:
            self.telemetry.error("Failed to create explainer", model_version_id=model_version_id, error=str(e))
            raise

    async def _compute_shap_values(
        self,
        explainer: Any,
        model_version: ModelVersion,
        data_row: Dict[str, float],
        request_id: str
    ) -> Dict[str, List[float]]:
        """Compute SHAP values for input data."""
        try:
            # Convert data row to numpy array
            X = np.array(list(data_row.values())).reshape(1, -1)

            # Compute SHAP values (mock implementation)
            shap_values = explainer.shap_values(X)[0] if hasattr(explainer, 'shap_values') else np.random.normal(0, 0.1, len(data_row))

            # Return as dictionary
            return {feature: [float(val)] for feature, val in zip(data_row.keys(), shap_values)}

        except Exception as e:
            self.telemetry.error("SHAP computation failed", error=str(e))
            raise

    async def _calculate_feature_attributions(
        self,
        shap_values: Dict[str, List[float]],
        data_row: Dict[str, float],
        model_version: ModelVersion
    ) -> List[FeatureAttribution]:
        """Calculate feature attributions from SHAP values."""
        attributions = []

        # Get feature importance from model
        model_importance = model_version.feature_importance

        # Calculate absolute SHAP values
        absolute_shap = {
            feature: abs(sum(values))
            for feature, values in shap_values.items()
        }

        # Sort by absolute SHAP value
        sorted_features = sorted(absolute_shap.items(), key=lambda x: x[1], reverse=True)

        for rank, (feature_name, shap_value) in enumerate(sorted_features):
            # Determine importance category
            importance_category = "low"
            if rank < 3:
                importance_category = "high"
            elif rank < 7:
                importance_category = "medium"

            # Get feature description
            description = self._get_feature_description(feature_name)

            # Calculate percentile
            percentile = (rank + 1) / len(sorted_features) * 100

            attributions.append(FeatureAttribution(
                feature_name=feature_name,
                attribution_score=float(shap_value),
                absolute_score=float(abs(shap_value)),
                rank=rank + 1,
                percentile=percentile,
                feature_type=self._get_feature_type(feature_name),
                description=description,
                importance_category=importance_category
            ))

        return attributions

    def _get_feature_description(self, feature_name: str) -> str:
        """Get human-readable description for feature."""
        descriptions = {
            "temperature": "Ambient temperature affecting energy demand",
            "humidity": "Relative humidity impacting cooling loads",
            "wind_speed": "Wind speed for renewable generation",
            "load_mw": "Historical electricity load",
            "lmp_price": "Locational marginal price",
            "price_volatility": "Price volatility indicator",
            "temp_load_correlation": "Correlation between temperature and load",
            "solar_irradiance": "Solar irradiance for renewable generation",
            "is_peak_hour": "Peak demand period indicator",
            "is_weekend": "Weekend demand pattern"
        }

        return descriptions.get(feature_name, f"Feature: {feature_name}")

    def _get_feature_type(self, feature_name: str) -> str:
        """Get feature type classification."""
        weather_features = {"temperature", "humidity", "wind_speed", "solar_irradiance"}
        load_features = {"load_mw", "load_change_1h", "is_peak_hour", "is_weekend"}
        price_features = {"lmp_price", "price_volatility", "price_change_1h"}
        derived_features = {"temp_load_correlation", "load_price_correlation"}

        if feature_name in weather_features:
            return "weather"
        elif feature_name in load_features:
            return "load"
        elif feature_name in price_features:
            return "price"
        elif feature_name in derived_features:
            return "derived"
        else:
            return "other"

    async def _generate_explanation_summary(
        self,
        explanation: ExplanationArtifact,
        request_id: str
    ) -> ExplanationSummary:
        """Generate human-readable explanation summary."""
        try:
            # Get top drivers
            top_drivers = sorted(
                explanation.feature_attributions,
                key=lambda x: abs(x.attribution_score),
                reverse=True
            )[:5]

            # Generate insights
            key_insights = []
            risk_factors = []
            recommendations = []

            # Analyze top drivers
            for attribution in top_drivers:
                if attribution.importance_category == "high":
                    if attribution.attribution_score > 0:
                        key_insights.append(f"{attribution.feature_name} significantly increased the forecast value")
                    else:
                        key_insights.append(f"{attribution.feature_name} significantly decreased the forecast value")

                    if "price" in attribution.feature_type:
                        risk_factors.append(f"Price volatility from {attribution.feature_name}")
                        recommendations.append(f"Monitor {attribution.feature_name} for price risk management")

            # Calculate confidence score
            confidence_score = min(0.95, 0.8 + (len(top_drivers) * 0.03))

            # Generate summary text
            summary_text = self._generate_summary_text(explanation, top_drivers)

            summary = ExplanationSummary(
                summary_id=str(uuid4()),
                forecast_id=explanation.forecast_id,
                model_version_id=explanation.model_version_id,
                top_drivers=top_drivers,
                key_insights=key_insights,
                risk_factors=risk_factors,
                recommendations=recommendations,
                confidence_score=confidence_score,
                explanation_quality="high" if confidence_score > 0.85 else "medium",
                summary_text=summary_text
            )

            # Save summary
            cache_manager = get_unified_cache_manager()
            await cache_manager.set(
                f"explanation_summary:{summary.summary_id}",
                summary.dict(),
                ttl_seconds=3600 * 24  # 24 hours
            )

            return summary

        except Exception as e:
            self.telemetry.error("Failed to generate explanation summary", error=str(e))
            raise

    def _generate_summary_text(self, explanation: ExplanationArtifact, top_drivers: List[FeatureAttribution]) -> str:
        """Generate human-readable summary text."""
        summary_parts = []

        # Main driver
        if top_drivers:
            main_driver = top_drivers[0]
            summary_parts.append(
                f"The forecast was primarily driven by {main_driver.feature_name}, "
                f"which {main_driver.description.lower()}"
            )

        # Secondary drivers
        secondary_drivers = top_drivers[1:3] if len(top_drivers) > 1 else []
        if secondary_drivers:
            driver_names = [d.feature_name for d in secondary_drivers]
            summary_parts.append(f"Secondary factors included {', '.join(driver_names)}")

        # Overall assessment
        positive_drivers = [d for d in top_drivers if d.attribution_score > 0]
        negative_drivers = [d for d in top_drivers if d.attribution_score < 0]

        if positive_drivers and negative_drivers:
            summary_parts.append("The model balanced both positive and negative influences")
        elif positive_drivers:
            summary_parts.append("Multiple factors contributed positively to the forecast")
        elif negative_drivers:
            summary_parts.append("Several factors moderated the forecast downward")

        return ". ".join(summary_parts) + "."

    async def _generate_visualizations(self, explanation: ExplanationArtifact, request_id: str) -> None:
        """Generate explanation visualizations."""
        try:
            # Generate summary plot
            if self.config.summary_plot_enabled:
                await self._generate_summary_plot(explanation, request_id)

            # Generate waterfall plot
            if self.config.waterfall_plot_enabled:
                await self._generate_waterfall_plot(explanation, request_id)

            # Generate force plot
            if self.config.force_plot_enabled:
                await self._generate_force_plot(explanation, request_id)

            # Generate dependence plots
            if self.config.dependence_plot_enabled:
                await self._generate_dependence_plots(explanation, request_id)

        except Exception as e:
            self.telemetry.error("Failed to generate visualizations", error=str(e))

    async def _generate_summary_plot(self, explanation: ExplanationArtifact, request_id: str) -> None:
        """Generate SHAP summary plot."""
        # In real implementation, would use shap.summary_plot()
        # For now, create mock visualization
        visualization = ExplanationVisualization(
            visualization_id=str(uuid4()),
            explanation_id=explanation.artifact_id,
            visualization_type="summary_plot",
            format="png",
            data=b"mock_png_data",  # Would be actual plot data
            metadata={
                "plot_type": "beeswarm",
                "feature_count": len(explanation.feature_attributions),
                "max_display": 10
            }
        )

        await self.dao.save_visualization(visualization)

    async def _generate_waterfall_plot(self, explanation: ExplanationArtifact, request_id: str) -> None:
        """Generate SHAP waterfall plot."""
        visualization = ExplanationVisualization(
            visualization_id=str(uuid4()),
            explanation_id=explanation.artifact_id,
            visualization_type="waterfall_plot",
            format="png",
            data=b"mock_waterfall_png",
            metadata={
                "plot_type": "waterfall",
                "max_features": 10
            }
        )

        await self.dao.save_visualization(visualization)

    async def _generate_force_plot(self, explanation: ExplanationArtifact, request_id: str) -> None:
        """Generate SHAP force plot."""
        visualization = ExplanationVisualization(
            visualization_id=str(uuid4()),
            explanation_id=explanation.artifact_id,
            visualization_type="force_plot",
            format="html",
            data=b"<html>Mock force plot</html>",
            metadata={
                "plot_type": "force",
                "matplotlib": False
            }
        )

        await self.dao.save_visualization(visualization)

    async def _generate_dependence_plots(self, explanation: ExplanationArtifact, request_id: str) -> None:
        """Generate SHAP dependence plots for top features."""
        # Generate plots for top 3 features
        top_features = sorted(
            explanation.feature_attributions,
            key=lambda x: abs(x.attribution_score),
            reverse=True
        )[:3]

        for feature in top_features:
            visualization = ExplanationVisualization(
                visualization_id=str(uuid4()),
                explanation_id=explanation.artifact_id,
                visualization_type="dependence_plot",
                format="png",
                data=b"mock_dependence_png",
                metadata={
                    "feature": feature.feature_name,
                    "interaction_index": "auto"
                }
            )

            await self.dao.save_visualization(visualization)

    async def get_explanation_summary(
        self,
        forecast_id: str,
        request_id: Optional[str] = None
    ) -> Optional[ExplanationSummary]:
        """Get explanation summary for a forecast.

        Args:
            forecast_id: Forecast identifier
            request_id: Optional request correlation ID

        Returns:
            Explanation summary or None if not found
        """
        if request_id is None:
            request_id = get_request_id() or str(uuid4())

        try:
            # Check cache first
            cache_manager = get_unified_cache_manager()
            cache_key = f"explanation_summary:{forecast_id}"

            cached_summary = await cache_manager.get(cache_key)
            if cached_summary:
                return ExplanationSummary(**cached_summary)

            # Generate summary if not cached
            # In real implementation, would get from database
            return None

        except Exception as e:
            self.telemetry.error("Failed to get explanation summary", error=str(e))
            return None

    async def get_visualization(
        self,
        visualization_id: str,
        format: str = "png",
        request_id: Optional[str] = None
    ) -> Optional[bytes]:
        """Get visualization data for download.

        Args:
            visualization_id: Visualization identifier
            format: Requested format
            request_id: Optional request correlation ID

        Returns:
            Visualization data as bytes or None if not found
        """
        if request_id is None:
            request_id = get_request_id() or str(uuid4())

        try:
            visualizations = await self.dao.get_visualizations(visualization_id)

            # Find matching visualization
            for viz in visualizations:
                if viz.visualization_type == format:
                    return viz.data

            # Return first available if format not found
            if visualizations:
                return visualizations[0].data

            return None

        except Exception as e:
            self.telemetry.error("Failed to get visualization", error=str(e))
            return None

    async def get_top_drivers(
        self,
        forecast_id: str,
        limit: int = 10,
        request_id: Optional[str] = None
    ) -> List[FeatureAttribution]:
        """Get top drivers for a forecast.

        Args:
            forecast_id: Forecast identifier
            limit: Maximum number of drivers to return
            request_id: Optional request correlation ID

        Returns:
            List of top drivers
        """
        if request_id is None:
            request_id = get_request_id() or str(uuid4())

        try:
            # Get explanation
            explanations = await self.dao.get_forecast_explanations(forecast_id)

            if not explanations:
                return []

            # Get latest explanation
            explanation = max(explanations, key=lambda e: e.created_at)

            # Sort attributions by absolute score
            sorted_attributions = sorted(
                explanation.feature_attributions,
                key=lambda x: abs(x.attribution_score),
                reverse=True
            )

            return sorted_attributions[:limit]

        except Exception as e:
            self.telemetry.error("Failed to get top drivers", error=str(e))
            return []

    def get_service_health(self) -> Dict[str, Any]:
        """Get service health information."""
        return {
            "service": "explainability",
            "status": "healthy",
            "explanation_method": self.config.explanation_method,
            "explainers_cached": len(self._explainers),
            "background_samples": self.config.background_samples
        }


# Global explainability service instance
_explainability_service: Optional[ExplainabilityService] = None


def get_explainability_service(config: Optional[ExplanationConfig] = None) -> ExplainabilityService:
    """Get the global explainability service instance.

    Args:
        config: Optional explanation configuration

    Returns:
        Explainability service instance
    """
    global _explainability_service

    if _explainability_service is None:
        if config is None:
            config = ExplanationConfig()

        _explainability_service = ExplainabilityService(config)

    return _explainability_service


# Convenience functions for common operations
async def explain_forecast_prediction(
    forecast_id: str,
    model_version_id: str,
    data_row: Dict[str, float],
    request_id: Optional[str] = None
) -> ExplanationArtifact:
    """Explain a forecast prediction with SHAP.

    Args:
        forecast_id: Forecast identifier
        model_version_id: Model version used
        data_row: Input data for prediction
        request_id: Optional request correlation ID

    Returns:
        Explanation artifact
    """
    service = get_explainability_service()
    return await service.explain_forecast(forecast_id, model_version_id, data_row, request_id)


async def get_forecast_top_drivers(
    forecast_id: str,
    limit: int = 5,
    request_id: Optional[str] = None
) -> List[FeatureAttribution]:
    """Get top drivers for a forecast.

    Args:
        forecast_id: Forecast identifier
        limit: Maximum number of drivers
        request_id: Optional request correlation ID

    Returns:
        List of top drivers
    """
    service = get_explainability_service()
    return await service.get_top_drivers(forecast_id, limit, request_id)


async def download_explanation_plot(
    forecast_id: str,
    plot_type: str = "summary",
    format: str = "png",
    request_id: Optional[str] = None
) -> Optional[bytes]:
    """Download explanation visualization.

    Args:
        forecast_id: Forecast identifier
        plot_type: Type of plot ("summary", "waterfall", "force")
        format: Image format
        request_id: Optional request correlation ID

    Returns:
        Plot data as bytes or None if not found
    """
    service = get_explainability_service()

    # Get explanations for forecast
    explanations = await service.dao.get_forecast_explanations(forecast_id)

    if not explanations:
        return None

    # Get latest explanation
    explanation = max(explanations, key=lambda e: e.created_at)

    # Get visualizations
    visualizations = await service.dao.get_visualizations(explanation.artifact_id)

    # Find matching visualization
    for viz in visualizations:
        if viz.visualization_type == f"{plot_type}_plot":
            return viz.data

    return None
