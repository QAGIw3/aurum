"""Compatibility shim for explainability service.

Provides backward compatibility for code using the old explainability_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from dataclasses import dataclass, field

from aurum.services.ml.explainability import ExplainabilityService


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


@dataclass
class ExplanationArtifact:
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


@dataclass
class ExplanationSummary:
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
    visualization_type: str  # "summary_plot", "waterfall", "force_plot", "dependence"
    visualization_data: Dict[str, Any]  # Plot data in format suitable for front-end
    visualization_url: Optional[str] = None  # Optional URL to stored visualization
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# Singleton instance
_service_instance = None


def get_explainability_service(config: Optional[ExplanationConfig] = None) -> ExplainabilityService:
    """Get singleton explainability service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = ExplainabilityService()
    return _service_instance


async def generate_forecast_explanation(
    forecast_id: str,
    model_version_id: str,
    input_data: Dict[str, float],
    config: Optional[ExplanationConfig] = None
) -> ExplanationArtifact:
    """Generate explanation for a forecast."""
    service = get_explainability_service(config)
    
    # Call the new service
    result = await service.explain_prediction(
        model_id=model_version_id,
        prediction_id=forecast_id,
        input_data=input_data,
        method=config.explanation_method if config else "shap"
    )
    
    if result.success and result.data:
        # Convert to legacy format
        attributions = []
        for i, (feature, value) in enumerate(result.data.get("attributions", [])):
            attributions.append(FeatureAttribution(
                feature_name=feature.get("feature", ""),
                attribution_score=feature.get("attribution", 0),
                absolute_score=abs(feature.get("attribution", 0)),
                rank=i + 1,
                percentile=100 - (i / len(result.data.get("attributions", []))) * 100,
                feature_type="derived",
                description=f"Feature {feature.get('feature', '')}",
                importance_category="high" if i < 3 else "medium" if i < 6 else "low"
            ))
        
        return ExplanationArtifact(
            artifact_id=f"exp_{forecast_id}",
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            explanation_method=result.data.get("method", "shap"),
            feature_attributions=attributions,
            shap_values=result.data.get("shap_values", {}),
            expected_value=0,
            base_value=result.data.get("base_value", 0),
            prediction_value=result.data.get("prediction_value", 0),
            data_row=input_data
        )
    else:
        # Return empty artifact on error
        return ExplanationArtifact(
            artifact_id=f"exp_{forecast_id}",
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            explanation_method="shap",
            feature_attributions=[],
            shap_values={},
            expected_value=0,
            base_value=0,
            prediction_value=0,
            data_row=input_data
        )


async def create_explanation_summary(
    forecast_id: str,
    model_version_id: str,
    top_k: int = 5
) -> ExplanationSummary:
    """Create a summary of model explanations."""
    service = get_explainability_service()
    
    result = await service.generate_summary(
        model_id=model_version_id,
        forecast_id=forecast_id,
        top_drivers=top_k
    )
    
    if result.success and result.data:
        data = result.data
        
        # Convert top drivers to FeatureAttribution objects
        top_drivers = []
        for driver in data.get("top_drivers", []):
            top_drivers.append(FeatureAttribution(
                feature_name=driver.get("feature", ""),
                attribution_score=driver.get("importance", 0),
                absolute_score=abs(driver.get("importance", 0)),
                rank=driver.get("rank", 0),
                percentile=100 - (driver.get("rank", 0) / top_k) * 100,
                feature_type=driver.get("category", "other"),
                description=f"Feature {driver.get('feature', '')}",
                importance_category="high" if driver.get("rank", 0) <= 2 else "medium"
            ))
        
        return ExplanationSummary(
            summary_id=f"summary_{forecast_id}",
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            top_drivers=top_drivers,
            key_insights=data.get("key_insights", []),
            risk_factors=[],  # Not in new service
            recommendations=[],  # Not in new service
            confidence_score=data.get("confidence_score", 0.85),
            explanation_quality="high" if data.get("confidence_score", 0.85) > 0.8 else "medium",
            summary_text=data.get("summary_text", "")
        )
    else:
        return ExplanationSummary(
            summary_id=f"summary_{forecast_id}",
            forecast_id=forecast_id,
            model_version_id=model_version_id,
            top_drivers=[],
            key_insights=[],
            risk_factors=[],
            recommendations=[],
            confidence_score=0,
            explanation_quality="low",
            summary_text=""
        )
