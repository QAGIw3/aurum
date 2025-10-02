"""Explainability service for model interpretability and feature attribution.

Implements business logic for SHAP values, feature importance, and model explanations.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class ExplainabilityEngine(Protocol):
    """Protocol for explainability implementations."""
    
    async def compute_shap_values(
        self,
        model_id: str,
        data: List[Dict[str, float]]
    ) -> Dict[str, List[float]]:
        """Compute SHAP values for predictions."""
        ...
    
    async def compute_feature_importance(
        self,
        model_id: str,
        method: str = "permutation"
    ) -> Dict[str, float]:
        """Compute feature importance scores."""
        ...


class ExplainabilityService(BaseService):
    """Service for model explainability operations.
    
    Explainability provides:
    - SHAP value computation
    - Feature attribution analysis
    - Model interpretability insights
    - Interactive visualizations
    - Explanation persistence
    - Integration with ML pipeline
    
    This service:
    - Computes feature attributions
    - Generates model explanations
    - Creates visualization artifacts
    - Provides interpretability insights
    - Manages explanation storage
    """
    
    def __init__(self, engine: Optional[ExplainabilityEngine] = None):
        """Initialize service with explainability engine.
        
        Args:
            engine: Explainability computation engine
        """
        super().__init__()
        self._engine = engine or DefaultExplainabilityEngine()
        self._explanation_cache: Dict[str, Dict[str, Any]] = {}
    
    async def explain_prediction(
        self,
        model_id: str,
        prediction_id: str,
        input_data: Dict[str, float],
        method: str = "shap",
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Generate explanation for a single prediction.
        
        Args:
            model_id: Model identifier
            prediction_id: Prediction to explain
            input_data: Input features used
            method: Explanation method (shap, lime, etc.)
            context: Service context
            
        Returns:
            ServiceResult with explanation data
        """
        self._track_operation("explain_prediction", {
            "model_id": model_id,
            "method": method
        })
        
        try:
            # Validate inputs
            if not input_data:
                return ServiceResult.error("Input data required for explanation")
            
            # Check cache
            cache_key = f"{model_id}:{prediction_id}:{method}"
            if cache_key in self._explanation_cache:
                return ServiceResult.ok(self._explanation_cache[cache_key])
            
            # Compute explanation based on method
            if method == "shap":
                explanation = await self._explain_with_shap(
                    model_id,
                    prediction_id,
                    input_data
                )
            elif method == "lime":
                explanation = await self._explain_with_lime(
                    model_id,
                    prediction_id,
                    input_data
                )
            else:
                return ServiceResult.error(f"Unsupported method: {method}")
            
            # Cache result
            self._explanation_cache[cache_key] = explanation
            
            return ServiceResult.ok(explanation)
            
        except Exception as e:
            logger.error(f"Explanation failed: {e}")
            return ServiceResult.error(f"Explanation generation failed: {str(e)}")
    
    async def batch_explain(
        self,
        model_id: str,
        predictions: List[Dict[str, Any]],
        method: str = "shap",
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Generate explanations for multiple predictions.
        
        Args:
            model_id: Model identifier
            predictions: List of predictions with input data
            method: Explanation method
            context: Service context
            
        Returns:
            ServiceResult with batch explanations
        """
        self._track_operation("batch_explain", {
            "model_id": model_id,
            "batch_size": len(predictions)
        })
        
        try:
            explanations = []
            
            # Process in batches for efficiency
            batch_size = 100
            for i in range(0, len(predictions), batch_size):
                batch = predictions[i:i + batch_size]
                
                # Extract input data
                input_data = [p["input_data"] for p in batch]
                
                # Compute SHAP values for batch
                shap_values = await self._engine.compute_shap_values(
                    model_id,
                    input_data
                )
                
                # Build explanations
                for j, pred in enumerate(batch):
                    explanation = {
                        "prediction_id": pred["prediction_id"],
                        "shap_values": {
                            feat: shap_values[feat][j]
                            for feat in shap_values
                        },
                        "prediction_value": pred["value"],
                        "input_data": pred["input_data"]
                    }
                    explanations.append(explanation)
            
            return ServiceResult.ok(explanations)
            
        except Exception as e:
            logger.error(f"Batch explanation failed: {e}")
            return ServiceResult.error(f"Batch processing failed: {str(e)}")
    
    async def get_feature_importance(
        self,
        model_id: str,
        method: str = "mean_shap",
        top_k: Optional[int] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get global feature importance for model.
        
        Args:
            model_id: Model identifier
            method: Importance calculation method
            top_k: Return only top K features
            context: Service context
            
        Returns:
            ServiceResult with feature importance data
        """
        self._track_operation("feature_importance", {
            "model_id": model_id,
            "method": method
        })
        
        try:
            # Get importance scores
            if method == "mean_shap":
                scores = await self._compute_mean_shap_importance(model_id)
            elif method == "permutation":
                scores = await self._engine.compute_feature_importance(
                    model_id,
                    method="permutation"
                )
            else:
                return ServiceResult.error(f"Unknown method: {method}")
            
            # Sort by importance
            sorted_features = sorted(
                scores.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )
            
            # Build result
            importance_list = []
            for i, (feature, score) in enumerate(sorted_features):
                if top_k and i >= top_k:
                    break
                
                importance_list.append({
                    "feature": feature,
                    "importance": score,
                    "rank": i + 1,
                    "category": self._categorize_feature(feature)
                })
            
            return ServiceResult.ok(importance_list)
            
        except Exception as e:
            logger.error(f"Feature importance failed: {e}")
            return ServiceResult.error(f"Importance calculation failed: {str(e)}")
    
    async def generate_summary(
        self,
        model_id: str,
        forecast_id: str,
        top_drivers: int = 5,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Generate explanation summary for forecast.
        
        Args:
            model_id: Model identifier
            forecast_id: Forecast to summarize
            top_drivers: Number of top drivers to include
            context: Service context
            
        Returns:
            ServiceResult with summary data
        """
        self._track_operation("generate_summary", {
            "model_id": model_id,
            "forecast_id": forecast_id
        })
        
        try:
            # Get feature importance
            importance_result = await self.get_feature_importance(
                model_id,
                top_k=top_drivers,
                context=context
            )
            
            if not importance_result.success:
                return importance_result
            
            top_features = importance_result.data
            
            # Generate insights
            insights = self._generate_insights(top_features)
            
            # Build summary
            summary = {
                "forecast_id": forecast_id,
                "model_id": model_id,
                "top_drivers": top_features,
                "key_insights": insights,
                "summary_text": self._generate_summary_text(top_features, insights),
                "confidence_score": 0.85,  # Placeholder
                "generated_at": datetime.utcnow().isoformat()
            }
            
            return ServiceResult.ok(summary)
            
        except Exception as e:
            logger.error(f"Summary generation failed: {e}")
            return ServiceResult.error(f"Summary failed: {str(e)}")
    
    async def create_visualization(
        self,
        explanation_id: str,
        viz_type: str,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create visualization for explanation.
        
        Args:
            explanation_id: Explanation to visualize
            viz_type: Type of visualization
            options: Visualization options
            context: Service context
            
        Returns:
            ServiceResult with visualization data
        """
        self._track_operation("create_visualization", {
            "explanation_id": explanation_id,
            "viz_type": viz_type
        })
        
        try:
            # Validate visualization type
            valid_types = ["summary_plot", "waterfall", "force_plot", "dependence"]
            if viz_type not in valid_types:
                return ServiceResult.error(f"Invalid visualization type: {viz_type}")
            
            # Generate visualization data
            viz_data = {
                "visualization_id": f"viz_{explanation_id}_{viz_type}",
                "explanation_id": explanation_id,
                "type": viz_type,
                "data": {},  # Would contain actual plot data
                "options": options or {},
                "created_at": datetime.utcnow().isoformat()
            }
            
            return ServiceResult.ok(viz_data)
            
        except Exception as e:
            logger.error(f"Visualization creation failed: {e}")
            return ServiceResult.error(f"Visualization failed: {str(e)}")
    
    # Private helper methods
    
    async def _explain_with_shap(
        self,
        model_id: str,
        prediction_id: str,
        input_data: Dict[str, float]
    ) -> Dict[str, Any]:
        """Generate SHAP-based explanation."""
        # Compute SHAP values
        shap_values = await self._engine.compute_shap_values(
            model_id,
            [input_data]
        )
        
        # Build feature attributions
        attributions = []
        for feature, values in shap_values.items():
            attributions.append({
                "feature": feature,
                "attribution": values[0],
                "value": input_data.get(feature, 0),
                "impact": "positive" if values[0] > 0 else "negative"
            })
        
        # Sort by absolute attribution
        attributions.sort(key=lambda x: abs(x["attribution"]), reverse=True)
        
        return {
            "method": "shap",
            "prediction_id": prediction_id,
            "attributions": attributions,
            "base_value": 0,  # Placeholder
            "prediction_value": sum(a["attribution"] for a in attributions)
        }
    
    async def _explain_with_lime(
        self,
        model_id: str,
        prediction_id: str,
        input_data: Dict[str, float]
    ) -> Dict[str, Any]:
        """Generate LIME-based explanation."""
        # Placeholder implementation
        return {
            "method": "lime",
            "prediction_id": prediction_id,
            "attributions": [],
            "note": "LIME not yet implemented"
        }
    
    async def _compute_mean_shap_importance(
        self,
        model_id: str
    ) -> Dict[str, float]:
        """Compute mean absolute SHAP values."""
        # In production, would aggregate over many predictions
        return {
            "temperature": 0.25,
            "humidity": 0.15,
            "load_lag_1h": 0.35,
            "hour_of_day": 0.20,
            "day_of_week": 0.05
        }
    
    def _categorize_feature(self, feature: str) -> str:
        """Categorize feature by type."""
        if "temp" in feature.lower() or "humid" in feature.lower():
            return "weather"
        elif "load" in feature.lower():
            return "load"
        elif "price" in feature.lower():
            return "price"
        else:
            return "other"
    
    def _generate_insights(
        self,
        top_features: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate insights from top features."""
        insights = []
        
        if top_features:
            top = top_features[0]
            insights.append(
                f"{top['feature']} is the most important driver with "
                f"{abs(top['importance']):.1%} contribution"
            )
        
        # Add category insights
        categories = {}
        for feat in top_features:
            cat = feat["category"]
            categories[cat] = categories.get(cat, 0) + abs(feat["importance"])
        
        for cat, imp in categories.items():
            if imp > 0.3:
                insights.append(f"{cat.title()} factors contribute {imp:.1%} to predictions")
        
        return insights
    
    def _generate_summary_text(
        self,
        top_features: List[Dict[str, Any]],
        insights: List[str]
    ) -> str:
        """Generate natural language summary."""
        summary_parts = ["Model predictions are primarily driven by:"]
        
        for feat in top_features[:3]:
            summary_parts.append(
                f"- {feat['feature']} ({abs(feat['importance']):.1%} importance)"
            )
        
        if insights:
            summary_parts.append("\nKey insights:")
            summary_parts.extend(f"- {insight}" for insight in insights)
        
        return "\n".join(summary_parts)


class DefaultExplainabilityEngine:
    """Default explainability engine with mock implementations."""
    
    async def compute_shap_values(
        self,
        model_id: str,
        data: List[Dict[str, float]]
    ) -> Dict[str, List[float]]:
        """Compute mock SHAP values."""
        # In production, would use actual SHAP library
        features = list(data[0].keys()) if data else []
        shap_values = {}
        
        for feature in features:
            # Mock SHAP values
            import random
            shap_values[feature] = [
                random.uniform(-0.5, 0.5) for _ in data
            ]
        
        return shap_values
    
    async def compute_feature_importance(
        self,
        model_id: str,
        method: str = "permutation"
    ) -> Dict[str, float]:
        """Compute mock feature importance."""
        # Mock importance scores
        return {
            "temperature": 0.22,
            "humidity": 0.18,
            "load_lag_1h": 0.30,
            "hour_of_day": 0.20,
            "day_of_week": 0.10
        }
