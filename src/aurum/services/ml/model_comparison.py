"""Model Comparison Service.

This service handles champion/challenger model comparison with statistical
significance testing and business impact assessment.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from uuid import uuid4
from statistics import mean, stdev

from pydantic import BaseModel, Field
import numpy as np

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class ModelComparison(BaseModel):
    """Represents a comparison between two model versions."""
    
    comparison_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    champion_version: str
    challenger_version: str
    comparison_metrics: Dict[str, float] = Field(default_factory=dict)
    statistical_significance: Dict[str, Any] = Field(default_factory=dict)
    business_impact: Dict[str, float] = Field(default_factory=dict)
    recommendation: str
    comparison_date: datetime = Field(default_factory=datetime.utcnow)
    performed_by: str = "system"
    test_data_summary: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ComparisonRepository(BaseRepository):
    """Repository interface for model comparison operations."""
    
    async def save_comparison(self, comparison: ModelComparison) -> ModelComparison:
        """Save a model comparison."""
        raise NotImplementedError
    
    async def get_comparison(self, comparison_id: str) -> Optional[ModelComparison]:
        """Get a comparison by ID."""
        raise NotImplementedError
    
    async def list_comparisons(
        self,
        model_name: Optional[str] = None,
        champion_version: Optional[str] = None,
        challenger_version: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelComparison]:
        """List comparisons with optional filters."""
        raise NotImplementedError


class ModelComparisonService(BaseService):
    """
    Champion/challenger model comparison service.
    
    This service provides comprehensive model comparison functionality including
    statistical significance testing, business impact assessment, and
    recommendation generation.
    """
    
    def __init__(
        self,
        repository: Optional[ComparisonRepository] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 300
    ):
        """
        Initialize the model comparison service.
        
        Args:
            repository: Repository for data persistence
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.logger = logging.getLogger(__name__)
        
        # In-memory comparison cache
        self._comparisons: Dict[str, ModelComparison] = {}
    
    def _get_default_repository(self) -> ComparisonRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        # For now, return a mock repository
        class MockRepository(ComparisonRepository):
            def __init__(self):
                self.comparisons = {}
            
            async def save_comparison(self, comparison: ModelComparison) -> ModelComparison:
                self.comparisons[comparison.comparison_id] = comparison
                return comparison
            
            async def get_comparison(self, comparison_id: str) -> Optional[ModelComparison]:
                return self.comparisons.get(comparison_id)
            
            async def list_comparisons(self, **kwargs) -> List[ModelComparison]:
                comparisons = list(self.comparisons.values())
                # Apply filters
                if kwargs.get('model_name'):
                    comparisons = [c for c in comparisons if c.model_name == kwargs['model_name']]
                return comparisons[:kwargs.get('limit', 100)]
        
        return MockRepository()
    
    async def compare_models(
        self,
        model_name: str,
        champion_version: str,
        challenger_version: str,
        champion_metrics: Dict[str, float],
        challenger_metrics: Dict[str, float],
        test_data: Optional[Tuple[Dict[str, List[float]], List[float]]] = None,
        comparison_config: Optional[Dict[str, Any]] = None
    ) -> ModelComparison:
        """
        Compare two model versions for champion/challenger testing.
        
        Args:
            model_name: Name of the model being compared
            champion_version: Champion version identifier
            challenger_version: Challenger version identifier
            champion_metrics: Performance metrics for champion
            challenger_metrics: Performance metrics for challenger
            test_data: Optional test data predictions and actuals
            comparison_config: Configuration for comparison thresholds
            
        Returns:
            ModelComparison with detailed results and recommendation
        """
        try:
            # Calculate comparison metrics
            comparison_metrics = await self._calculate_comparison_metrics(
                champion_metrics,
                challenger_metrics
            )
            
            # Perform statistical significance testing
            statistical_significance = await self._calculate_statistical_significance(
                champion_metrics,
                challenger_metrics,
                test_data
            )
            
            # Assess business impact
            business_impact = await self._calculate_business_impact(
                comparison_metrics,
                champion_metrics,
                challenger_metrics,
                comparison_config
            )
            
            # Generate recommendation
            recommendation = await self._generate_recommendation(
                comparison_metrics,
                statistical_significance,
                business_impact,
                comparison_config
            )
            
            # Create comparison object
            comparison = ModelComparison(
                model_name=model_name,
                champion_version=champion_version,
                challenger_version=challenger_version,
                comparison_metrics=comparison_metrics,
                statistical_significance=statistical_significance,
                business_impact=business_impact,
                recommendation=recommendation
            )
            
            if test_data:
                comparison.test_data_summary = {
                    "test_size": len(test_data[1]) if test_data[1] else 0,
                    "features_used": list(test_data[0].keys()) if test_data[0] else []
                }
            
            # Save comparison
            comparison = await self.repository.save_comparison(comparison)
            self._comparisons[comparison.comparison_id] = comparison
            
            self.logger.info(
                f"Model comparison completed: {model_name} - {recommendation}",
                extra={
                    "comparison_id": comparison.comparison_id,
                    "champion": champion_version,
                    "challenger": challenger_version,
                    "recommendation": recommendation
                }
            )
            
            # Emit metric
            await self._emit_metric(
                "model_comparison_completed",
                tags={
                    "model_name": model_name,
                    "recommendation": recommendation
                }
            )
            
            return comparison
            
        except Exception as e:
            self.logger.error(
                f"Failed to compare models: {e}",
                extra={
                    "model_name": model_name,
                    "champion": champion_version,
                    "challenger": challenger_version
                }
            )
            raise
    
    async def _calculate_comparison_metrics(
        self,
        champion_metrics: Dict[str, float],
        challenger_metrics: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate comparison metrics between champion and challenger."""
        metrics = {}
        
        # Accuracy improvement
        champion_acc = champion_metrics.get("accuracy", 0)
        challenger_acc = challenger_metrics.get("accuracy", 0)
        metrics["accuracy_improvement"] = challenger_acc - champion_acc
        metrics["accuracy_improvement_pct"] = (
            (challenger_acc - champion_acc) / champion_acc * 100 
            if champion_acc > 0 else 0
        )
        
        # RMSE improvement (lower is better)
        champion_rmse = champion_metrics.get("rmse", float('inf'))
        challenger_rmse = challenger_metrics.get("rmse", float('inf'))
        metrics["rmse_improvement"] = champion_rmse - challenger_rmse
        metrics["rmse_improvement_pct"] = (
            (champion_rmse - challenger_rmse) / champion_rmse * 100
            if champion_rmse > 0 else 0
        )
        
        # R² improvement
        champion_r2 = champion_metrics.get("r2_score", 0)
        challenger_r2 = challenger_metrics.get("r2_score", 0)
        metrics["r2_improvement"] = challenger_r2 - champion_r2
        
        # Model size comparison
        champion_size = champion_metrics.get("model_size_bytes", 1)
        challenger_size = challenger_metrics.get("model_size_bytes", 1)
        metrics["model_size_ratio"] = challenger_size / champion_size if champion_size > 0 else 1
        
        # Training time comparison
        champion_time = champion_metrics.get("training_time_seconds", 1)
        challenger_time = challenger_metrics.get("training_time_seconds", 1)
        metrics["training_time_ratio"] = challenger_time / champion_time if champion_time > 0 else 1
        
        # Inference time comparison (if available)
        champion_inf_time = champion_metrics.get("avg_inference_ms", 0)
        challenger_inf_time = challenger_metrics.get("avg_inference_ms", 0)
        if champion_inf_time > 0 and challenger_inf_time > 0:
            metrics["inference_speedup"] = champion_inf_time / challenger_inf_time
        
        return metrics
    
    async def _calculate_statistical_significance(
        self,
        champion_metrics: Dict[str, float],
        challenger_metrics: Dict[str, float],
        test_data: Optional[Tuple[Dict[str, List[float]], List[float]]] = None
    ) -> Dict[str, Any]:
        """Calculate statistical significance of performance differences."""
        significance = {}
        
        # Get cross-validation scores if available
        champion_cv_scores = champion_metrics.get("cross_validation_scores", [])
        challenger_cv_scores = challenger_metrics.get("cross_validation_scores", [])
        
        if champion_cv_scores and challenger_cv_scores:
            # Calculate p-value using simple t-test approximation
            # In production, use scipy.stats.ttest_ind
            champion_mean = mean(champion_cv_scores)
            challenger_mean = mean(challenger_cv_scores)
            
            champion_std = stdev(champion_cv_scores) if len(champion_cv_scores) > 1 else 0
            challenger_std = stdev(challenger_cv_scores) if len(challenger_cv_scores) > 1 else 0
            
            # Simple effect size calculation (Cohen's d)
            pooled_std = np.sqrt((champion_std**2 + challenger_std**2) / 2) if champion_std + challenger_std > 0 else 1
            effect_size = abs(challenger_mean - champion_mean) / pooled_std
            
            # Simplified p-value estimation
            # In real implementation, use proper statistical tests
            if effect_size > 0.8:  # Large effect
                p_value = 0.01
            elif effect_size > 0.5:  # Medium effect
                p_value = 0.05
            elif effect_size > 0.2:  # Small effect
                p_value = 0.10
            else:
                p_value = 0.50
            
            significance["p_value"] = p_value
            significance["effect_size"] = effect_size
            significance["champion_mean"] = champion_mean
            significance["challenger_mean"] = challenger_mean
            significance["champion_std"] = champion_std
            significance["challenger_std"] = challenger_std
            significance["sample_size"] = len(champion_cv_scores) + len(challenger_cv_scores)
            significance["statistically_significant"] = p_value < 0.05
        else:
            # No CV scores available
            significance["p_value"] = None
            significance["effect_size"] = None
            significance["statistically_significant"] = False
            significance["note"] = "No cross-validation scores available for statistical testing"
        
        significance["confidence_level"] = 0.95
        
        return significance
    
    async def _calculate_business_impact(
        self,
        comparison_metrics: Dict[str, float],
        champion_metrics: Dict[str, float],
        challenger_metrics: Dict[str, float],
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, float]:
        """Calculate business impact of switching to challenger model."""
        impact = {}
        config = config or {}
        
        # Cost reduction from smaller model
        size_reduction = 1 - comparison_metrics.get("model_size_ratio", 1)
        storage_cost_per_gb = config.get("storage_cost_per_gb", 0.023)  # S3 pricing
        model_size_gb = challenger_metrics.get("model_size_bytes", 0) / (1024**3)
        impact["storage_cost_savings_monthly"] = size_reduction * model_size_gb * storage_cost_per_gb * 30
        
        # Performance improvement value
        accuracy_improvement = comparison_metrics.get("accuracy_improvement", 0)
        revenue_per_accuracy_point = config.get("revenue_per_accuracy_point", 10000)
        impact["revenue_lift_monthly"] = accuracy_improvement * revenue_per_accuracy_point
        
        # Inference cost impact
        inference_speedup = comparison_metrics.get("inference_speedup", 1)
        monthly_predictions = config.get("monthly_predictions", 1000000)
        cost_per_1k_predictions = config.get("cost_per_1k_predictions", 0.01)
        current_inference_cost = (monthly_predictions / 1000) * cost_per_1k_predictions
        impact["inference_cost_savings_monthly"] = current_inference_cost * (1 - 1/inference_speedup) if inference_speedup > 0 else 0
        
        # Training cost impact
        training_time_ratio = comparison_metrics.get("training_time_ratio", 1)
        monthly_retrains = config.get("monthly_retrains", 4)
        cost_per_training_hour = config.get("cost_per_training_hour", 10)
        champion_training_hours = champion_metrics.get("training_time_seconds", 3600) / 3600
        training_cost_diff = champion_training_hours * cost_per_training_hour * (training_time_ratio - 1)
        impact["training_cost_impact_monthly"] = training_cost_diff * monthly_retrains
        
        # Total monthly impact
        impact["total_monthly_impact"] = (
            impact["revenue_lift_monthly"] +
            impact["storage_cost_savings_monthly"] +
            impact["inference_cost_savings_monthly"] -
            impact["training_cost_impact_monthly"]
        )
        
        # Risk assessment
        impact["deployment_risk_score"] = self._calculate_risk_score(
            comparison_metrics,
            champion_metrics,
            challenger_metrics
        )
        
        return impact
    
    def _calculate_risk_score(
        self,
        comparison_metrics: Dict[str, float],
        champion_metrics: Dict[str, float],
        challenger_metrics: Dict[str, float]
    ) -> float:
        """Calculate deployment risk score (0-1, higher is riskier)."""
        risk_score = 0.0
        
        # Model size increase risk
        size_ratio = comparison_metrics.get("model_size_ratio", 1)
        if size_ratio > 2:
            risk_score += 0.2
        elif size_ratio > 1.5:
            risk_score += 0.1
        
        # Training time increase risk
        time_ratio = comparison_metrics.get("training_time_ratio", 1)
        if time_ratio > 2:
            risk_score += 0.2
        elif time_ratio > 1.5:
            risk_score += 0.1
        
        # Performance degradation risk
        acc_improvement = comparison_metrics.get("accuracy_improvement", 0)
        if acc_improvement < -0.02:  # More than 2% worse
            risk_score += 0.4
        elif acc_improvement < 0:  # Any degradation
            risk_score += 0.2
        
        # Model complexity risk (based on parameter count if available)
        champion_params = champion_metrics.get("parameter_count", 0)
        challenger_params = challenger_metrics.get("parameter_count", 0)
        if challenger_params > champion_params * 2:
            risk_score += 0.1
        
        return min(risk_score, 1.0)
    
    async def _generate_recommendation(
        self,
        comparison_metrics: Dict[str, float],
        statistical_significance: Dict[str, Any],
        business_impact: Dict[str, float],
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate recommendation based on comparison results.
        
        Returns one of:
        - "promote_challenger": Challenger should replace champion
        - "keep_champion": Keep current champion
        - "needs_more_testing": More data needed for decision
        - "monitor_both": Deploy shadow mode for further evaluation
        """
        config = config or {}
        
        # Scoring system
        score = 0
        max_score = 0
        
        # Performance improvement (weight: 40%)
        acc_improvement = comparison_metrics.get("accuracy_improvement", 0)
        if acc_improvement > config.get("min_accuracy_improvement", 0.02):
            score += 4
        elif acc_improvement > 0:
            score += 2
        elif acc_improvement < -config.get("max_accuracy_degradation", 0.01):
            score -= 4
        max_score += 4
        
        # Statistical significance (weight: 20%)
        if statistical_significance.get("statistically_significant", False):
            score += 2
        elif statistical_significance.get("p_value", 1.0) < 0.10:
            score += 1
        max_score += 2
        
        # Business impact (weight: 20%)
        total_impact = business_impact.get("total_monthly_impact", 0)
        min_positive_impact = config.get("min_monthly_impact", 1000)
        if total_impact > min_positive_impact:
            score += 2
        elif total_impact > 0:
            score += 1
        elif total_impact < -min_positive_impact:
            score -= 2
        max_score += 2
        
        # Risk assessment (weight: 10%)
        risk_score = business_impact.get("deployment_risk_score", 0)
        if risk_score < 0.2:
            score += 1
        elif risk_score > 0.5:
            score -= 1
        max_score += 1
        
        # Model efficiency (weight: 10%)
        size_ratio = comparison_metrics.get("model_size_ratio", 1)
        if size_ratio < 0.8 and acc_improvement >= 0:  # Smaller and not worse
            score += 1
        elif size_ratio > 1.5 and acc_improvement < 0.05:  # Much larger without major improvement
            score -= 1
        max_score += 1
        
        # Generate recommendation
        score_percentage = score / max_score if max_score > 0 else 0
        
        if score_percentage >= 0.7:
            recommendation = "promote_challenger"
        elif score_percentage >= 0.5:
            recommendation = "monitor_both"
        elif score_percentage >= 0.3:
            recommendation = "needs_more_testing"
        else:
            recommendation = "keep_champion"
        
        # Override based on critical conditions
        if acc_improvement < -config.get("critical_accuracy_degradation", 0.05):
            recommendation = "keep_champion"
        elif risk_score > config.get("max_acceptable_risk", 0.8):
            recommendation = "needs_more_testing"
        
        return recommendation
    
    async def get_comparison(self, comparison_id: str) -> Optional[ModelComparison]:
        """
        Get a specific model comparison.
        
        Args:
            comparison_id: Comparison identifier
            
        Returns:
            ModelComparison if found, None otherwise
        """
        # Check memory cache
        if comparison_id in self._comparisons:
            return self._comparisons[comparison_id]
        
        # Check persistent cache
        cache_key = f"comparison:{comparison_id}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                comparison = ModelComparison(**cached)
                self._comparisons[comparison_id] = comparison
                return comparison
        
        # Load from repository
        comparison = await self.repository.get_comparison(comparison_id)
        if comparison:
            self._comparisons[comparison_id] = comparison
            if self.cache_enabled:
                await self._set_cache(cache_key, comparison.dict(), ttl=self.cache_ttl)
        
        return comparison
    
    async def list_comparisons(
        self,
        model_name: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[ModelComparison]:
        """
        List model comparisons with optional filters.
        
        Args:
            model_name: Filter by model name
            limit: Maximum results to return
            offset: Pagination offset
            
        Returns:
            List of ModelComparison instances
        """
        return await self.repository.list_comparisons(
            model_name=model_name,
            limit=limit,
            offset=offset
        )
    
    async def get_latest_comparison(
        self,
        model_name: str,
        champion_version: Optional[str] = None,
        challenger_version: Optional[str] = None
    ) -> Optional[ModelComparison]:
        """
        Get the most recent comparison for a model or version pair.
        
        Args:
            model_name: Model name
            champion_version: Optional champion version filter
            challenger_version: Optional challenger version filter
            
        Returns:
            Most recent ModelComparison if found
        """
        comparisons = await self.list_comparisons(model_name=model_name, limit=100)
        
        # Filter by versions if specified
        if champion_version:
            comparisons = [c for c in comparisons if c.champion_version == champion_version]
        if challenger_version:
            comparisons = [c for c in comparisons if c.challenger_version == challenger_version]
        
        # Return most recent
        if comparisons:
            return max(comparisons, key=lambda c: c.comparison_date)
        
        return None
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
