"""Model Comparison Service - Handles model comparison and champion selection."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

try:
    from aurum.telemetry.context import get_request_id, get_tenant_id, get_user_id
    from aurum.observability.telemetry_facade import get_telemetry_facade
except ImportError:
    # Fallback for demo
    def get_telemetry_facade():
        class MockTelemetry:
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
        return MockTelemetry()
    def get_request_id(): return "demo-request"
    def get_tenant_id(): return "demo-tenant"
    def get_user_id(): return "demo-user"
from .models import ModelVersion, ModelComparison, ChampionChallengerSelection
from .interfaces import IModelComparisonService


class ModelComparisonService(IModelComparisonService):
    """Service for comparing models and selecting champions."""

    def __init__(self, management_service=None):
        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()
        self.management_service = management_service
        self.comparisons: Dict[str, ModelComparison] = {}

    async def compare_models(
        self,
        champion_version_id: str,
        challenger_version_id: str,
        compared_by: str
    ) -> ModelComparison:
        """Compare two model versions."""
        try:
            # Get model versions from management service
            if not self.management_service:
                raise ValueError("Management service not available")

            # For simplicity, we'll create mock versions based on IDs
            # In real implementation, would fetch from management service
            champion_version = self._get_mock_version(champion_version_id, "champion")
            challenger_version = self._get_mock_version(challenger_version_id, "challenger")

            if not champion_version or not challenger_version:
                raise ValueError("Model versions not found")

            # Perform comprehensive comparison
            comparison_metrics = self._calculate_comparison_metrics(champion_version, challenger_version)

            # Calculate statistical significance
            statistical_significance = self._calculate_statistical_significance(
                champion_version, challenger_version
            )

            # Assess business impact
            business_impact = self._calculate_business_impact(comparison_metrics)

            # Generate recommendation
            recommendation = self._generate_recommendation(
                comparison_metrics, statistical_significance, business_impact
            )

            # Create comparison record
            comparison = ModelComparison(
                comparison_id=str(uuid4()),
                champion_version=champion_version_id,
                challenger_version=challenger_version_id,
                comparison_metrics=comparison_metrics,
                statistical_significance=statistical_significance,
                business_impact=business_impact,
                recommendation=recommendation,
                compared_by=compared_by
            )

            # Store comparison
            self.comparisons[comparison.comparison_id] = comparison

            self.telemetry.info(
                "model_comparison.comparison_completed",
                comparison_id=comparison.comparison_id,
                champion_version=champion_version_id,
                challenger_version=challenger_version_id,
                recommendation=recommendation,
                compared_by=compared_by
            )

            return comparison

        except Exception as exc:
            self.telemetry.error("Failed to compare models", error=str(exc))
            raise

    async def select_champion_model(
        self,
        model_name: str,
        selection_criteria: Optional[Dict[str, float]] = None,
        selected_by: str = "system"
    ) -> Optional[ModelVersion]:
        """Select the best model version as champion."""
        try:
            if not self.management_service:
                raise ValueError("Management service not available")

            # Get all active model versions
            versions = await self.management_service.list_model_versions(model_name)

            if not versions:
                return None

            # Filter to active and champion-eligible versions
            eligible_versions = [
                v for v in versions
                if v.status in ["active", "champion"]
            ]

            if not eligible_versions:
                return None

            # Use provided criteria or defaults
            criteria = selection_criteria or {
                "accuracy": 0.4,
                "rmse": 0.3,
                "model_size": 0.1,
                "training_time": 0.1,
                "business_impact": 0.1
            }

            # Score each version
            scored_versions = []
            for version in eligible_versions:
                score = self._calculate_champion_score(version, criteria)
                version.champion_score = score
                scored_versions.append((version, score))

            # Select highest scoring version
            best_version, best_score = max(scored_versions, key=lambda x: x[1])

            # Update champion status if needed
            if best_version.status != "champion":
                await self.management_service.update_model_version_status(
                    model_name=model_name,
                    version=best_version.version_number,
                    status="champion",
                    updated_by=selected_by
                )

            self.telemetry.info(
                "model_comparison.champion_selected",
                model_name=model_name,
                selected_version=best_version.version_number,
                score=best_score,
                selected_by=selected_by
            )

            return best_version

        except Exception as exc:
            self.telemetry.error("Failed to select champion model", error=str(exc))
            return None

    async def promote_to_champion(
        self,
        model_name: str,
        version_id: str,
        promoted_by: str
    ) -> bool:
        """Promote a model version to champion."""
        try:
            if not self.management_service:
                raise ValueError("Management service not available")

            # Update version status to champion
            success = await self.management_service.update_model_version_status(
                model_name=model_name,
                version=version_id,  # This should be version number, not ID
                status="champion",
                updated_by=promoted_by
            )

            if success:
                self.telemetry.info(
                    "model_comparison.model_promoted_to_champion",
                    model_name=model_name,
                    version_id=version_id,
                    promoted_by=promoted_by
                )

            return success

        except Exception as exc:
            self.telemetry.error("Failed to promote model to champion", error=str(exc))
            return False

    async def create_champion_challenger_selection(
        self,
        model_name: str,
        champion_version: str,
        challenger_versions: List[str],
        selection_criteria: Dict[str, float],
        created_by: str
    ) -> ChampionChallengerSelection:
        """Create a champion/challenger comparison configuration."""
        try:
            selection = ChampionChallengerSelection(
                model_name=model_name,
                champion_version=champion_version,
                challenger_versions=challenger_versions,
                selection_criteria=selection_criteria,
                created_by=created_by
            )

            self.telemetry.info(
                "model_comparison.champion_challenger_created",
                model_name=model_name,
                champion_version=champion_version,
                challenger_count=len(challenger_versions),
                created_by=created_by
            )

            return selection

        except Exception as exc:
            self.telemetry.error("Failed to create champion/challenger selection", error=str(exc))
            raise

    async def list_champion_challenger_comparisons(
        self,
        model_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ChampionChallengerSelection]:
        """List champion/challenger comparisons."""
        # This would typically query a database
        # For now, return empty list as this is a simplified implementation
        return []

    def _get_mock_version(self, version_id: str, version_type: str) -> Optional[ModelVersion]:
        """Get a mock model version for testing."""
        # In real implementation, this would fetch from management service
        return ModelVersion(
            version_id=version_id,
            model_name="test_model",
            version_number=f"v1.{hash(version_id) % 10}",
            description=f"Mock {version_type} version",
            config=None,  # Would be populated in real implementation
            training_start_date=datetime.utcnow(),
            training_end_date=datetime.utcnow(),
            model_path=f"models/test_model/{version_id}",
            model_size_bytes=1024 * 1024,
            performance_metrics={
                "accuracy": 0.94 if version_type == "champion" else 0.96,
                "rmse": 0.06 if version_type == "champion" else 0.04,
                "r2_score": 0.96 if version_type == "champion" else 0.98
            },
            created_by="test_user",
            status="active"
        )

    def _calculate_comparison_metrics(
        self,
        champion: ModelVersion,
        challenger: ModelVersion
    ) -> Dict[str, float]:
        """Calculate comparison metrics between champion and challenger."""
        return {
            "accuracy_improvement": (
                challenger.performance_metrics.get("accuracy", 0) -
                champion.performance_metrics.get("accuracy", 0)
            ),
            "rmse_improvement": (
                champion.performance_metrics.get("rmse", float('inf')) -
                challenger.performance_metrics.get("rmse", float('inf'))
            ),
            "r2_improvement": (
                challenger.performance_metrics.get("r2_score", 0) -
                champion.performance_metrics.get("r2_score", 0)
            ),
            "model_size_ratio": (
                challenger.model_size_bytes /
                max(champion.model_size_bytes, 1)
            ),
            "training_time_ratio": 1.0  # Would calculate actual time difference
        }

    def _calculate_statistical_significance(
        self,
        champion: ModelVersion,
        challenger: ModelVersion
    ) -> Dict[str, float]:
        """Calculate statistical significance of the comparison."""
        # Simplified statistical calculation
        accuracy_diff = abs(
            challenger.performance_metrics.get("accuracy", 0) -
            champion.performance_metrics.get("accuracy", 0)
        )

        return {
            "p_value": 0.01 if accuracy_diff > 0.02 else 0.15,
            "confidence_level": 0.95,
            "effect_size": accuracy_diff,
            "sample_size": 1000  # Would use actual CV scores
        }

    def _calculate_business_impact(self, metrics: Dict[str, float]) -> Dict[str, float]:
        """Calculate business impact of the model improvement."""
        return {
            "cost_reduction": max(0, 1 - metrics.get("model_size_ratio", 1)) * 0.1,
            "accuracy_improvement": metrics.get("accuracy_improvement", 0),
            "expected_revenue_lift": metrics.get("accuracy_improvement", 0) * 1_000_000,
            "deployment_complexity": (
                0.3 if metrics.get("model_size_ratio", 1) < 1.2 else 0.6
            )
        }

    def _generate_recommendation(
        self,
        metrics: Dict[str, float],
        statistical: Dict[str, float],
        business: Dict[str, float]
    ) -> str:
        """Generate recommendation based on comparison results."""
        score = 0

        # Accuracy improvement (40% weight)
        if metrics.get("accuracy_improvement", 0) > 0.02:
            score += 4
        elif metrics.get("accuracy_improvement", 0) > 0.01:
            score += 2
        elif metrics.get("accuracy_improvement", 0) > 0:
            score += 1

        # Statistical significance (30% weight)
        if statistical.get("p_value", 1) < 0.01:
            score += 3
        elif statistical.get("p_value", 1) < 0.05:
            score += 2

        # Model efficiency (20% weight)
        size_ratio = metrics.get("model_size_ratio", 1)
        if size_ratio < 0.8:  # Smaller model
            score += 2
        elif size_ratio > 1.5:  # Much larger model
            score -= 1

        # Business impact (10% weight)
        if business.get("expected_revenue_lift", 0) > 50000:
            score += 1

        # Final recommendation
        if score >= 5:
            return "promote_challenger"
        elif score >= 3:
            return "needs_review"
        else:
            return "keep_champion"

    def _calculate_champion_score(
        self,
        version: ModelVersion,
        criteria: Dict[str, float]
    ) -> float:
        """Calculate champion score for a model version."""
        score = 0.0

        # Accuracy score
        accuracy = version.performance_metrics.get("accuracy", 0)
        score += accuracy * criteria.get("accuracy", 0.4)

        # RMSE score (lower is better, so invert)
        rmse = version.performance_metrics.get("rmse", 1)
        score += (1 - min(rmse, 1)) * criteria.get("rmse", 0.3)

        # Model size score (smaller is better)
        size_score = min(1.0, 1.0 / (version.model_size_bytes / (1024 * 1024)))
        score += size_score * criteria.get("model_size", 0.1)

        # Training time score (would need actual training time data)
        score += 0.8 * criteria.get("training_time", 0.1)

        # Business impact score (would need actual impact data)
        score += 0.8 * criteria.get("business_impact", 0.1)

        return score

    async def health_check(self) -> bool:
        """Health check for the model comparison service."""
        try:
            # Check if we can access the comparisons dictionary
            if not hasattr(self, 'comparisons') or self.comparisons is None:
                return False

            # Check if management service is available (if configured)
            if hasattr(self, 'management_service') and self.management_service is None:
                return False

            return True

        except Exception as exc:
            self.logger.error(f"Health check failed: {exc}")
            return False

    def get_service_health(self) -> Dict[str, Any]:
        """Get detailed health information for the service."""
        return {
            "healthy": True,  # Would be determined by health_check()
            "service_name": "ModelComparisonService",
            "comparisons_count": len(self.comparisons),
            "management_service_available": self.management_service is not None,
            "last_health_check": datetime.utcnow().isoformat()
        }
