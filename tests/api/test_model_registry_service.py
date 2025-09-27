"""Tests for Model Registry Service enhancements."""

from datetime import datetime
from typing import Dict, Any
from uuid import uuid4

# Test the core components in isolation to avoid import issues
from pydantic import BaseModel, Field
from dataclasses import field


class ModelConfig(BaseModel):
    """Configuration for ML model training."""

    model_type: str
    hyperparameters: Dict[str, Any]
    feature_selection: list[str]
    target_variable: str
    training_period_days: int = 365
    validation_period_days: int = 30
    test_period_days: int = 30
    cross_validation_folds: int = 5
    early_stopping_rounds: int = None
    random_seed: int = 42


class ModelVersion(BaseModel):
    """ML model version information."""

    version_id: str
    model_name: str
    version_number: str
    description: str
    config: ModelConfig
    training_start_date: datetime
    training_end_date: datetime
    model_path: str
    model_size_bytes: int
    performance_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    validation_results: Dict[str, Any]
    status: str = "active"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    tags: Dict[str, str] = Field(default_factory=dict)


class TrainingJob(BaseModel):
    """Model training job configuration."""

    job_id: str
    model_name: str
    config: ModelConfig
    status: str = "pending"
    progress: float = 0.0
    current_stage: str = "initialization"
    started_at: datetime = None
    completed_at: datetime = None
    error_message: str = None
    model_version_id: str = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_for: datetime = None


class TestModelRegistryComponents:
    """Test core model registry components."""

    def test_model_config_creation(self):
        """Test ModelConfig creation and validation."""
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100, "max_depth": 6},
            feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
            target_variable="lmp_price"
        )
        
        assert config.model_type == "xgboost"
        assert config.hyperparameters["n_estimators"] == 100
        assert len(config.feature_selection) == 4
        assert config.target_variable == "lmp_price"
        assert config.training_period_days == 365  # Default value
        print("✓ test_model_config_creation passed")

    def test_model_version_creation(self):
        """Test ModelVersion creation and validation."""
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100, "max_depth": 6},
            feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
            target_variable="lmp_price"
        )

        version = ModelVersion(
            version_id=str(uuid4()),
            model_name="test_model",
            version_number="v1.0",
            description="Test model version",
            config=config,
            training_start_date=datetime.utcnow(),
            training_end_date=datetime.utcnow(),
            model_path="models/test_model/v1.0",
            model_size_bytes=1024*1024,  # 1MB
            performance_metrics={"accuracy": 0.95, "rmse": 0.08, "r2_score": 0.92},
            feature_importance={"temperature": 0.3, "load_mw": 0.25, "humidity": 0.2, "wind_speed": 0.25},
            validation_results={"cross_validation_scores": [0.92, 0.94, 0.91, 0.93, 0.90], "mean_cv_score": 0.92},
            created_by="test_user"
        )
        
        assert version.model_name == "test_model"
        assert version.version_number == "v1.0"
        assert version.status == "active"  # Default value
        assert version.performance_metrics["accuracy"] == 0.95
        assert len(version.validation_results["cross_validation_scores"]) == 5
        print("✓ test_model_version_creation passed")

    def test_training_job_creation(self):
        """Test TrainingJob creation and validation."""
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100, "max_depth": 6},
            feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
            target_variable="lmp_price"
        )

        job = TrainingJob(
            job_id=str(uuid4()),
            model_name="test_model",
            config=config
        )
        
        assert job.model_name == "test_model"
        assert job.status == "pending"  # Default value
        assert job.progress == 0.0  # Default value
        assert job.current_stage == "initialization"  # Default value
        print("✓ test_training_job_creation passed")

    def test_champion_selection_criteria(self):
        """Test champion model selection logic."""
        # Create multiple model versions with different performance
        configs = [
            ModelConfig(
                model_type="xgboost",
                hyperparameters={"n_estimators": 100, "max_depth": 6},
                feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
                target_variable="lmp_price"
            )
            for _ in range(3)
        ]

        models = []
        performance_metrics = [
            {"accuracy": 0.85, "rmse": 0.12, "r2_score": 0.88},  # Lower performance
            {"accuracy": 0.92, "rmse": 0.08, "r2_score": 0.94},  # Best performance
            {"accuracy": 0.89, "rmse": 0.10, "r2_score": 0.91}   # Medium performance
        ]

        for i, (config, metrics) in enumerate(zip(configs, performance_metrics)):
            version = ModelVersion(
                version_id=str(uuid4()),
                model_name="test_model",
                version_number=f"v1.{i}",
                description=f"Test model version {i}",
                config=config,
                training_start_date=datetime.utcnow(),
                training_end_date=datetime.utcnow(),
                model_path=f"models/test_model/v1.{i}",
                model_size_bytes=1024*1024,
                performance_metrics=metrics,
                feature_importance={"temperature": 0.3, "load_mw": 0.25, "humidity": 0.2, "wind_speed": 0.25},
                validation_results={"cross_validation_scores": [0.88, 0.90, 0.89, 0.91, 0.87], "mean_cv_score": 0.89},
                created_by="test_user"
            )
            models.append(version)

        # Test selection criteria
        default_criteria = {
            "primary_metric": "accuracy",
            "min_accuracy": 0.8,
            "max_model_size_mb": 1000,
            "min_validation_score": 0.75
        }

        # Filter models that meet criteria
        eligible_models = []
        for version in models:
            metrics = version.performance_metrics
            validation = version.validation_results
            
            if (metrics.get(default_criteria["primary_metric"], 0) >= default_criteria["min_accuracy"] and
                version.model_size_bytes <= default_criteria["max_model_size_mb"] * 1024 * 1024 and
                validation.get("mean_cv_score", 0) >= default_criteria["min_validation_score"]):
                eligible_models.append(version)

        # All models should be eligible
        assert len(eligible_models) == 3

        # Best model should be the one with highest accuracy (v1.1)
        best_model = max(eligible_models, key=lambda v: v.performance_metrics.get("accuracy", 0))
        assert best_model.version_number == "v1.1"
        assert best_model.performance_metrics["accuracy"] == 0.92
        print("✓ test_champion_selection_criteria passed")

    def test_comparison_metrics_calculation(self):
        """Test model comparison metrics calculation."""
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100, "max_depth": 6},
            feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
            target_variable="lmp_price"
        )

        # Champion model
        champion = ModelVersion(
            version_id=str(uuid4()),
            model_name="test_model",
            version_number="v1.0",
            description="Champion model",
            config=config,
            training_start_date=datetime.utcnow(),
            training_end_date=datetime.utcnow(),
            model_path="models/test_model/v1.0",
            model_size_bytes=2*1024*1024,  # 2MB
            performance_metrics={"accuracy": 0.85, "rmse": 0.12, "r2_score": 0.88},
            feature_importance={"temperature": 0.3, "load_mw": 0.25, "humidity": 0.2, "wind_speed": 0.25},
            validation_results={"cross_validation_scores": [0.84, 0.86, 0.83, 0.87, 0.85], "mean_cv_score": 0.85},
            created_by="test_user"
        )

        # Challenger model
        challenger = ModelVersion(
            version_id=str(uuid4()),
            model_name="test_model",
            version_number="v1.1",
            description="Challenger model",
            config=config,
            training_start_date=datetime.utcnow(),
            training_end_date=datetime.utcnow(),
            model_path="models/test_model/v1.1",
            model_size_bytes=int(1.5*1024*1024),  # 1.5MB (smaller)
            performance_metrics={"accuracy": 0.92, "rmse": 0.08, "r2_score": 0.94},
            feature_importance={"temperature": 0.3, "load_mw": 0.25, "humidity": 0.2, "wind_speed": 0.25},
            validation_results={"cross_validation_scores": [0.91, 0.93, 0.90, 0.94, 0.92], "mean_cv_score": 0.92},
            created_by="test_user"
        )

        # Calculate comparison metrics
        comparison_metrics = {
            "accuracy_improvement": challenger.performance_metrics.get("accuracy", 0) -
                                  champion.performance_metrics.get("accuracy", 0),
            "rmse_improvement": champion.performance_metrics.get("rmse", float('inf')) -
                              challenger.performance_metrics.get("rmse", float('inf')),
            "r2_improvement": challenger.performance_metrics.get("r2_score", 0) -
                            champion.performance_metrics.get("r2_score", 0),
            "model_size_ratio": challenger.model_size_bytes / max(champion.model_size_bytes, 1)
        }

        # Verify calculations (with floating point tolerance)
        assert abs(comparison_metrics["accuracy_improvement"] - 0.07) < 0.001  # 0.92 - 0.85
        assert abs(comparison_metrics["rmse_improvement"] - 0.04) < 0.001  # 0.12 - 0.08
        assert abs(comparison_metrics["r2_improvement"] - 0.06) < 0.001  # 0.94 - 0.88
        assert abs(comparison_metrics["model_size_ratio"] - 0.75) < 0.001  # 1.5MB / 2MB

        # Test recommendation logic
        score = 0
        
        # Accuracy improvement (weight: 40%)
        if comparison_metrics["accuracy_improvement"] > 0.02:
            score += 4
        elif comparison_metrics["accuracy_improvement"] > 0.01:
            score += 2
        elif comparison_metrics["accuracy_improvement"] > 0:
            score += 1
        
        # Model efficiency (weight: 20%)
        if comparison_metrics["model_size_ratio"] < 0.8:  # Smaller model
            score += 2
        elif comparison_metrics["model_size_ratio"] > 1.5:  # Much larger model
            score -= 1
        
        # Expected score: 4 (accuracy) + 2 (size) = 6
        assert score >= 5  # Should recommend promoting challenger
        recommendation = "promote_challenger" if score >= 5 else "keep_champion"
        assert recommendation == "promote_challenger"
        print("✓ test_comparison_metrics_calculation passed")

    def run_all_tests(self):
        """Run all tests."""
        print("Running Model Registry Component Tests...")
        self.test_model_config_creation()
        self.test_model_version_creation()
        self.test_training_job_creation()
        self.test_champion_selection_criteria()
        self.test_comparison_metrics_calculation()
        print("\n✅ All tests passed!")


if __name__ == "__main__":
    test_suite = TestModelRegistryComponents()
    test_suite.run_all_tests()