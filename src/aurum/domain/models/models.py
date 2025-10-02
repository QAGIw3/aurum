"""Domain models for model management services."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from uuid import uuid4

from pydantic import BaseModel, Field


class ModelConfig(BaseModel):
    """Configuration for ML model training."""

    model_type: str  # "linear_regression", "random_forest", "xgboost", "neural_network"
    hyperparameters: Dict[str, Any]
    feature_selection: List[str]
    target_variable: str
    training_period_days: int = 365
    validation_period_days: int = 30
    test_period_days: int = 30
    cross_validation_folds: int = 5
    early_stopping_rounds: Optional[int] = None
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
    performance_metrics: Dict[str, float] = Field(default_factory=dict)
    feature_importance: Dict[str, float] = Field(default_factory=dict)
    validation_results: Dict[str, Any] = Field(default_factory=dict)
    status: str = "active"  # "active", "deprecated", "archived"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    champion_score: Optional[float] = None


class TrainingJob(BaseModel):
    """Model training job configuration."""

    job_id: str
    model_name: str
    config: ModelConfig
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    progress: float = 0.0
    current_stage: str = ""
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    model_version: Optional[ModelVersion] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    tags: Dict[str, str] = Field(default_factory=dict)


class ModelComparison(BaseModel):
    """Result of model comparison between champion and challenger."""

    comparison_id: str
    champion_version: str
    challenger_version: str
    comparison_metrics: Dict[str, float]
    statistical_significance: Dict[str, float]
    business_impact: Dict[str, float]
    recommendation: str  # "keep_champion", "promote_challenger", "needs_review"
    confidence_level: float = 0.95
    comparison_date: datetime = Field(default_factory=datetime.utcnow)
    compared_by: str


class RetrainSchedule(BaseModel):
    """Schedule for automated model retraining."""

    schedule_id: str
    model_name: str
    cron_expression: str
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    config: ModelConfig
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str


class RegisteredModel(BaseModel):
    """Registered model metadata."""

    model_name: str
    description: str
    model_type: str
    latest_version: str
    champion_version: Optional[str] = None
    total_versions: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    tags: Dict[str, str] = Field(default_factory=dict)


class ChampionChallengerSelection(BaseModel):
    """Champion/Challenger model selection configuration."""

    model_name: str
    champion_version: str
    challenger_versions: List[str]
    selection_criteria: Dict[str, float]  # weights for different metrics
    auto_promotion_threshold: float = 0.05  # 5% improvement threshold
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
