"""Pydantic models for scenario management."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .models import AurumBaseModel


class CreateScenarioRequest(AurumBaseModel):
    """Request model for creating a scenario."""

    tenant_id: str = Field(..., description="Tenant identifier")
    name: str = Field(..., description="Scenario name")
    description: Optional[str] = Field(None, description="Scenario description")
    assumptions: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of scenario assumptions"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Scenario parameters"
    )
    tags: List[str] = Field(default_factory=list, description="Scenario tags")


class ScenarioData(AurumBaseModel):
    """Scenario data model."""

    id: str = Field(..., description="Scenario ID")
    tenant_id: str = Field(..., description="Tenant identifier")
    name: str = Field(..., description="Scenario name")
    description: Optional[str] = Field(None, description="Scenario description")
    status: str = Field(..., description="Scenario status")
    assumptions: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of scenario assumptions"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Scenario parameters"
    )
    tags: List[str] = Field(default_factory=list, description="Scenario tags")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="Creator identifier")
    version: int = Field(default=1, description="Scenario version")


class ScenarioResponse(AurumBaseModel):
    """Response model for scenario operations."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: ScenarioData = Field(..., description="Scenario data")


class ScenarioListResponse(AurumBaseModel):
    """Response model for listing scenarios."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List[ScenarioData] = Field(..., description="List of scenarios")


class ScenarioRunOptions(AurumBaseModel):
    """Options for running a scenario."""

    priority: str = Field(default="normal", description="Run priority")
    timeout_minutes: int = Field(default=60, description="Timeout in minutes")
    max_memory_mb: int = Field(default=1024, description="Max memory in MB")
    environment: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Runtime parameters"
    )


class ScenarioRunData(AurumBaseModel):
    """Scenario run data model."""

    id: str = Field(..., description="Run ID")
    scenario_id: str = Field(..., description="Parent scenario ID")
    status: str = Field(..., description="Run status")
    priority: str = Field(..., description="Run priority")
    started_at: Optional[datetime] = Field(None, description="Start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    duration_seconds: Optional[float] = Field(None, description="Duration in seconds")
    error_message: Optional[str] = Field(None, description="Error message")
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Run parameters"
    )
    environment: Dict[str, str] = Field(
        default_factory=dict,
        description="Run environment"
    )
    created_at: datetime = Field(..., description="Creation timestamp")


class ScenarioRunResponse(AurumBaseModel):
    """Response model for scenario run operations."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: ScenarioRunData = Field(..., description="Scenario run data")


class ScenarioRunListResponse(AurumBaseModel):
    """Response model for listing scenario runs."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List[ScenarioRunData] = Field(..., description="List of scenario runs")


class ScenarioOutputPoint(AurumBaseModel):
    """Individual output point from scenario run."""

    timestamp: datetime = Field(..., description="Output timestamp")
    metric_name: str = Field(..., description="Metric name")
    value: float = Field(..., description="Metric value")
    unit: str = Field(..., description="Unit of measurement")
    tags: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional tags"
    )


class ScenarioOutputResponse(AurumBaseModel):
    """Response model for scenario outputs."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List[ScenarioOutputPoint] = Field(..., description="Output data points")


class ScenarioMetricLatest(AurumBaseModel):
    """Latest metrics for a scenario."""

    scenario_id: str = Field(..., description="Scenario ID")
    metric_name: str = Field(..., description="Metric name")
    value: float = Field(..., description="Latest value")
    unit: str = Field(..., description="Unit of measurement")
    timestamp: datetime = Field(..., description="Metric timestamp")
    trend: Optional[str] = Field(None, description="Trend direction")
    change_percent: Optional[float] = Field(None, description="Percentage change")


class ScenarioMetricLatestResponse(AurumBaseModel):
    """Response model for latest scenario metrics."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: ScenarioMetricLatest = Field(..., description="Latest metrics")


# Legacy models for backward compatibility
ScenarioRequest = CreateScenarioRequest
Scenario = ScenarioData
ScenarioList = ScenarioListResponse
ScenarioRun = ScenarioRunData
ScenarioRunList = ScenarioRunListResponse
ScenarioOutput = ScenarioOutputResponse
