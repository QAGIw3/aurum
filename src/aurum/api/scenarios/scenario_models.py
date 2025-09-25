"""Pydantic models for scenario management."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from aurum.core.models import AurumBaseModel


class ScenarioStatus(str, Enum):
    """Scenario status lifecycle."""
    CREATED = "created"
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


class ScenarioRunStatus(str, Enum):
    """Scenario run status lifecycle."""
    QUEUED = "queued"          # Run has been queued for execution
    RUNNING = "running"        # Run is currently executing
    SUCCEEDED = "succeeded"    # Run completed successfully
    FAILED = "failed"          # Run failed with error
    CANCELLED = "cancelled"    # Run was cancelled
    TIMEOUT = "timeout"        # Run timed out


class ScenarioRunPriority(str, Enum):
    """Scenario run priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


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
    status: ScenarioStatus = Field(..., description="Scenario status")
    unique_key: str = Field(..., description="Deterministic scenario key")
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
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="Creator identifier")
    version: int = Field(default=1, description="Scenario version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Scenario metadata")


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

    code_version: Optional[str] = Field(None, description="Scenario engine code version")
    seed: Optional[int] = Field(None, description="Random seed for deterministic runs")
    priority: ScenarioRunPriority = Field(default=ScenarioRunPriority.NORMAL, description="Run priority")
    timeout_minutes: int = Field(default=60, description="Timeout in minutes", ge=1, le=1440)  # Max 24 hours
    max_memory_mb: int = Field(default=1024, description="Max memory in MB", ge=256, le=16384)  # 256MB to 16GB
    environment: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Runtime parameters"
    )
    max_retries: int = Field(default=3, description="Maximum retry attempts", ge=0, le=10)
    idempotency_key: Optional[str] = Field(None, description="Idempotency key for deduplication")

    @field_validator("idempotency_key")
    def _validate_idempotency_key(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("idempotency_key cannot be empty")
        if len(trimmed) > 255:
            raise ValueError("idempotency_key cannot exceed 255 characters")
        if not all(ch.isalnum() or ch in "_-.:/" for ch in trimmed):
            raise ValueError(
                "idempotency_key can only contain alphanumeric characters, underscore, hyphen, dot, colon, or slash"
            )
        return trimmed


# Backward compatibility aliases for legacy imports
ScenarioCreateRequest = CreateScenarioRequest  # backwards compatibility for v1 imports
ScenarioRunCreateRequest = ScenarioRunOptions  # backwards compatibility for v1 imports


class ScenarioRunData(AurumBaseModel):
    """Scenario run data model."""

    id: str = Field(..., description="Run ID")
    scenario_id: str = Field(..., description="Parent scenario ID")
    status: ScenarioRunStatus = Field(..., description="Run status")
    priority: ScenarioRunPriority = Field(..., description="Run priority")
    run_key: Optional[str] = Field(None, description="Deterministic run key")
    input_hash: Optional[str] = Field(None, description="Fingerprint of scenario inputs")
    started_at: Optional[datetime] = Field(None, description="Start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    duration_seconds: Optional[float] = Field(None, description="Duration in seconds", ge=0)
    error_message: Optional[str] = Field(None, description="Error message")
    retry_count: int = Field(default=0, description="Number of retries attempted", ge=0)
    max_retries: int = Field(default=3, description="Maximum number of retries", ge=0)
    progress_percent: Optional[float] = Field(None, description="Progress percentage", ge=0, le=100)
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Run parameters"
    )
    environment: Dict[str, str] = Field(
        default_factory=dict,
        description="Run environment"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    queued_at: Optional[datetime] = Field(None, description="When run was queued")
    cancelled_at: Optional[datetime] = Field(None, description="When run was cancelled")


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


class ScenarioOutputFilter(AurumBaseModel):
    """Filter model for scenario outputs."""

    start_time: Optional[datetime] = Field(None, description="Start time filter (ISO 8601)")
    end_time: Optional[datetime] = Field(None, description="End time filter (ISO 8601)")
    metric_name: Optional[str] = Field(None, description="Filter by metric name")
    min_value: Optional[float] = Field(None, description="Minimum value filter", ge=0)
    max_value: Optional[float] = Field(None, description="Maximum value filter", ge=0)
    tags: Optional[Dict[str, str]] = Field(None, description="Filter by tags")


class ScenarioOutputListResponse(AurumBaseModel):
    """Response model for listing scenario outputs with filtering."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List[ScenarioOutputPoint] = Field(..., description="Output data points")
    filter: Optional[ScenarioOutputFilter] = Field(None, description="Applied filters")


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


class BulkScenarioRunRequest(AurumBaseModel):
    """Request model for bulk scenario run submission."""

    scenario_id: Optional[str] = Field(None, description="Parent scenario ID")
    runs: List["BulkScenarioRunItem"] = Field(..., min_items=1, max_items=100, description="List of runs to create")
    idempotency_key: Optional[str] = Field(None, max_length=255, description="Idempotency key for deduplication")


class BulkScenarioRunItem(AurumBaseModel):
    """Individual run item in bulk request."""

    idempotency_key: Optional[str] = Field(None, max_length=255, description="Unique key for this run")
    priority: ScenarioRunPriority = Field(default=ScenarioRunPriority.NORMAL, description="Run priority")
    timeout_minutes: int = Field(default=60, description="Timeout in minutes", ge=1, le=1440)
    max_memory_mb: int = Field(default=1024, description="Max memory in MB", ge=256, le=16384)
    environment: Dict[str, str] = Field(default_factory=dict, description="Environment variables")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Runtime parameters")

    @field_validator("idempotency_key")
    def validate_idempotency_key(cls, v):
        """Validate idempotency key format."""
        if v is not None:
            if not v or not v.strip():
                raise ValueError("idempotency_key cannot be empty if provided")
            if len(v) > 255:
                raise ValueError("idempotency_key cannot exceed 255 characters")
            # Allow alphanumeric, underscore, hyphen, dot, and colon
            if not all(c.isalnum() or c in "_-.:/" for c in v):
                raise ValueError("idempotency_key can only contain alphanumeric characters, underscore, hyphen, dot, and colon")
        return v.strip() if v else v


class BulkScenarioRunResponse(AurumBaseModel):
    """Response model for bulk scenario run submission."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List["BulkScenarioRunResult"] = Field(..., description="Results for each run")
    duplicates: List["BulkScenarioRunDuplicate"] = Field(default_factory=list, description="Duplicate runs detected")


class BulkScenarioRunResult(AurumBaseModel):
    """Result for an individual run in bulk submission."""

    index: int = Field(..., description="Index of the run in the request")
    idempotency_key: Optional[str] = Field(None, description="Idempotency key used")
    run_id: Optional[str] = Field(None, description="Created run ID")
    status: str = Field(..., description="Status of the run creation")
    error: Optional[str] = Field(None, description="Error message if creation failed")


class BulkScenarioRunDuplicate(AurumBaseModel):
    """Information about duplicate runs detected."""

    index: int = Field(..., description="Index of the duplicate run in the request")
    idempotency_key: Optional[str] = Field(None, description="Idempotency key of the duplicate")
    existing_run_id: str = Field(..., description="ID of existing run")
    existing_status: str = Field(..., description="Status of existing run")
    created_at: datetime = Field(..., description="When the existing run was created")


class ScenarioRunBulkResponse(AurumBaseModel):
    """Response model for bulk scenario run operations."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: List[ScenarioRunData] = Field(..., description="Created or existing runs")


# Schema v2 models

class CurveFamilyType(str, Enum):
    """Types of curve families for scenario modeling."""
    DEMAND = "demand"
    SUPPLY = "supply"
    PRICE = "price"
    RENEWABLE = "renewable"
    LOAD = "load"
    WEATHER = "weather"
    ECONOMIC = "economic"


class ConstraintType(str, Enum):
    """Types of constraints for scenario validation."""
    BUDGET = "budget"
    CAPACITY = "capacity"
    POLICY = "policy"
    TECHNICAL = "technical"
    REGULATORY = "regulatory"


class ConstraintSeverity(str, Enum):
    """Severity levels for constraint violations."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ConstraintOperator(str, Enum):
    """Operators for constraint validation."""
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"


class CurveFamily(AurumBaseModel):
    """Curve family configuration for scenario modeling."""

    id: str = Field(..., description="Curve family ID")
    name: str = Field(..., description="Curve family name")
    description: Optional[str] = Field(None, description="Curve family description")
    family_type: CurveFamilyType = Field(..., description="Type of curve family")
    curve_keys: List[str] = Field(default_factory=list, description="Curve keys in this family")
    default_parameters: Dict[str, Any] = Field(default_factory=dict, description="Default parameters for the family")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules for the family")
    is_active: bool = Field(default=True, description="Whether the curve family is active")
    created_by: str = Field(..., description="User who created the curve family")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_by: Optional[str] = Field(None, description="User who last updated the curve family")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")


class ScenarioConstraint(AurumBaseModel):
    """Constraint definition for scenario validation."""

    id: str = Field(..., description="Constraint ID")
    scenario_id: str = Field(..., description="Parent scenario ID")
    constraint_type: ConstraintType = Field(..., description="Type of constraint")
    constraint_key: str = Field(..., description="Key in scenario parameters to validate")
    constraint_value: Any = Field(..., description="Expected value or range for constraint")
    operator: ConstraintOperator = Field(..., description="Validation operator")
    severity: ConstraintSeverity = Field(default=ConstraintSeverity.WARNING, description="Severity of constraint violation")
    is_enforced: bool = Field(default=False, description="Whether constraint is enforced")
    violation_message: Optional[str] = Field(None, description="Message to show on violation")
    created_by: str = Field(..., description="User who created the constraint")
    created_at: datetime = Field(..., description="Creation timestamp")


class ScenarioProvenance(AurumBaseModel):
    """Provenance tracking for scenario data sources."""

    id: str = Field(..., description="Provenance ID")
    scenario_id: str = Field(..., description="Parent scenario ID")
    data_source: str = Field(..., description="Source of the data")
    source_version: Optional[str] = Field(None, description="Version of the data source")
    data_timestamp: datetime = Field(..., description="Timestamp when data was sourced")
    transformation_hash: str = Field(..., description="Hash of the transformation applied")
    input_parameters: Dict[str, Any] = Field(default_factory=dict, description="Parameters used in transformation")
    quality_metrics: Dict[str, Any] = Field(default_factory=dict, description="Quality metrics of the data")
    lineage_metadata: Dict[str, Any] = Field(default_factory=dict, description="Data lineage information")
    created_at: datetime = Field(..., description="Creation timestamp")


class ScenarioCurveFamily(AurumBaseModel):
    """Association between scenario and curve family with configuration."""

    scenario_id: str = Field(..., description="Scenario ID")
    curve_family_id: str = Field(..., description="Curve family ID")
    weight: float = Field(default=1.0, ge=0.0, le=1.0, description="Weight of this family in the scenario")
    is_primary: bool = Field(default=False, description="Whether this is the primary curve family")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Family-specific parameters")
    created_at: datetime = Field(..., description="Creation timestamp")


class CreateCurveFamilyRequest(AurumBaseModel):
    """Request model for creating a curve family."""

    name: str = Field(..., description="Curve family name")
    description: Optional[str] = Field(None, description="Curve family description")
    family_type: CurveFamilyType = Field(..., description="Type of curve family")
    curve_keys: List[str] = Field(default_factory=list, description="Curve keys in this family")
    default_parameters: Dict[str, Any] = Field(default_factory=dict, description="Default parameters")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules")


class CreateScenarioConstraintRequest(AurumBaseModel):
    """Request model for creating a scenario constraint."""

    constraint_type: ConstraintType = Field(..., description="Type of constraint")
    constraint_key: str = Field(..., description="Key in scenario parameters to validate")
    constraint_value: Any = Field(..., description="Expected value or range")
    operator: ConstraintOperator = Field(..., description="Validation operator")
    severity: ConstraintSeverity = Field(default=ConstraintSeverity.WARNING, description="Severity level")
    is_enforced: bool = Field(default=False, description="Whether to enforce the constraint")
    violation_message: Optional[str] = Field(None, description="Violation message")


class CreateScenarioProvenanceRequest(AurumBaseModel):
    """Request model for creating scenario provenance."""

    data_source: str = Field(..., description="Source of the data")
    source_version: Optional[str] = Field(None, description="Version of data source")
    data_timestamp: datetime = Field(..., description="When data was sourced")
    transformation_hash: str = Field(..., description="Hash of transformation")
    input_parameters: Dict[str, Any] = Field(default_factory=dict, description="Transformation parameters")
    quality_metrics: Dict[str, Any] = Field(default_factory=dict, description="Quality metrics")
    lineage_metadata: Dict[str, Any] = Field(default_factory=dict, description="Lineage metadata")


class ScenarioSchemaV2(AurumBaseModel):
    """Extended scenario model with schema v2 features."""

    # Inherit from ScenarioData
    id: str = Field(..., description="Scenario ID")
    tenant_id: str = Field(..., description="Tenant identifier")
    name: str = Field(..., description="Scenario name")
    description: Optional[str] = Field(None, description="Scenario description")
    status: ScenarioStatus = Field(..., description="Scenario status")
    unique_key: str = Field(..., description="Deterministic scenario key")
    assumptions: List[Dict[str, Any]] = Field(default_factory=list, description="Scenario assumptions")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Scenario parameters")
    tags: List[str] = Field(default_factory=list, description="Scenario tags")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="Creator identifier")
    version: int = Field(default=1, description="Scenario version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Scenario metadata")

    # Schema v2 additions
    schema_version: int = Field(default=2, description="Schema version")
    curve_families: List[str] = Field(default_factory=list, description="Associated curve family names")
    constraints: List[ScenarioConstraint] = Field(default_factory=list, description="Scenario constraints")
    provenance_enabled: bool = Field(default=False, description="Whether provenance tracking is enabled")


# Legacy models for backward compatibility
ScenarioRequest = CreateScenarioRequest
Scenario = ScenarioData
ScenarioList = ScenarioListResponse
ScenarioRun = ScenarioRunData
ScenarioRunList = ScenarioRunListResponse
ScenarioOutput = ScenarioOutputResponse


# Resolve forward references in Pydantic models
BulkScenarioRunRequest.model_rebuild()
BulkScenarioRunItem.model_rebuild()
BulkScenarioRunResponse.model_rebuild()
BulkScenarioRunResult.model_rebuild()
BulkScenarioRunDuplicate.model_rebuild()
ScenarioRunBulkResponse.model_rebuild()
