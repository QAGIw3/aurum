"""Typed schema definitions for workflow configuration files.

These models are used to validate JSON/YAML workflow definitions before they
are materialised into Airflow DAGs. Validation happens both via the CLI and at
scheduler import time to fail fast when configuration drifts.
"""

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class MappingConfig(BaseModel):
    """Configuration for dynamic task mapping expansion."""

    model_config = ConfigDict(extra="ignore")

    kwargs: Optional[List[Dict[str, Any]]] = None
    expand: Optional[Dict[str, Any]] = None
    param: Optional[str] = None
    items: Optional[List[Any]] = None

    @model_validator(mode="after")
    def validate_mapping(self) -> "MappingConfig":
        if not any([self.kwargs, self.expand, (self.param and self.items is not None)]):
            raise ValueError(
                "mapping requires one of kwargs, expand, or param+items"
            )
        return self


class TaskConfig(BaseModel):
    """Single task definition within a workflow."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    task_id: str = Field(
        ...,
        validation_alias=AliasChoices("task_id", "id"),
        serialization_alias="task_id",
    )
    type: str = Field(default="empty", min_length=1)
    params: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("depends_on", "upstream"),
    )
    wait_for_datasets: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("wait_for_datasets", "wait_for", "datasets_wait"),
        serialization_alias="wait_for_datasets",
    )
    trigger_rule: Optional[str] = None
    condition: Optional[str] = None
    pool: Optional[str] = None
    retries: Optional[int] = Field(default=None, ge=0)
    retry_delay_minutes: Optional[int] = Field(default=None, ge=0)
    task_concurrency: Optional[int] = Field(default=None, ge=1)
    execution_timeout_minutes: Optional[int] = Field(default=None, ge=1)
    sla_minutes: Optional[int] = Field(default=None, ge=1)
    doc_md: Optional[str] = None
    inlets: Optional[List[str]] = None
    outlets: Optional[List[str]] = None
    on_failure: Optional[Dict[str, Any]] = None
    task_kwargs: Dict[str, Any] = Field(default_factory=dict)
    mapping: Optional[MappingConfig] = Field(
        default=None,
        validation_alias=AliasChoices("mapping", "map"),
        serialization_alias="map",
    )

    @field_validator("task_id", mode="before")
    @classmethod
    def _trim_task_id(cls, value: str) -> str:
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError("task_id cannot be empty")
        return value

    @field_validator("depends_on")
    @classmethod
    def _dedupe_dependencies(cls, value: List[str]) -> List[str]:
        seen: Dict[str, None] = {}
        for item in value:
            key = str(item).strip()
            if key and key not in seen:
                seen[key] = None
        return list(seen.keys())

    @field_validator("wait_for_datasets")
    @classmethod
    def _normalise_datasets(cls, value: List[str]) -> List[str]:
        result: Dict[str, None] = {}
        for uri in value:
            key = str(uri).strip()
            if key and key not in result:
                result[key] = None
        return list(result.keys())

    @field_validator("params", "task_kwargs")
    @classmethod
    def _default_dict(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        return value or {}

    @model_validator(mode="after")
    def _extract_mapping(self) -> "TaskConfig":
        if self.mapping is None and isinstance(self.params, dict):
            raw = self.params.pop("map", None)
            if raw is not None:
                self.mapping = MappingConfig.model_validate(raw)
        return self


class GroupConfig(BaseModel):
    """TaskGroup declaration within the workflow."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    group_id: str = Field(
        ...,
        validation_alias=AliasChoices("group_id", "id"),
        serialization_alias="group_id",
    )
    tooltip: Optional[str] = None
    default_task_kwargs: Dict[str, Any] = Field(default_factory=dict)
    tasks: List[TaskConfig] = Field(default_factory=list)
    template: Optional[str] = None
    template_params: Dict[str, Any] = Field(default_factory=dict, alias="params")

    @field_validator("group_id", mode="before")
    @classmethod
    def _trim_group_id(cls, value: str) -> str:
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError("group_id cannot be empty")
        return value


class WorkflowConfig(BaseModel):
    """Top-level workflow definition used to build DAGs."""

    model_config = ConfigDict(extra="ignore")

    dag_id: str = Field(..., min_length=1)
    description: str = Field(default="")
    schedule: Optional[str] = Field(default=None)
    start_date: datetime
    catchup: bool = False
    max_active_runs: int = Field(default=1, ge=1)
    tags: List[str] = Field(default_factory=lambda: ["aurum", "dynamic"])
    default_args: Dict[str, Any] = Field(default_factory=dict)
    default_task_kwargs: Dict[str, Any] = Field(default_factory=dict)
    tasks: List[TaskConfig] = Field(default_factory=list)
    groups: List[GroupConfig] = Field(default_factory=list)
    feature_flags: Dict[str, Any] = Field(default_factory=dict)
    user_defined_macros: Dict[str, Any] = Field(default_factory=dict)
    doc_md: Optional[str] = None
    datasets: List[Any] = Field(default_factory=list)

    @field_validator("dag_id", mode="before")
    @classmethod
    def _trim_dag_id(cls, value: str) -> str:
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError("dag_id cannot be empty")
        return value

    @field_validator("tags")
    @classmethod
    def _dedupe_tags(cls, value: List[str]) -> List[str]:
        seen: Dict[str, None] = {}
        for item in value:
            key = item.strip()
            if key and key not in seen:
                seen[key] = None
        return list(seen.keys())

    @field_validator("start_date", mode="before")
    @classmethod
    def _parse_start_date(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"Invalid start_date format: {value}") from exc
        raise ValueError("start_date must be a datetime, date, or ISO string")

    @model_validator(mode="after")
    def _ensure_unique_tasks(self) -> "WorkflowConfig":
        seen: Dict[str, str] = {}
        for task in self.tasks:
            if task.task_id in seen:
                raise ValueError(f"duplicate task id '{task.task_id}' declared")
            seen[task.task_id] = "root"
        for group in self.groups:
            if group.group_id in seen:
                raise ValueError(f"group id '{group.group_id}' conflicts with task id")
            for task in group.tasks:
                if task.task_id in seen:
                    raise ValueError(
                        f"duplicate task id '{task.task_id}' across group {group.group_id}"
                    )
                seen[task.task_id] = group.group_id
        return self


def load_workflow_config(raw: Dict[str, Any]) -> WorkflowConfig:
    """Validate raw workflow data and return a typed model."""

    try:
        return WorkflowConfig.model_validate(raw)
    except ValidationError as exc:  # pragma: no cover - forwarded to caller
        raise exc
