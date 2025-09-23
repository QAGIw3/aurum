"""Scenario management functionality for the Aurum API."""

from .scenario_models import (
    CreateScenarioRequest,
    ScenarioResponse,
    ScenarioData,
    ScenarioListResponse,
    ScenarioRunOptions,
    ScenarioRunData,
    ScenarioRunResponse,
    ScenarioRunListResponse,
    ScenarioOutputPoint,
    ScenarioOutputResponse,
    ScenarioMetricLatest,
    ScenarioMetricLatestResponse,
    ScenarioOutputListResponse,
    ScenarioStatus,
    ScenarioRunStatus,
    ScenarioRunPriority,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    BulkScenarioRunDuplicate,
    ScenarioRunBulkResponse,
)

from .scenario_service import STORE as ScenarioStore
from .scenarios import router as scenarios_router

__all__ = [
    # Models
    "CreateScenarioRequest",
    "ScenarioResponse",
    "ScenarioData",
    "ScenarioListResponse",
    "ScenarioRunOptions",
    "ScenarioRunData",
    "ScenarioRunResponse",
    "ScenarioRunListResponse",
    "ScenarioOutputPoint",
    "ScenarioOutputResponse",
    "ScenarioMetricLatest",
    "ScenarioMetricLatestResponse",
    "ScenarioOutputListResponse",
    "ScenarioStatus",
    "ScenarioRunStatus",
    "ScenarioRunPriority",
    "BulkScenarioRunRequest",
    "BulkScenarioRunResponse",
    "BulkScenarioRunResult",
    "BulkScenarioRunDuplicate",
    "ScenarioRunBulkResponse",
    # Services
    "ScenarioStore",
    "scenarios_router",
]
