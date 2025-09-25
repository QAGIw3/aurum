"""Scenario management functionality for the Aurum API.

This package primarily exposes the shared Pydantic models used by both v1 and
v2 scenario endpoints. Historically it also re-exported the long-lived v1
router and store singletons at import time. Importing those large modules
pulls in a wide range of optional dependencies, which breaks lightweight
contexts (unit tests, schema generation, etc.).

To keep the public surface area stable while avoiding heavy side effects, the
router and store exports are now resolved lazily via ``__getattr__``. Existing
``from aurum.api.scenarios import scenarios_router`` style imports continue to
work, but the expensive modules load only when explicitly requested.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .scenario_models import (
    BulkScenarioRunDuplicate,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    CreateScenarioRequest,
    ScenarioData,
    ScenarioListResponse,
    ScenarioMetricLatest,
    ScenarioMetricLatestResponse,
    ScenarioOutputListResponse,
    ScenarioOutputPoint,
    ScenarioOutputResponse,
    ScenarioResponse,
    ScenarioRunBulkResponse,
    ScenarioRunData,
    ScenarioRunListResponse,
    ScenarioRunOptions,
    ScenarioRunPriority,
    ScenarioRunResponse,
    ScenarioRunStatus,
    ScenarioStatus,
)

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from .scenario_service import STORE as _ScenarioStore
    from .scenarios import router as _Router

_MODEL_EXPORTS = [
    "BulkScenarioRunDuplicate",
    "BulkScenarioRunRequest",
    "BulkScenarioRunResponse",
    "BulkScenarioRunResult",
    "CreateScenarioRequest",
    "ScenarioData",
    "ScenarioListResponse",
    "ScenarioMetricLatest",
    "ScenarioMetricLatestResponse",
    "ScenarioOutputListResponse",
    "ScenarioOutputPoint",
    "ScenarioOutputResponse",
    "ScenarioResponse",
    "ScenarioRunBulkResponse",
    "ScenarioRunData",
    "ScenarioRunListResponse",
    "ScenarioRunOptions",
    "ScenarioRunPriority",
    "ScenarioRunResponse",
    "ScenarioRunStatus",
    "ScenarioStatus",
]

__all__ = sorted(_MODEL_EXPORTS + ["ScenarioStore", "scenarios_router"])


def __getattr__(name: str) -> Any:
    if name == "ScenarioStore":
        from .scenario_service import STORE

        return STORE
    if name == "scenarios_router":
        from .scenarios import router

        return router
    raise AttributeError(f"module 'aurum.api.scenarios' has no attribute {name!r}")


def __dir__() -> list[str]:
    return __all__
