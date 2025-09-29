"""Legacy scenario model shims for import compatibility in tests."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class ScenarioRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ScenarioRunData:
    scenario_id: str
    run_id: str
    status: ScenarioRunStatus = ScenarioRunStatus.PENDING
    params: Dict[str, Any] | None = None
    result: Dict[str, Any] | None = None


