from __future__ import annotations

"""Scenario domain service façade."""

from typing import Any, Dict


class ScenarioService:
    @staticmethod
    def create_scenario(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        from aurum.api import service as legacy

        return legacy.create_scenario(*_args, **_kwargs)  # type: ignore[attr-defined]

