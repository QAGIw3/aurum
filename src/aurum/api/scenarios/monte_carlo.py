"""Minimal Monte Carlo engine shim for tests and legacy imports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class MonteCarloConfig:
    num_simulations: int = 1000
    random_seed: int | None = None
    confidence_level: float = 0.95


class _DummyMonteCarloEngine:
    async def run_multi_model_simulation(self, model_configs: Dict[str, Dict[str, Any]], global_config: MonteCarloConfig, seed: int | None = None) -> Dict[str, Any]:
        # Return stable fake results structure for tests
        result = {}
        for name in model_configs.keys():
            result[name] = type("R", (), {"mean": 0.0, "std_dev": 0.0, "confidence_interval": (0.0, 0.0)})
        return result


def get_monte_carlo_engine() -> _DummyMonteCarloEngine:
    return _DummyMonteCarloEngine()


