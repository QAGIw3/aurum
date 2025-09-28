"""Stress testing utilities built on top of VaREngine.

Provides reusable scenario definitions and an execution harness producing
scenario PnL impacts and VaR metrics under altered vol/correlation regimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from pydantic import BaseModel, Field

import numpy as np

from .var_engine import (
    PortfolioInput,
    VaRConfig,
    VaRMethod,
    VaRResult,
    VaREngine,
)


class StressScenario(BaseModel):
    scenario_id: str
    name: str
    description: Optional[str] = None
    # Factor shocks are direct additive shocks to factor values (e.g., return or rate deltas)
    factor_shocks: Dict[str, float] = Field(default_factory=dict)
    # Volatility multipliers per factor (e.g., {"oil": 1.5} increases var by 50%)
    volatility_multipliers: Dict[str, float] = Field(default_factory=dict)
    # Correlation shift to apply to off-diagonal entries (-1..+1); small values sensible
    correlation_shift: float = 0.0
    horizon_days: int = 1


class StressTestConfig(BaseModel):
    var_config: VaRConfig = Field(default_factory=lambda: VaRConfig(method=VaRMethod.MONTE_CARLO))
    include_baseline: bool = True


class StressTestResult(BaseModel):
    scenario_id: str
    name: str
    pnl_impact: float
    var_before: Optional[float] = None
    var_after: Optional[float] = None
    cvar_after: Optional[float] = None
    var_result: Optional[VaRResult] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StressTester:
    def __init__(self, engine: Optional[VaREngine] = None) -> None:
        self._engine = engine or VaREngine()

    @staticmethod
    def _adjust_cov(
        cov: np.ndarray,
        factor_names: Sequence[str],
        volatility_multipliers: Mapping[str, float],
        corr_shift: float,
    ) -> np.ndarray:
        if cov.size == 0:
            return cov
        cov = np.array(cov, dtype=float)
        cov = np.nan_to_num(cov, nan=0.0, posinf=0.0, neginf=0.0)
        nf = cov.shape[0]
        # Scale variances via multipliers
        scales = np.ones(nf)
        for i, f in enumerate(factor_names):
            if f in volatility_multipliers:
                mult = max(float(volatility_multipliers[f]), 0.0)
                scales[i] = mult
        # Apply scaling: Sigma' = D * Sigma * D
        D = np.diag(scales)
        cov = D @ cov @ D
        if corr_shift != 0.0 and nf > 1:
            diag = np.sqrt(np.clip(np.diag(cov), 1e-12, None))
            corr = cov / np.outer(diag, diag)
            # shift off-diagonals by corr_shift (clamp [-0.99, 0.99])
            i_upper = np.triu_indices(nf, k=1)
            corr[i_upper] = np.clip(corr[i_upper] + corr_shift, -0.99, 0.99)
            corr[(i_upper[1], i_upper[0])] = corr[i_upper]  # mirror
            cov = corr * np.outer(diag, diag)
        return cov

    def run(
        self,
        portfolio: PortfolioInput,
        scenarios: Iterable[StressScenario],
        *,
        factor_returns: Optional[Dict[str, np.ndarray]] = None,
        base_var: Optional[VaRResult] = None,
        config: Optional[StressTestConfig] = None,
    ) -> List[StressTestResult]:
        cfg = config or StressTestConfig()
        base = base_var
        if cfg.include_baseline and base is None:
            base = self._engine.calculate_var(portfolio, cfg.var_config, factor_returns=factor_returns)

        # Build a base covariance if we can derive from provided factor returns
        factor_names = sorted({f for p in portfolio.positions for f in p.risk_factors})
        if factor_returns and factor_names:
            cols = [np.asarray(factor_returns.get(f, np.zeros(0)), dtype=float) for f in factor_names]
            min_len = min((len(c) for c in cols if len(c) > 0), default=0)
            arr = np.vstack([c[-min_len:] for c in cols]).T if min_len > 0 else np.zeros((0, len(cols)))
            base_cov = np.cov(arr, rowvar=False, ddof=0) if arr.size else np.eye(len(factor_names) or 1)
        else:
            base_cov = np.eye(len(factor_names) or 1)

        results: List[StressTestResult] = []
        for scen in scenarios:
            pnl = self._engine.scenario_pnl(portfolio, scen.factor_shocks)

            # Adjust covariance and rerun VaR after scenario to reflect new regime
            adj_cov = self._adjust_cov(base_cov, factor_names, scen.volatility_multipliers, scen.correlation_shift)
            # Reuse MONTE_CARLO regardless of var_config method to reflect simulated tails
            scen_cfg = VaRConfig(
                method=VaRMethod.MONTE_CARLO,
                confidence_level=cfg.var_config.confidence_level,
                horizon_days=max(cfg.var_config.horizon_days, scen.horizon_days),
                num_simulations=max(cfg.var_config.num_simulations, 5000),
                covariance_method=cfg.var_config.covariance_method,
                random_seed=cfg.var_config.random_seed,
            )

            # We feed the adjusted covariance by injecting synthetic returns with that covariance
            # to VaREngine's Monte Carlo path by directly calling the protected method would
            # violate encapsulation; instead, we call calculate_var with a hint via factor_returns
            # and rely on VaREngine to estimate covariance. To honor adj_cov, we simulate a small
            # synthetic matrix whose sample cov approximates adj_cov.
            synthetic = np.random.default_rng(1234).multivariate_normal(
                mean=np.zeros(adj_cov.shape[0]), cov=adj_cov + np.eye(adj_cov.shape[0]) * 1e-9, size=5000, method="eigh"
            )
            fr: Dict[str, np.ndarray] = {}
            for i, f in enumerate(factor_names):
                fr[f] = synthetic[:, i]

            var_after = self._engine.calculate_var(portfolio, scen_cfg, factor_returns=fr)
            results.append(
                StressTestResult(
                    scenario_id=scen.scenario_id,
                    name=scen.name,
                    pnl_impact=pnl,
                    var_before=(base.var if base else None),
                    var_after=var_after.var,
                    cvar_after=var_after.cvar,
                    var_result=var_after,
                    metadata={
                        "factor_shocks": dict(scen.factor_shocks),
                        "volatility_multipliers": dict(scen.volatility_multipliers),
                        "correlation_shift": scen.correlation_shift,
                    },
                )
            )

        return results


def default_scenarios() -> List[StressScenario]:
    return [
        StressScenario(
            scenario_id="market_crash",
            name="Market Crash",
            description="Broad selloff: prices down, vol up, correlation up",
            factor_shocks={"market": -0.08},
            volatility_multipliers={"market": 2.0},
            correlation_shift=0.15,
            horizon_days=1,
        ),
        StressScenario(
            scenario_id="rate_spike",
            name="Rate Spike",
            description="Interest rates +150 bps",
            factor_shocks={"rate": 0.015},
            volatility_multipliers={"rate": 1.5},
            correlation_shift=0.05,
            horizon_days=1,
        ),
        StressScenario(
            scenario_id="weather_extreme",
            name="Extreme Weather",
            description="Severe weather impacts demand/supply",
            factor_shocks={"weather": 2.5, "demand": 0.10},
            volatility_multipliers={"weather": 1.8, "demand": 1.3},
            correlation_shift=0.10,
            horizon_days=3,
        ),
    ]

