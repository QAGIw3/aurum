"""Risk Engine Service with Monte Carlo + VaR/CVaR calculations.

This service provides:
- Monte Carlo simulation for portfolio risk assessment
- Value at Risk (VaR) and Conditional Value at Risk (CVaR) calculations
- Portfolio aggregation and correlation modeling
- Risk distribution configuration and scenario analysis
- Integration with forecasting and carbon pricing
- Real-time risk monitoring and alerting
"""

from __future__ import annotations

import asyncio
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4
from enum import Enum

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..dao.experimental import TrinoDAO
from ...scenarios.monte_carlo import MonteCarloConfig, SimulationResult, BaseMonteCarloModel


class RiskDistributionType(str, Enum):
    """Risk distribution types for modeling."""
    NORMAL = "normal"
    LOGNORMAL = "lognormal"
    T_STUDENT = "t_student"
    GEV = "gev"
    GARCH = "garch"
    COPULA = "copula"


class CorrelationModel(str, Enum):
    """Correlation modeling approaches."""
    PEARSON = "pearson"
    SPEARMAN = "spearman"
    KENDALL = "kendall"
    COPULA = "copula"
    PCA = "pca"


class RiskMetricType(str, Enum):
    """Risk metrics to calculate."""
    VAR = "var"  # Value at Risk
    CVAR = "cvar"  # Conditional Value at Risk
    ES = "es"  # Expected Shortfall (same as CVaR)
    VOLATILITY = "volatility"
    DRAWDOWN = "drawdown"
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    BETA = "beta"
    ALPHA = "alpha"


class RiskDistributionConfig(BaseModel):
    """Configuration for risk distribution modeling."""

    distribution_type: RiskDistributionType
    parameters: Dict[str, float]  # Distribution parameters (mu, sigma, df, etc.)
    correlation_model: CorrelationModel = CorrelationModel.PEARSON
    correlation_matrix: Optional[List[List[float]]] = None
    volatility_regime: str = "normal"  # "normal", "high", "crisis"
    fat_tail_adjustment: bool = True
    seasonality_enabled: bool = True


class PortfolioPosition(BaseModel):
    """Portfolio position for risk analysis."""

    asset_id: str
    position_type: str  # "long", "short", "hedge"
    notional_value: float
    currency: str = "USD"
    maturity_date: Optional[datetime] = None
    risk_factors: Dict[str, float]  # Risk factor sensitivities
    metadata: Dict[str, Any] = field(default_factory=dict)


class RiskScenario(BaseModel):
    """Risk scenario definition."""

    scenario_id: str
    scenario_name: str
    description: str
    probability: float  # Scenario probability (0-1)
    risk_factors: Dict[str, float]  # Risk factor shocks
    correlation_shocks: Dict[str, float]  # Correlation changes
    duration_days: int = 1
    market_impact: str = "moderate"  # "low", "moderate", "high", "extreme"


class RiskCalculationResult(BaseModel):
    """Result of risk calculation."""

    calculation_id: str
    portfolio_id: str
    calculation_date: datetime
    risk_metrics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    scenario_results: Dict[str, Dict[str, float]]
    methodology: str
    parameters: Dict[str, Any]
    execution_time: float
    warnings: List[str] = field(default_factory=list)


class PortfolioAggregation(BaseModel):
    """Portfolio-level risk aggregation."""

    portfolio_id: str
    aggregation_date: datetime
    total_var: float
    total_cvar: float
    total_volatility: float
    diversification_benefit: float
    concentration_risk: float
    liquidity_risk: float
    counterparty_risk: float
    asset_breakdown: Dict[str, Dict[str, float]]
    risk_attribution: Dict[str, float]


@dataclass
class PortfolioSimulationCache:
    """Cached Monte Carlo outputs for downstream analytics."""

    portfolio_returns: np.ndarray
    factor_names: List[str]
    factor_draws: np.ndarray
    correlation_matrix: np.ndarray
    position_names: List[str]
    position_pnl: np.ndarray
    exposures: np.ndarray
    metadata: Dict[str, Any] = field(default_factory=dict)


class RiskEngineService:
    """Comprehensive Risk Engine with Monte Carlo + VaR/CVaR."""

    def __init__(self):
        """Initialize risk engine service."""
        try:
            self.dao = TrinoDAO()
        except TypeError:
            # Base DAO is abstract in lightweight contexts; defer binding until runtime integration.
            self.dao = None  # type: ignore[assignment]
        cache_manager = None
        try:
            cache_manager = get_unified_cache_manager()
        except Exception:
            cache_manager = None
        self.cache_manager = cache_manager

        telemetry = None
        try:
            telemetry = get_telemetry_facade()
        except Exception:
            telemetry = None

        if telemetry is None:
            telemetry = SimpleNamespace(info=lambda *a, **k: None, error=lambda *a, **k: None)
        self.telemetry = telemetry

        # Risk modeling state
        self._portfolio_positions: Dict[str, List[PortfolioPosition]] = defaultdict(list)
        self._risk_distributions: Dict[str, RiskDistributionConfig] = {}
        self._correlation_matrices: Dict[str, np.ndarray] = {}
        self._monte_carlo_models: Dict[str, BaseMonteCarloModel] = {}
        self._simulation_cache: Dict[str, PortfolioSimulationCache] = {}

        # Monte Carlo configuration + RNG state
        self._default_monte_carlo_config = MonteCarloConfig()
        self._rng = np.random.default_rng()
        self._rng_lock = asyncio.Lock()

        # Initialize default risk distributions
        self._initialize_default_distributions()

    def _initialize_default_distributions(self) -> None:
        """Initialize default risk distribution configurations."""
        # Energy price risk (lognormal distribution)
        self._risk_distributions["energy_price"] = RiskDistributionConfig(
            distribution_type=RiskDistributionType.LOGNORMAL,
            parameters={"mu": 4.0, "sigma": 0.3},  # Log space parameters
            correlation_model=CorrelationModel.PEARSON,
            volatility_regime="normal"
        )

        # Demand risk (normal distribution)
        self._risk_distributions["demand"] = RiskDistributionConfig(
            distribution_type=RiskDistributionType.NORMAL,
            parameters={"mu": 0.0, "sigma": 0.1},
            correlation_model=CorrelationModel.PEARSON,
            seasonality_enabled=True
        )

        # Weather risk (t-student for fat tails)
        self._risk_distributions["weather"] = RiskDistributionConfig(
            distribution_type=RiskDistributionType.T_STUDENT,
            parameters={"df": 5, "loc": 0.0, "scale": 1.0},
            fat_tail_adjustment=True
        )

        # Generic fallback risk configuration
        self._risk_distributions["default"] = RiskDistributionConfig(
            distribution_type=RiskDistributionType.NORMAL,
            parameters={"mu": 0.0, "sigma": 0.1},
            correlation_model=CorrelationModel.PEARSON,
            volatility_regime="normal",
            fat_tail_adjustment=False,
            seasonality_enabled=False
        )

    def set_random_seed(self, seed: int) -> None:
        """Reset RNG seed for reproducible simulations."""
        self._rng = np.random.default_rng(seed)

    async def add_portfolio_position(self, portfolio_id: str, position: PortfolioPosition) -> None:
        """Add position to portfolio for risk analysis."""
        self._portfolio_positions[portfolio_id].append(position)
        self.telemetry.info("Portfolio position added", portfolio_id=portfolio_id, asset_id=position.asset_id)

    async def get_portfolio_positions(self, portfolio_id: str) -> List[PortfolioPosition]:
        """Return current positions for a portfolio.

        Exposes a safe accessor for positions managed by the risk engine. The
        returned list is a shallow copy to prevent external mutation of
        internal state.
        """
        positions = self._portfolio_positions.get(portfolio_id, [])
        return list(positions)

    async def set_risk_distribution(self, asset_type: str, config: RiskDistributionConfig) -> None:
        """Set risk distribution for asset type."""
        self._risk_distributions[asset_type] = config

        # Update correlation matrix if provided
        if config.correlation_matrix:
            self._correlation_matrices[asset_type] = np.array(config.correlation_matrix)

        self.telemetry.info("Risk distribution updated", asset_type=asset_type)

    def _get_distribution_config(self, factor_name: str) -> RiskDistributionConfig:
        """Return configured distribution for factor with default fallback."""
        return self._risk_distributions.get(factor_name, self._risk_distributions["default"])

    def _collect_portfolio_factors(
        self,
        positions: List[PortfolioPosition]
    ) -> Tuple[List[str], np.ndarray, np.ndarray, List[str]]:
        """Compile factor exposures and scaling vectors for portfolio positions."""

        factor_names = sorted({factor for position in positions for factor in position.risk_factors})

        if not factor_names:
            raise ValueError("Portfolio has no risk factor exposures configured")

        exposures = np.zeros((len(positions), len(factor_names)))
        position_scalars = np.zeros(len(positions))
        position_names: List[str] = []

        for idx, position in enumerate(positions):
            position_names.append(position.asset_id)
            direction = 1.0
            if position.position_type.lower() == "short":
                direction = -1.0
            elif position.position_type.lower() == "hedge":
                direction = -0.5

            for factor_idx, factor_name in enumerate(factor_names):
                exposures[idx, factor_idx] = float(position.risk_factors.get(factor_name, 0.0))

            position_scalars[idx] = direction * float(position.notional_value)

        return factor_names, exposures, position_scalars, position_names

    def _determine_correlation_model(self, factor_names: List[str]) -> CorrelationModel:
        """Derive dominant correlation model from factor configurations."""

        if not factor_names:
            return CorrelationModel.PEARSON

        priority = [
            CorrelationModel.COPULA,
            CorrelationModel.PCA,
            CorrelationModel.KENDALL,
            CorrelationModel.SPEARMAN,
            CorrelationModel.PEARSON,
        ]

        configured = {self._get_distribution_config(name).correlation_model for name in factor_names}

        for candidate in priority:
            if candidate in configured:
                return candidate

        return CorrelationModel.PEARSON

    def _build_correlation_matrix(
        self,
        exposures: np.ndarray,
        factor_names: List[str],
        correlation_model: CorrelationModel
    ) -> np.ndarray:
        """Construct correlation matrix using requested methodology with caching."""

        n_factors = len(factor_names)

        if n_factors == 1:
            return np.array([[1.0]])

        if exposures.shape[0] < 2:
            return np.eye(n_factors)

        exposures_df = pd.DataFrame(exposures, columns=factor_names)

        if correlation_model == CorrelationModel.SPEARMAN:
            corr_df = exposures_df.rank().corr(method="pearson").fillna(0.0)
        elif correlation_model == CorrelationModel.KENDALL:
            corr_df = exposures_df.corr(method="kendall").fillna(0.0)
        elif correlation_model == CorrelationModel.PCA:
            centered = exposures_df - exposures_df.mean()
            cov_matrix = np.cov(centered.T)
            eigvals, eigvecs = np.linalg.eigh(cov_matrix)
            order = np.argsort(eigvals)[::-1]
            eigvals = eigvals[order]
            eigvecs = eigvecs[:, order]
            total_var = np.sum(np.maximum(eigvals, 0.0))
            captured = 0.0
            reconstructed = np.zeros_like(cov_matrix)

            for val, vec in zip(eigvals, eigvecs.T):
                if val <= 0:
                    continue
                reconstructed += val * np.outer(vec, vec)
                captured += val
                if total_var > 0 and captured / total_var >= 0.9:
                    break

            diag = np.sqrt(np.clip(np.diag(reconstructed), 1e-9, None))
            corr_matrix = reconstructed / np.outer(diag, diag)
            corr_df = pd.DataFrame(corr_matrix, index=factor_names, columns=factor_names)
        elif correlation_model == CorrelationModel.COPULA:
            ranked = exposures_df.rank(pct=True)
            corr_df = ranked.corr(method="spearman").fillna(0.0)
        else:
            corr_df = exposures_df.corr().fillna(0.0)

        corr_matrix = corr_df.to_numpy()
        if not np.all(np.isfinite(corr_matrix)):
            corr_matrix = np.nan_to_num(corr_matrix, nan=0.0)
        corr_matrix = np.clip(corr_matrix, -0.99, 0.99)
        np.fill_diagonal(corr_matrix, 1.0)
        corr_matrix = self._ensure_positive_definite(corr_matrix)

        return corr_matrix

    def _ensure_positive_definite(self, matrix: np.ndarray, epsilon: float = 1e-6) -> np.ndarray:
        """Project correlation matrix to nearest positive definite matrix."""

        sym_matrix = (matrix + matrix.T) / 2
        eigvals, eigvecs = np.linalg.eigh(sym_matrix)
        eigvals_clipped = np.clip(eigvals, epsilon, None)
        adjusted = eigvecs @ np.diag(eigvals_clipped) @ eigvecs.T

        diag = np.sqrt(np.clip(np.diag(adjusted), epsilon, None))
        adjusted = adjusted / np.outer(diag, diag)
        np.fill_diagonal(adjusted, 1.0)

        return adjusted

    def _apply_volatility_regime(self, config: RiskDistributionConfig) -> float:
        """Return volatility multiplier for configured regime."""

        mapping = {
            "normal": 1.0,
            "high": 1.5,
            "crisis": 2.5,
            "stressed": 2.0,
        }
        return mapping.get(config.volatility_regime.lower(), 1.0)

    def _apply_fat_tail_adjustment(self, draws: np.ndarray) -> np.ndarray:
        """Amplify tail events to approximate fat-tailed behavior."""

        tail_scale = 1.0 + 0.15 * np.power(np.abs(draws), 1.2)
        return draws * tail_scale

    def _seasonality_adjustment(self, factor_name: str, time_horizon_days: int) -> float:
        """Simple seasonality drift component based on factor name hints."""

        factor_lower = factor_name.lower()
        now = datetime.utcnow()
        day_of_year = now.timetuple().tm_yday

        seasonal = 0.0
        if "demand" in factor_lower:
            seasonal = 0.01 * math.sin(2 * math.pi * day_of_year / 365)
        elif "weather" in factor_lower:
            seasonal = 0.015 * math.cos(2 * math.pi * day_of_year / 365)
        elif "price" in factor_lower:
            seasonal = 0.005 * math.sin(4 * math.pi * day_of_year / 365)

        return seasonal * max(time_horizon_days, 1)

    def _normal_cdf(self, values: np.ndarray) -> np.ndarray:
        """Vectorised standard normal CDF."""

        return 0.5 * (1.0 + np.erf(values / math.sqrt(2)))

    def _apply_gev_inverse_cdf(self, u: np.ndarray, parameters: Dict[str, float]) -> np.ndarray:
        """Inverse CDF for Generalised Extreme Value distribution."""

        mu = parameters.get("loc", 0.0)
        sigma = max(parameters.get("scale", 1.0), 1e-6)
        shape = parameters.get("shape", 0.0)

        u = np.clip(u, 1e-10, 1 - 1e-10)

        if abs(shape) < 1e-6:
            return mu - sigma * np.log(-np.log(u))

        return mu + sigma / shape * (np.power(-np.log(u), -shape) - 1.0)

    def _transform_draws_for_distribution(
        self,
        base_draws: np.ndarray,
        factor_name: str,
        time_horizon_days: int,
        scenario_shock: Optional[float] = None,
        rng: Optional[np.random.Generator] = None
    ) -> np.ndarray:
        """Map standard normal draws to configured distribution space."""

        config = self._get_distribution_config(factor_name)
        params = config.parameters

        rng = rng or np.random.default_rng()

        horizon = max(time_horizon_days, 1)
        regime_scale = self._apply_volatility_regime(config)
        seasonal = self._seasonality_adjustment(factor_name, horizon) if config.seasonality_enabled else 0.0
        shock_multiplier = 1.0 + scenario_shock if scenario_shock is not None else 1.0

        draws = base_draws.copy()
        if config.fat_tail_adjustment:
            draws = self._apply_fat_tail_adjustment(draws)

        distribution = config.distribution_type

        if distribution == RiskDistributionType.LOGNORMAL:
            log_mu = params.get("mu", 0.0) + seasonal
            log_sigma = params.get("sigma", 0.3) * regime_scale * math.sqrt(horizon)
            log_term = np.clip(log_mu + log_sigma * draws, -10.0, 10.0)
            transformed = (np.exp(log_term) - 1.0) * shock_multiplier
        elif distribution == RiskDistributionType.T_STUDENT:
            df = max(int(params.get("df", 5)), 2)
            loc = (params.get("loc", 0.0) + seasonal) * horizon
            scale = params.get("scale", 1.0) * regime_scale * math.sqrt(horizon)
            chi_samples = rng.chisquare(df, size=draws.shape[0])
            t_draws = draws / np.sqrt(chi_samples / df)
            transformed = (loc + scale * t_draws) * shock_multiplier
        elif distribution == RiskDistributionType.GEV:
            uniforms = self._normal_cdf(draws)
            transformed = self._apply_gev_inverse_cdf(uniforms, params) * shock_multiplier
        elif distribution == RiskDistributionType.GARCH:
            omega = params.get("omega", 0.0001)
            alpha = params.get("alpha", 0.05)
            beta = params.get("beta", 0.9)
            variance = max(params.get("sigma0", 0.02), 1e-4) ** 2
            series = np.zeros_like(draws)
            for idx, shock in enumerate(draws):
                variance = omega + alpha * (series[idx - 1] ** 2 if idx > 0 else 0.0) + beta * variance
                variance = max(variance, 1e-8)
                series[idx] = shock * math.sqrt(variance)
            loc = (params.get("mu", params.get("loc", 0.0)) + seasonal) * horizon
            transformed = (loc + series) * shock_multiplier
        else:
            mu = (params.get("mu", params.get("loc", 0.0)) + seasonal) * horizon
            sigma = params.get("sigma", params.get("scale", 0.1)) * regime_scale * math.sqrt(horizon)
            transformed = (mu + sigma * draws) * shock_multiplier

        transformed = np.clip(transformed, -1e6, 1e6)

        return transformed

    def _apply_scenario_to_correlation(
        self,
        correlation_matrix: np.ndarray,
        factor_names: List[str],
        scenario: Optional[RiskScenario]
    ) -> np.ndarray:
        """Adjust correlation matrix using scenario definition."""

        if scenario is None or not scenario.correlation_shocks:
            return correlation_matrix

        adjusted = correlation_matrix.copy()

        for factor, shock in scenario.correlation_shocks.items():
            if factor in factor_names:
                idx = factor_names.index(factor)
                multiplier = max(0.0, 1.0 + shock)
                adjusted[idx, :] *= multiplier
                adjusted[:, idx] *= multiplier

        adjusted = np.clip(adjusted, -0.99, 0.99)
        np.fill_diagonal(adjusted, 1.0)
        adjusted = self._ensure_positive_definite(adjusted)

        return adjusted

    def _generate_factor_draws(
        self,
        rng: np.random.Generator,
        factor_names: List[str],
        correlation_matrix: np.ndarray,
        num_simulations: int,
        time_horizon_days: int,
        scenario: Optional[RiskScenario]
    ) -> np.ndarray:
        """Generate correlated factor shocks with distribution-aware transforms."""

        if len(factor_names) == 0:
            raise ValueError("No factors provided for Monte Carlo simulation")

        safe_cov = correlation_matrix + np.eye(len(factor_names)) * 1e-6
        err_state = np.seterr(invalid="ignore", divide="ignore", over="ignore")
        try:
            base_draws = rng.multivariate_normal(
                mean=np.zeros(len(factor_names)),
                cov=safe_cov,
                size=num_simulations,
                check_valid="ignore",
                method="eigh"
            )
        finally:
            np.seterr(**err_state)

        factor_draws = np.zeros_like(base_draws)

        for idx, factor_name in enumerate(factor_names):
            shock = None
            if scenario is not None and scenario.risk_factors:
                shock = scenario.risk_factors.get(factor_name)
            factor_draws[:, idx] = self._transform_draws_for_distribution(
                base_draws[:, idx],
                factor_name,
                time_horizon_days,
                scenario_shock=shock,
                rng=rng
            )

        factor_draws = np.nan_to_num(factor_draws, nan=0.0, posinf=1e6, neginf=-1e6)

        return factor_draws

    def _aggregate_position_pnl(
        self,
        factor_draws: np.ndarray,
        exposures: np.ndarray,
        position_scalars: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Convert factor shocks into position and portfolio PnL arrays."""

        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            position_factor_returns = factor_draws @ exposures.T  # shape: simulations x positions
            position_factor_returns = np.nan_to_num(position_factor_returns, nan=0.0, posinf=1e6, neginf=-1e6)
            position_pnl = position_factor_returns * position_scalars  # broadcast scalars
            portfolio_returns = np.sum(position_pnl, axis=1)

        return portfolio_returns, position_pnl

    def _bootstrap_confidence_interval(
        self,
        returns: np.ndarray,
        confidence_level: float,
        bootstrap_samples: int = 500
    ) -> Tuple[float, float]:
        """Bootstrap mean confidence interval for Monte Carlo outputs."""

        if len(returns) == 0:
            return 0.0, 0.0

        rng = np.random.default_rng()
        sample_means = []
        for _ in range(min(bootstrap_samples, len(returns))):
            resample = rng.choice(returns, size=len(returns), replace=True)
            sample_means.append(np.mean(resample))

        lower = np.percentile(sample_means, (1 - confidence_level) / 2 * 100)
        upper = np.percentile(sample_means, (1 + confidence_level) / 2 * 100)

        return float(lower), float(upper)

    async def _simulate_portfolio_returns(
        self,
        portfolio_id: str,
        positions: List[PortfolioPosition],
        num_simulations: int,
        time_horizon_days: int,
        scenario: Optional[RiskScenario] = None,
        monte_carlo_config: Optional[MonteCarloConfig] = None,
        cache_key: Optional[str] = None,
    ) -> PortfolioSimulationCache:
        """Run Monte Carlo simulation and persist cache for downstream risk metrics."""

        factor_names, exposures, position_scalars, position_names = self._collect_portfolio_factors(positions)
        correlation_model = self._determine_correlation_model(factor_names)
        correlation_matrix = self._build_correlation_matrix(exposures, factor_names, correlation_model)
        correlation_matrix = self._apply_scenario_to_correlation(correlation_matrix, factor_names, scenario)

        config = monte_carlo_config or MonteCarloConfig()
        simulations = int(max(num_simulations, 1))
        effective_horizon = max(time_horizon_days, scenario.duration_days if scenario else time_horizon_days)

        async with self._rng_lock:
            rng = self._rng if config.random_seed is None else np.random.default_rng(config.random_seed)
            factor_draws = self._generate_factor_draws(
                rng,
                factor_names,
                correlation_matrix,
                simulations,
                effective_horizon,
                scenario,
            )

            if config.random_seed is not None:
                # Reset shared RNG to avoid repeated seeded draws in subsequent calls
                self._rng = np.random.default_rng()

        portfolio_returns, position_pnl = self._aggregate_position_pnl(
            factor_draws,
            exposures,
            position_scalars
        )

        mean_return = float(np.mean(portfolio_returns))
        std_return = float(np.std(portfolio_returns))
        cv = std_return / abs(mean_return) if mean_return else float("inf")
        converged = cv < config.convergence_threshold if mean_return else False

        factor_volatility = {
            factor: float(np.std(factor_draws[:, idx]))
            for idx, factor in enumerate(factor_names)
        }

        metadata = {
            "correlation_model": correlation_model.value,
            "time_horizon_days": effective_horizon,
            "mean_return": mean_return,
            "std_return": std_return,
            "coefficient_of_variation": cv,
            "converged": converged,
            "factor_volatility": factor_volatility,
            "scenario_id": scenario.scenario_id if scenario else None,
        }

        simulation_cache = PortfolioSimulationCache(
            portfolio_returns=portfolio_returns,
            factor_names=factor_names,
            factor_draws=factor_draws,
            correlation_matrix=correlation_matrix,
            position_names=position_names,
            position_pnl=position_pnl,
            exposures=exposures,
            metadata=metadata,
        )

        key = cache_key or portfolio_id
        self._simulation_cache[key] = simulation_cache

        return simulation_cache

    async def calculate_portfolio_var(
        self,
        portfolio_id: str,
        confidence_level: float = 0.95,
        time_horizon_days: int = 1,
        num_simulations: int = 10000,
        monte_carlo_config: Optional[MonteCarloConfig] = None
    ) -> RiskCalculationResult:
        """Calculate Value at Risk for portfolio."""
        calculation_id = str(uuid4())

        try:
            start_time = datetime.utcnow()

            # Get portfolio positions
            positions = self._portfolio_positions.get(portfolio_id, [])
            if not positions:
                raise ValueError(f"No positions found for portfolio {portfolio_id}")

            config = monte_carlo_config or self._default_monte_carlo_config

            simulation_cache = await self._simulate_portfolio_returns(
                portfolio_id,
                positions,
                num_simulations=num_simulations,
                time_horizon_days=time_horizon_days,
                scenario=None,
                monte_carlo_config=config,
            )

            portfolio_returns = simulation_cache.portfolio_returns
            losses = -portfolio_returns
            sorted_losses = np.sort(losses)

            var_percentile = confidence_level * 100
            var_value = float(np.percentile(sorted_losses, var_percentile))
            tail_losses = sorted_losses[sorted_losses >= var_value]
            cvar_value = float(np.mean(tail_losses)) if len(tail_losses) else float(var_value)

            var_99 = float(np.percentile(sorted_losses, 99))
            tail_losses_99 = sorted_losses[sorted_losses >= var_99]
            cvar_99 = float(np.mean(tail_losses_99)) if len(tail_losses_99) else float(var_99)

            var_value = max(var_value, 0.0)
            cvar_value = max(cvar_value, var_value)
            var_99 = max(var_99, 0.0)
            cvar_99 = max(cvar_99, var_99)

            volatility = float(np.std(portfolio_returns))
            mean_return = float(np.mean(portfolio_returns))
            median_return = float(np.median(portfolio_returns))

            risk_free_rate = 0.02
            excess_return = mean_return - risk_free_rate
            sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0

            downside_excess = portfolio_returns - risk_free_rate
            downside_returns = np.where(downside_excess < 0, downside_excess, 0.0)
            downside_deviation = float(np.sqrt(np.mean(np.square(downside_returns))))
            sortino_ratio = excess_return / downside_deviation if downside_deviation > 0 else 0.0

            max_drawdown = self._calculate_max_drawdown(portfolio_returns)

            var_bands = 2.5
            lower_pct = max(0.0, var_percentile - var_bands)
            upper_pct = min(100.0, var_percentile + var_bands)

            confidence_intervals = {
                "mean_return": self._bootstrap_confidence_interval(portfolio_returns, config.confidence_level),
                f"var_{int(confidence_level * 100)}": (
                    max(float(np.percentile(sorted_losses, lower_pct)), 0.0),
                    max(float(np.percentile(sorted_losses, upper_pct)), 0.0)
                ),
                "var_99": (
                    max(float(np.percentile(sorted_losses, 97)), 0.0),
                    max(float(np.percentile(sorted_losses, 100)), 0.0)
                ),
            }

            total_variance = float(np.var(portfolio_returns))
            factor_contributions = {}
            if total_variance > 0:
                for idx, factor in enumerate(simulation_cache.factor_names):
                    covariance = np.cov(simulation_cache.factor_draws[:, idx], portfolio_returns, ddof=0)[0, 1]
                    factor_contributions[factor] = float(covariance / total_variance)

            position_var = np.var(simulation_cache.position_pnl, axis=0)
            position_contributions = {}
            if total_variance > 0:
                for idx, name in enumerate(simulation_cache.position_names):
                    position_contributions[name] = float(position_var[idx] / total_variance)

            execution_time = (datetime.utcnow() - start_time).total_seconds()

            risk_metrics = {
                "expected_return": mean_return,
                "median_return": median_return,
                "volatility": volatility,
                "var": var_value,
                "cvar": cvar_value,
                "var_99": var_99,
                "cvar_99": cvar_99,
                "sharpe_ratio": sharpe_ratio,
                "sortino_ratio": sortino_ratio,
                "max_drawdown": max_drawdown,
                "confidence_level": confidence_level,
                "converged": simulation_cache.metadata.get("converged", False),
            }

            warnings: List[str] = []
            if not simulation_cache.metadata.get("converged"):
                warnings.append(
                    f"Monte Carlo did not meet convergence threshold {config.convergence_threshold:.4f}"
                )

            return RiskCalculationResult(
                calculation_id=calculation_id,
                portfolio_id=portfolio_id,
                calculation_date=datetime.utcnow(),
                risk_metrics=risk_metrics,
                confidence_intervals=confidence_intervals,
                scenario_results={},
                methodology="monte_carlo",
                parameters={
                    "num_simulations": num_simulations,
                    "time_horizon_days": time_horizon_days,
                    "confidence_level": confidence_level,
                    "correlation_model": simulation_cache.metadata.get("correlation_model"),
                    "factor_contributions": factor_contributions,
                    "position_contributions": position_contributions,
                },
                execution_time=execution_time,
                warnings=warnings
            )

        except Exception as e:
            self.telemetry.error("VaR calculation failed", portfolio_id=portfolio_id, error=str(e))
            raise

    def _calculate_max_drawdown(self, returns: np.ndarray) -> float:
        """Calculate maximum drawdown from return series."""
        if len(returns) < 2:
            return 0.0

        # Calculate cumulative returns
        cumulative = np.cumsum(returns)
        peak = np.maximum.accumulate(cumulative)
        drawdown = cumulative - peak
        return float(abs(np.min(drawdown)))

    async def calculate_portfolio_correlations(self, portfolio_id: str) -> Dict[str, Any]:
        """Calculate correlation matrix for portfolio positions."""
        positions = self._portfolio_positions.get(portfolio_id, [])

        if len(positions) < 2:
            return {"error": "Need at least 2 positions for correlation analysis"}

        factor_names, exposures, _, position_names = self._collect_portfolio_factors(positions)
        correlation_model = self._determine_correlation_model(factor_names)
        factor_correlation = self._build_correlation_matrix(exposures, factor_names, correlation_model)

        position_correlation: Optional[np.ndarray] = None
        if portfolio_id in self._simulation_cache:
            cached = self._simulation_cache[portfolio_id]
            if cached.position_pnl.shape[0] > 1:
                position_correlation = np.corrcoef(cached.position_pnl, rowvar=False)

        return {
            "portfolio_id": portfolio_id,
            "factor_names": factor_names,
            "factor_correlation_matrix": factor_correlation.tolist(),
            "correlation_method": correlation_model.value,
            "position_names": position_names,
            "position_correlation_matrix": position_correlation.tolist() if position_correlation is not None else None,
        }

    async def run_risk_scenario_analysis(
        self,
        portfolio_id: str,
        scenarios: List[RiskScenario],
        num_simulations: int = 5000,
        monte_carlo_config: Optional[MonteCarloConfig] = None
    ) -> Dict[str, Any]:
        """Run risk scenario analysis with multiple scenarios."""

        positions = self._portfolio_positions.get(portfolio_id, [])

        if not positions:
            raise ValueError(f"No positions found for portfolio {portfolio_id}")

        results: Dict[str, Dict[str, float]] = {}

        for scenario in scenarios:
            scenario_result = await self._calculate_scenario_risk(
                portfolio_id,
                positions,
                scenario,
                num_simulations,
                monte_carlo_config,
            )
            results[scenario.scenario_id] = scenario_result

        comparison_metrics = self._compare_scenario_results(results)

        return {
            "portfolio_id": portfolio_id,
            "scenario_results": results,
            "comparison_metrics": comparison_metrics,
        }

    async def _calculate_scenario_risk(
        self,
        portfolio_id: str,
        positions: List[PortfolioPosition],
        scenario: RiskScenario,
        num_simulations: int,
        monte_carlo_config: Optional[MonteCarloConfig] = None,
    ) -> Dict[str, float]:
        """Calculate risk metrics for a specific scenario."""
        config = monte_carlo_config or self._default_monte_carlo_config

        horizon = max(1, scenario.duration_days)

        simulation_cache = await self._simulate_portfolio_returns(
            portfolio_id,
            positions,
            num_simulations=num_simulations,
            time_horizon_days=horizon,
            scenario=scenario,
            monte_carlo_config=config,
            cache_key=f"{portfolio_id}:{scenario.scenario_id}",
        )

        portfolio_returns = simulation_cache.portfolio_returns
        losses = -portfolio_returns

        var_95 = float(np.percentile(losses, 95))
        tail_95 = losses[losses >= var_95]
        cvar_95 = float(np.mean(tail_95)) if len(tail_95) else float(var_95)

        var_99 = float(np.percentile(losses, 99))
        tail_99 = losses[losses >= var_99]
        cvar_99 = float(np.mean(tail_99)) if len(tail_99) else float(var_99)

        var_95 = max(var_95, 0.0)
        cvar_95 = max(cvar_95, var_95)
        var_99 = max(var_99, 0.0)
        cvar_99 = max(cvar_99, var_99)

        expected_return = float(np.mean(portfolio_returns))
        volatility = float(np.std(portfolio_returns))
        max_loss = float(np.max(losses))

        return {
            "scenario_id": scenario.scenario_id,
            "scenario_name": scenario.scenario_name,
            "expected_return": expected_return,
            "volatility": volatility,
            "var_95": var_95,
            "cvar_95": cvar_95,
            "var_99": var_99,
            "cvar_99": cvar_99,
            "max_loss": max_loss,
            "probability": scenario.probability,
            "correlation_model": simulation_cache.metadata.get("correlation_model"),
        }

    def _compare_scenario_results(self, scenario_results: Dict[str, Any]) -> Dict[str, Any]:
        """Compare results across scenarios."""

        if not scenario_results:
            return {}

        total_prob = sum(result.get("probability", 0.0) for result in scenario_results.values())

        def _weighted(metric: str) -> float:
            values = [result.get(metric, 0.0) for result in scenario_results.values()]
            if total_prob > 0:
                return float(sum(result.get(metric, 0.0) * result.get("probability", 0.0) for result in scenario_results.values()) / total_prob)
            return float(np.mean(values)) if values else 0.0

        return {
            "scenario_count": len(scenario_results),
            "weighted_expected_return": _weighted("expected_return"),
            "weighted_volatility": _weighted("volatility"),
            "weighted_var_95": _weighted("var_95"),
            "weighted_cvar_95": _weighted("cvar_95"),
            "worst_case_var": max(result.get("var_95", 0.0) for result in scenario_results.values()),
            "best_case_var": min(result.get("var_95", 0.0) for result in scenario_results.values()),
            "worst_case_loss": max(result.get("max_loss", 0.0) for result in scenario_results.values()),
        }

    async def calculate_portfolio_aggregation(
        self,
        portfolio_id: str,
        include_correlations: bool = True
    ) -> PortfolioAggregation:
        """Calculate portfolio-level risk aggregation."""

        positions = self._portfolio_positions.get(portfolio_id, [])

        if not positions:
            raise ValueError(f"No positions found for portfolio {portfolio_id}")

        simulation_cache = self._simulation_cache.get(portfolio_id)

        if simulation_cache is None or simulation_cache.portfolio_returns.size == 0:
            simulation_cache = await self._simulate_portfolio_returns(
                portfolio_id,
                positions,
                num_simulations=self._default_monte_carlo_config.num_simulations,
                time_horizon_days=1,
                monte_carlo_config=self._default_monte_carlo_config,
                cache_key=portfolio_id,
            )

        losses = -simulation_cache.portfolio_returns
        total_var = max(float(np.percentile(losses, 95)), 0.0)
        tail_losses = losses[losses >= total_var]
        total_cvar = max(float(np.mean(tail_losses)) if len(tail_losses) else float(total_var), total_var)
        total_volatility = float(np.std(simulation_cache.portfolio_returns))

        position_losses = -simulation_cache.position_pnl
        position_vars = {
            position_id: max(float(np.percentile(position_losses[:, idx], 95)), 0.0)
            for idx, position_id in enumerate(simulation_cache.position_names)
        }

        standalone_var_sum = sum(position_vars.values())
        diversification_benefit = 0.0
        if include_correlations and standalone_var_sum > 0:
            diversification_benefit = max(0.0, (standalone_var_sum - total_var) / standalone_var_sum)

        total_notional = sum(abs(pos.notional_value) for pos in positions)
        max_position_notional = max(abs(pos.notional_value) for pos in positions)
        concentration_risk = (max_position_notional / total_notional) if total_notional else 0.0

        liquidity_risk = min(0.5, 0.05 + 0.25 * concentration_risk)
        counterparty_risk = min(0.3, 0.02 * len(positions))

        total_variance = float(np.var(simulation_cache.portfolio_returns))
        risk_attribution = {
            name: float(np.var(simulation_cache.position_pnl[:, idx]) / total_variance) if total_variance > 0 else 0.0
            for idx, name in enumerate(simulation_cache.position_names)
        }

        asset_breakdown = {}
        for pos in positions:
            weight = pos.notional_value / total_notional if total_notional else 0.0
            type_indicator = 1.0
            if pos.position_type.lower() == "short":
                type_indicator = -1.0
            elif pos.position_type.lower() == "hedge":
                type_indicator = 0.0
            asset_breakdown[pos.asset_id] = {
                "var": position_vars.get(pos.asset_id, 0.0),
                "weight": weight,
                "position_direction": type_indicator,
            }

        return PortfolioAggregation(
            portfolio_id=portfolio_id,
            aggregation_date=datetime.utcnow(),
            total_var=total_var,
            total_cvar=total_cvar,
            total_volatility=total_volatility,
            diversification_benefit=diversification_benefit,
            concentration_risk=concentration_risk,
            liquidity_risk=liquidity_risk,
            counterparty_risk=counterparty_risk,
            asset_breakdown=asset_breakdown,
            risk_attribution=risk_attribution,
        )

    async def get_risk_dashboard(
        self,
        portfolio_id: str,
        time_horizon_days: int = 1
    ) -> Dict[str, Any]:
        """Get comprehensive risk dashboard for portfolio."""

        # Calculate current risk metrics
        var_result = await self.calculate_portfolio_var(portfolio_id, time_horizon_days=time_horizon_days)

        # Get portfolio aggregation
        aggregation = await self.calculate_portfolio_aggregation(portfolio_id)

        # Get correlation analysis
        correlations = await self.calculate_portfolio_correlations(portfolio_id)

        return {
            "portfolio_id": portfolio_id,
            "calculation_date": datetime.utcnow(),
            "risk_metrics": var_result.risk_metrics,
            "portfolio_aggregation": aggregation,
            "correlations": correlations,
            "recommendations": [
                "Monitor concentration risk in largest positions",
                "Consider hedging strategies for high-risk assets",
                "Regularly rebalance portfolio to maintain diversification"
            ]
        }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "portfolios_tracked": len(self._portfolio_positions),
            "risk_distributions": len(self._risk_distributions),
            "correlation_matrices": len(self._correlation_matrices),
            "last_calculation": datetime.utcnow()
        }


_risk_engine_service: Optional[RiskEngineService] = None


def get_risk_engine_service() -> RiskEngineService:
    """Get the global risk engine service instance (singleton)."""
    global _risk_engine_service
    if _risk_engine_service is None:
        _risk_engine_service = RiskEngineService()
    return _risk_engine_service


async def calculate_portfolio_risk_metrics(
    portfolio_id: str,
    confidence_level: float = 0.95,
    time_horizon_days: int = 1
) -> RiskCalculationResult:
    """Calculate comprehensive risk metrics for portfolio."""
    service = get_risk_engine_service()
    return await service.calculate_portfolio_var(
        portfolio_id, confidence_level, time_horizon_days
    )


async def run_stress_test(
    portfolio_id: str,
    stress_scenarios: List[RiskScenario]
) -> Dict[str, Any]:
    """Run stress test scenarios on portfolio."""
    service = get_risk_engine_service()
    return await service.run_risk_scenario_analysis(portfolio_id, stress_scenarios)
