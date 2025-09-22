"""Monte Carlo simulation engine for demand, renewables, and price curves.

This module provides a comprehensive Monte Carlo simulation framework that includes:
- Reproducible random number generation with seeds
- Specialized models for demand, renewable generation, and price curves
- Statistical validation and convergence checking
- Integration with scenario constraints and curve families
"""

from __future__ import annotations

import asyncio
import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
from numpy.random import Generator, PCG64
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured


class MonteCarloConfig(BaseModel):
    """Configuration for Monte Carlo simulations."""

    num_simulations: int = Field(default=1000, ge=1, le=100000, description="Number of Monte Carlo simulations")
    random_seed: Optional[int] = Field(None, description="Random seed for reproducible results")
    convergence_threshold: float = Field(default=0.01, ge=0.0, le=1.0, description="Convergence threshold for stopping early")
    max_iterations: int = Field(default=10000, ge=1, description="Maximum number of iterations")
    confidence_level: float = Field(default=0.95, ge=0.0, le=1.0, description="Confidence level for statistical bounds")
    parallel_processing: bool = Field(default=True, description="Whether to use parallel processing")
    chunk_size: int = Field(default=100, ge=1, description="Chunk size for parallel processing")


@dataclass
class SimulationResult:
    """Results from a Monte Carlo simulation."""

    mean: float
    median: float
    std_dev: float
    confidence_interval: Tuple[float, float]
    percentiles: Dict[str, float]
    converged: bool
    iterations: int
    execution_time: float
    raw_results: np.ndarray = field(default_factory=lambda: np.array([]))
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseMonteCarloModel(ABC):
    """Abstract base class for Monte Carlo models."""

    def __init__(self, config: MonteCarloConfig):
        self.config = config
        self.rng: Optional[Generator] = None
        self._setup_rng()

    def _setup_rng(self) -> None:
        """Set up random number generator with seed."""
        if self.config.random_seed is not None:
            self.rng = Generator(PCG64(self.config.random_seed))
        else:
            self.rng = Generator(PCG64())

    def reset_rng(self, seed: Optional[int] = None) -> None:
        """Reset random number generator with new seed."""
        if seed is not None:
            self.config.random_seed = seed
        self._setup_rng()

    @abstractmethod
    def simulate(self, parameters: Dict[str, Any], num_simulations: Optional[int] = None) -> SimulationResult:
        """Run Monte Carlo simulation with given parameters."""
        pass

    @abstractmethod
    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate simulation parameters."""
        pass

    def _check_convergence(self, results: np.ndarray, threshold: float = None) -> Tuple[bool, float]:
        """Check if simulation has converged."""
        if threshold is None:
            threshold = self.config.convergence_threshold

        if len(results) < 100:
            return False, 1.0

        # Use coefficient of variation as convergence metric
        mean_val = np.mean(results)
        std_val = np.std(results)

        if mean_val == 0:
            return False, 1.0

        cv = std_val / abs(mean_val)
        converged = cv < threshold

        return converged, cv


class DemandModel(BaseMonteCarloModel):
    """Monte Carlo model for electricity demand forecasting."""

    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate demand model parameters."""
        required_params = ["base_demand", "trend_coefficient", "seasonal_volatility", "weather_sensitivity"]
        return all(param in parameters for param in required_params)

    def simulate(self, parameters: Dict[str, Any], num_simulations: Optional[int] = None) -> SimulationResult:
        """Simulate demand using Monte Carlo methods."""
        if not self.validate_parameters(parameters):
            raise ValueError("Invalid parameters for demand model")

        n = num_simulations or self.config.num_simulations
        start_time = datetime.now()

        # Extract parameters
        base_demand = parameters["base_demand"]
        trend_coeff = parameters["trend_coefficient"]
        seasonal_vol = parameters["seasonal_volatility"]
        weather_sens = parameters["weather_sensitivity"]
        time_horizon = parameters.get("time_horizon", 24)  # hours

        # Generate demand scenarios
        results = np.zeros(n)

        for i in range(n):
            # Base demand with trend
            demand = base_demand * (1 + trend_coeff * self.rng.random())

            # Seasonal variation (simplified daily pattern)
            hour_of_day = self.rng.integers(0, 24)
            seasonal_factor = 0.7 + 0.6 * math.sin(2 * math.pi * hour_of_day / 24)
            demand *= seasonal_factor

            # Weather impact
            temperature_deviation = self.rng.normal(0, 5)  # °C
            demand += weather_sens * temperature_deviation

            # Random volatility
            demand *= (1 + self.rng.normal(0, seasonal_vol))

            # Economic factors
            economic_factor = parameters.get("economic_factor", 1.0)
            demand *= economic_factor

            results[i] = max(0, demand)  # Ensure non-negative

        # Calculate statistics
        mean_demand = np.mean(results)
        median_demand = np.median(results)
        std_demand = np.std(results)

        # Confidence interval
        confidence_level = self.config.confidence_level
        z_score = 1.96  # For 95% confidence
        margin_error = z_score * (std_demand / math.sqrt(n))
        ci_lower = mean_demand - margin_error
        ci_upper = mean_demand + margin_error

        # Percentiles
        percentiles = {
            "p5": np.percentile(results, 5),
            "p25": np.percentile(results, 25),
            "p75": np.percentile(results, 75),
            "p95": np.percentile(results, 95),
        }

        # Check convergence
        converged, cv = self._check_convergence(results)

        execution_time = (datetime.now() - start_time).total_seconds()

        return SimulationResult(
            mean=mean_demand,
            median=median_demand,
            std_dev=std_demand,
            confidence_interval=(ci_lower, ci_upper),
            percentiles=percentiles,
            converged=converged,
            iterations=n,
            execution_time=execution_time,
            raw_results=results,
            metadata={
                "model_type": "demand",
                "parameters": parameters,
                "convergence_metric": cv,
            }
        )


class RenewableModel(BaseMonteCarloModel):
    """Monte Carlo model for renewable energy generation."""

    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate renewable model parameters."""
        required_params = ["capacity_mw", "capacity_factor", "intermittency_factor", "curtailment_rate"]
        return all(param in parameters for param in required_params)

    def simulate(self, parameters: Dict[str, Any], num_simulations: Optional[int] = None) -> SimulationResult:
        """Simulate renewable generation using Monte Carlo methods."""
        if not self.validate_parameters(parameters):
            raise ValueError("Invalid parameters for renewable model")

        n = num_simulations or self.config.num_simulations
        start_time = datetime.now()

        # Extract parameters
        capacity = parameters["capacity_mw"]
        base_cf = parameters["capacity_factor"]
        intermittency = parameters["intermittency_factor"]
        curtailment = parameters["curtailment_rate"]
        forecast_error = parameters.get("forecast_error", 0.1)

        results = np.zeros(n)

        for i in range(n):
            # Base capacity factor with variation
            cf = base_cf * (1 + self.rng.normal(0, intermittency))

            # Weather variability (more complex in reality)
            weather_variation = self.rng.normal(0, forecast_error)
            cf *= (1 + weather_variation)

            # Curtailment
            if self.rng.random() < curtailment:
                cf *= self.rng.uniform(0, 0.5)  # 0-50% curtailment

            # Ensure physical constraints
            cf = max(0, min(1, cf))

            # Calculate generation
            generation = capacity * cf * 24  # MWh per day
            results[i] = generation

        # Calculate statistics
        mean_gen = np.mean(results)
        median_gen = np.median(results)
        std_gen = np.std(results)

        # Confidence interval
        confidence_level = self.config.confidence_level
        z_score = 1.96
        margin_error = z_score * (std_gen / math.sqrt(n))
        ci_lower = max(0, mean_gen - margin_error)
        ci_upper = mean_gen + margin_error

        # Percentiles
        percentiles = {
            "p5": np.percentile(results, 5),
            "p25": np.percentile(results, 25),
            "p75": np.percentile(results, 75),
            "p95": np.percentile(results, 95),
        }

        # Check convergence
        converged, cv = self._check_convergence(results)

        execution_time = (datetime.now() - start_time).total_seconds()

        return SimulationResult(
            mean=mean_gen,
            median=median_gen,
            std_dev=std_gen,
            confidence_interval=(ci_lower, ci_upper),
            percentiles=percentiles,
            converged=converged,
            iterations=n,
            execution_time=execution_time,
            raw_results=results,
            metadata={
                "model_type": "renewable",
                "parameters": parameters,
                "convergence_metric": cv,
            }
        )


class PriceModel(BaseMonteCarloModel):
    """Monte Carlo model for electricity price forecasting."""

    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate price model parameters."""
        required_params = ["base_price", "demand_elasticity", "supply_shock_probability", "volatility"]
        return all(param in parameters for param in required_params)

    def simulate(self, parameters: Dict[str, Any], num_simulations: Optional[int] = None) -> SimulationResult:
        """Simulate price using Monte Carlo methods."""
        if not self.validate_parameters(parameters):
            raise ValueError("Invalid parameters for price model")

        n = num_simulations or self.config.num_simulations
        start_time = datetime.now()

        # Extract parameters
        base_price = parameters["base_price"]
        demand_elasticity = parameters["demand_elasticity"]
        supply_shock_prob = parameters["supply_shock_probability"]
        volatility = parameters["volatility"]
        price_cap = parameters.get("price_cap", 1000)
        price_floor = parameters.get("price_floor", -100)

        results = np.zeros(n)

        for i in range(n):
            # Start with base price
            price = base_price

            # Demand-driven price changes
            demand_shock = self.rng.normal(0, 0.1)  # 10% std dev
            price *= (1 + demand_elasticity * demand_shock)

            # Supply shocks
            if self.rng.random() < supply_shock_prob:
                shock_magnitude = self.rng.normal(0, 0.3)  # Up to 30% shock
                price *= (1 + shock_magnitude)

            # Market volatility
            price *= (1 + self.rng.normal(0, volatility))

            # Random market sentiment
            sentiment = self.rng.normal(0, 0.05)  # 5% std dev
            price *= (1 + sentiment)

            # Apply caps and floors
            price = max(price_floor, min(price_cap, price))

            results[i] = price

        # Calculate statistics
        mean_price = np.mean(results)
        median_price = np.median(results)
        std_price = np.std(results)

        # Confidence interval
        confidence_level = self.config.confidence_level
        z_score = 1.96
        margin_error = z_score * (std_price / math.sqrt(n))
        ci_lower = max(price_floor, mean_price - margin_error)
        ci_upper = min(price_cap, mean_price + margin_error)

        # Percentiles
        percentiles = {
            "p5": np.percentile(results, 5),
            "p25": np.percentile(results, 25),
            "p75": np.percentile(results, 75),
            "p95": np.percentile(results, 95),
        }

        # Check convergence
        converged, cv = self._check_convergence(results)

        execution_time = (datetime.now() - start_time).total_seconds()

        return SimulationResult(
            mean=mean_price,
            median=median_price,
            std_dev=std_price,
            confidence_interval=(ci_lower, ci_upper),
            percentiles=percentiles,
            converged=converged,
            iterations=n,
            execution_time=execution_time,
            raw_results=results,
            metadata={
                "model_type": "price",
                "parameters": parameters,
                "convergence_metric": cv,
            }
        )


class MonteCarloEngine:
    """Main Monte Carlo simulation engine."""

    def __init__(self):
        self.models: Dict[str, BaseMonteCarloModel] = {}
        self._register_default_models()

    def _register_default_models(self) -> None:
        """Register default Monte Carlo models."""
        self.models["demand"] = DemandModel(MonteCarloConfig())
        self.models["renewable"] = RenewableModel(MonteCarloConfig())
        self.models["price"] = PriceModel(MonteCarloConfig())

    def register_model(self, name: str, model: BaseMonteCarloModel) -> None:
        """Register a custom Monte Carlo model."""
        self.models[name] = model

    async def run_simulation(
        self,
        model_type: str,
        parameters: Dict[str, Any],
        config: Optional[MonteCarloConfig] = None,
        seed: Optional[int] = None,
    ) -> SimulationResult:
        """Run Monte Carlo simulation for given model."""

        if model_type not in self.models:
            raise ValueError(f"Unknown model type: {model_type}")

        model = self.models[model_type]

        # Update config if provided
        if config:
            model.config = config

        # Set seed for reproducibility
        if seed is not None:
            model.reset_rng(seed)

        # Validate parameters
        if not model.validate_parameters(parameters):
            raise ValueError(f"Invalid parameters for {model_type} model")

        # Run simulation
        log_structured(
            "info",
            "monte_carlo_simulation_starting",
            model_type=model_type,
            num_simulations=model.config.num_simulations,
            seed=seed,
        )

        result = await asyncio.get_event_loop().run_in_executor(
            None, model.simulate, parameters, None
        )

        log_structured(
            "info",
            "monte_carlo_simulation_completed",
            model_type=model_type,
            mean=result.mean,
            converged=result.converged,
            execution_time=result.execution_time,
        )

        return result

    async def run_multi_model_simulation(
        self,
        model_configs: Dict[str, Dict[str, Any]],
        global_config: Optional[MonteCarloConfig] = None,
        seed: Optional[int] = None,
    ) -> Dict[str, SimulationResult]:
        """Run simulations for multiple models concurrently."""

        tasks = []

        # Create base config
        config = global_config or MonteCarloConfig()

        for model_type, parameters in model_configs.items():
            task = self.run_simulation(model_type, parameters, config, seed)
            tasks.append((model_type, task))

        # Run all simulations concurrently
        results = {}
        for model_type, task in tasks:
            results[model_type] = await task

        return results

    def get_model_info(self, model_type: str) -> Dict[str, Any]:
        """Get information about a model."""
        if model_type not in self.models:
            raise ValueError(f"Unknown model type: {model_type}")

        model = self.models[model_type]
        return {
            "model_type": model_type,
            "config": model.config.dict(),
        }

    def list_models(self) -> List[str]:
        """List available model types."""
        return list(self.models.keys())


# Global Monte Carlo engine instance
_monte_carlo_engine: Optional[MonteCarloEngine] = None


def get_monte_carlo_engine() -> MonteCarloEngine:
    """Get the global Monte Carlo engine instance."""
    global _monte_carlo_engine
    if _monte_carlo_engine is None:
        _monte_carlo_engine = MonteCarloEngine()
    return _monte_carlo_engine
