"""Phase 3: Scenario validation and simulation framework.

This module provides advanced validation and simulation capabilities for
complex scenario compositions with support for:
- Multi-driver validation
- Scenario composition validation
- Simulation framework with confidence intervals
- Cross-scenario consistency checks
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured as _log_structured

# Create async wrapper for log_structured
async def log_structured(level: str, event: str, **kwargs):
    """Async wrapper for log_structured."""
    return _log_structured(level, event, **kwargs)
from .models import DriverType, ScenarioAssumption, _PAYLOAD_MODELS


logger = logging.getLogger(__name__)


class ValidationSeverity(str, Enum):
    """Validation issue severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a validation issue found during scenario validation."""
    severity: ValidationSeverity
    code: str
    message: str
    field_path: Optional[str] = None
    suggestion: Optional[str] = None


class ScenarioValidationResult(BaseModel):
    """Result of scenario validation."""
    is_valid: bool = Field(..., description="Whether scenario passed validation")
    issues: List[ValidationIssue] = Field(default_factory=list, description="Validation issues found")
    validation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique validation ID")
    validated_at: datetime = Field(default_factory=datetime.utcnow, description="Validation timestamp")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Validation metrics")


class SimulationConfig(BaseModel):
    """Configuration for scenario simulations."""
    simulation_type: str = Field(..., description="Type of simulation (monte_carlo, sensitivity, stress)")
    num_iterations: int = Field(default=1000, ge=1, le=100000, description="Number of simulation iterations")
    confidence_level: float = Field(default=0.95, ge=0.0, le=1.0, description="Confidence level for intervals")
    random_seed: Optional[int] = Field(None, description="Random seed for reproducibility")
    parallel_execution: bool = Field(default=True, description="Enable parallel execution")
    max_workers: int = Field(default=4, ge=1, le=32, description="Maximum parallel workers")


@dataclass
class SimulationResult:
    """Result of scenario simulation."""
    simulation_id: str
    scenario_id: str
    results: np.ndarray
    statistics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    execution_time: float
    metadata: Dict[str, Any]


class ScenarioValidator:
    """Advanced scenario validation with composition support."""

    def __init__(self):
        self._custom_validators = {}
        
    async def validate_scenario(
        self, 
        assumptions: List[ScenarioAssumption],
        scenario_id: Optional[str] = None
    ) -> ScenarioValidationResult:
        """Validate a complete scenario with all assumptions."""
        issues = []
        metrics = {}
        
        start_time = datetime.utcnow()
        
        try:
            # Basic payload validation
            payload_issues = await self._validate_payloads(assumptions)
            issues.extend(payload_issues)
            
            # Cross-driver consistency validation
            consistency_issues = await self._validate_consistency(assumptions)
            issues.extend(consistency_issues)
            
            # Composition validation for composite drivers
            composition_issues = await self._validate_compositions(assumptions)
            issues.extend(composition_issues)
            
            # Time series validation
            timeseries_issues = await self._validate_time_series(assumptions)
            issues.extend(timeseries_issues)
            
            validation_time = (datetime.utcnow() - start_time).total_seconds()
            metrics = {
                "validation_time_seconds": validation_time,
                "total_assumptions": len(assumptions),
                "driver_types": list(set(a.driver_type for a in assumptions)),
                "error_count": len([i for i in issues if i.severity == ValidationSeverity.ERROR]),
                "warning_count": len([i for i in issues if i.severity == ValidationSeverity.WARNING])
            }
            
            is_valid = not any(issue.severity == ValidationSeverity.ERROR for issue in issues)
            
            await log_structured(
                "info",
                "scenario_validation_completed",
                scenario_id=scenario_id,
                is_valid=is_valid,
                issue_count=len(issues),
                validation_time=validation_time
            )
            
            return ScenarioValidationResult(
                is_valid=is_valid,
                issues=issues,
                metrics=metrics
            )
            
        except Exception as e:
            logger.exception("Scenario validation failed")
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                code="VALIDATION_FAILED",
                message=f"Validation process failed: {str(e)}"
            ))
            
            return ScenarioValidationResult(
                is_valid=False,
                issues=issues,
                metrics=metrics
            )
    
    async def _validate_payloads(self, assumptions: List[ScenarioAssumption]) -> List[ValidationIssue]:
        """Validate individual assumption payloads."""
        issues = []
        
        for i, assumption in enumerate(assumptions):
            try:
                model_class = _PAYLOAD_MODELS.get(assumption.driver_type)
                if model_class is None:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        code="UNSUPPORTED_DRIVER_TYPE",
                        message=f"Unsupported driver type: {assumption.driver_type}",
                        field_path=f"assumptions[{i}].driver_type"
                    ))
                    continue
                
                # Validate payload structure
                model_class(**assumption.payload)
                
            except Exception as e:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    code="PAYLOAD_VALIDATION_FAILED",
                    message=f"Payload validation failed: {str(e)}",
                    field_path=f"assumptions[{i}].payload"
                ))
        
        return issues
    
    async def _validate_consistency(self, assumptions: List[ScenarioAssumption]) -> List[ValidationIssue]:
        """Validate cross-driver consistency."""
        issues = []
        
        # Check for conflicting assumptions
        driver_counts = {}
        for assumption in assumptions:
            driver_counts[assumption.driver_type] = driver_counts.get(assumption.driver_type, 0) + 1
        
        # Warn about multiple assumptions of same type without composition
        for driver_type, count in driver_counts.items():
            if count > 1 and driver_type != DriverType.COMPOSITE:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    code="MULTIPLE_ASSUMPTIONS_SAME_TYPE",
                    message=f"Multiple assumptions of type {driver_type} found. Consider using composite driver.",
                    suggestion="Use composite driver to combine multiple assumptions of the same type"
                ))
        
        return issues
    
    async def _validate_compositions(self, assumptions: List[ScenarioAssumption]) -> List[ValidationIssue]:
        """Validate composite driver compositions."""
        issues = []
        
        composite_assumptions = [a for a in assumptions if a.driver_type == DriverType.COMPOSITE]
        
        for i, assumption in enumerate(composite_assumptions):
            payload = assumption.payload
            components = payload.get("components", [])
            
            # Validate component references
            for j, component in enumerate(components):
                if not isinstance(component, dict):
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        code="INVALID_COMPONENT_FORMAT",
                        message="Component must be a dictionary",
                        field_path=f"assumptions[{i}].payload.components[{j}]"
                    ))
                    continue
                
                # Check component has required driver_type
                if "driver_type" not in component:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        code="MISSING_COMPONENT_DRIVER_TYPE",
                        message="Component missing driver_type",
                        field_path=f"assumptions[{i}].payload.components[{j}].driver_type"
                    ))
        
        return issues
    
    async def _validate_time_series(self, assumptions: List[ScenarioAssumption]) -> List[ValidationIssue]:
        """Validate time series assumptions."""
        issues = []
        
        ts_assumptions = [a for a in assumptions if a.driver_type == DriverType.TIME_SERIES]
        
        for i, assumption in enumerate(ts_assumptions):
            payload = assumption.payload
            
            # Validate forecast horizon
            horizon = payload.get("forecast_horizon", 24)
            if horizon < 1 or horizon > 8760:  # Max 1 year
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    code="EXTREME_FORECAST_HORIZON",
                    message=f"Forecast horizon {horizon} may be too extreme",
                    field_path=f"assumptions[{i}].payload.forecast_horizon",
                    suggestion="Consider using a horizon between 1 and 8760 periods"
                ))
        
        return issues


class ScenarioSimulator:
    """Advanced scenario simulation engine with parallel execution."""
    
    def __init__(self):
        self._simulation_cache = {}
        
    async def run_simulation(
        self,
        assumptions: List[ScenarioAssumption],
        config: SimulationConfig,
        scenario_id: Optional[str] = None
    ) -> SimulationResult:
        """Run scenario simulation with specified configuration."""
        simulation_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        try:
            # Set random seed for reproducibility
            if config.random_seed is not None:
                np.random.seed(config.random_seed)
            
            # Choose simulation method based on type
            if config.simulation_type == "monte_carlo":
                results = await self._run_monte_carlo(assumptions, config)
            elif config.simulation_type == "sensitivity":
                results = await self._run_sensitivity_analysis(assumptions, config)
            elif config.simulation_type == "stress":
                results = await self._run_stress_testing(assumptions, config)
            else:
                raise ValueError(f"Unsupported simulation type: {config.simulation_type}")
            
            # Calculate statistics
            statistics = {
                "mean": float(np.mean(results)),
                "std": float(np.std(results)),
                "min": float(np.min(results)),
                "max": float(np.max(results)),
                "median": float(np.median(results)),
                "skewness": float(self._calculate_skewness(results)),
                "kurtosis": float(self._calculate_kurtosis(results))
            }
            
            # Calculate confidence intervals
            confidence_intervals = self._calculate_confidence_intervals(results, config.confidence_level)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            await log_structured(
                "info",
                "scenario_simulation_completed",
                simulation_id=simulation_id,
                scenario_id=scenario_id,
                simulation_type=config.simulation_type,
                num_iterations=config.num_iterations,
                execution_time=execution_time
            )
            
            return SimulationResult(
                simulation_id=simulation_id,
                scenario_id=scenario_id or "unknown",
                results=results,
                statistics=statistics,
                confidence_intervals=confidence_intervals,
                execution_time=execution_time,
                metadata={
                    "config": config.model_dump(),
                    "num_assumptions": len(assumptions),
                    "driver_types": [a.driver_type for a in assumptions]
                }
            )
            
        except Exception as e:
            logger.exception("Scenario simulation failed")
            raise RuntimeError(f"Simulation failed: {str(e)}")
    
    async def _run_monte_carlo(self, assumptions: List[ScenarioAssumption], config: SimulationConfig) -> np.ndarray:
        """Run Monte Carlo simulation."""
        results = []
        
        if config.parallel_execution:
            # Parallel execution using asyncio
            semaphore = asyncio.Semaphore(config.max_workers)
            tasks = []
            
            for i in range(config.num_iterations):
                task = self._run_single_iteration(assumptions, semaphore, i)
                tasks.append(task)
            
            iteration_results = await asyncio.gather(*tasks)
            results = np.array(iteration_results)
        else:
            # Sequential execution
            for i in range(config.num_iterations):
                result = await self._simulate_single_iteration(assumptions, i)
                results.append(result)
            results = np.array(results)
        
        return results
    
    async def _run_single_iteration(self, assumptions: List[ScenarioAssumption], semaphore: asyncio.Semaphore, iteration: int) -> float:
        """Run a single simulation iteration with semaphore control."""
        async with semaphore:
            return await self._simulate_single_iteration(assumptions, iteration)
    
    async def _simulate_single_iteration(self, assumptions: List[ScenarioAssumption], iteration: int) -> float:
        """Simulate a single iteration - placeholder implementation."""
        # This is a simplified simulation - in real implementation would run actual scenario logic
        await asyncio.sleep(0.001)  # Simulate work
        return np.random.normal(100, 15)  # Placeholder result
    
    async def _run_sensitivity_analysis(self, assumptions: List[ScenarioAssumption], config: SimulationConfig) -> np.ndarray:
        """Run sensitivity analysis simulation."""
        # Placeholder implementation
        results = []
        for i in range(config.num_iterations):
            # Vary parameters systematically
            variation = (i / config.num_iterations - 0.5) * 2  # -1 to 1
            result = 100 + variation * 20  # Base value with variation
            results.append(result)
        return np.array(results)
    
    async def _run_stress_testing(self, assumptions: List[ScenarioAssumption], config: SimulationConfig) -> np.ndarray:
        """Run stress testing simulation."""
        # Placeholder implementation with extreme scenarios
        results = []
        for i in range(config.num_iterations):
            # Generate extreme scenarios
            stress_factor = np.random.choice([-2, -1.5, 1.5, 2])  # Extreme multipliers
            result = 100 * stress_factor
            results.append(result)
        return np.array(results)
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """Calculate skewness of data."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0
        return np.mean(((data - mean) / std) ** 3)
    
    def _calculate_kurtosis(self, data: np.ndarray) -> float:
        """Calculate kurtosis of data."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0
        return np.mean(((data - mean) / std) ** 4) - 3
    
    def _calculate_confidence_intervals(self, data: np.ndarray, confidence_level: float) -> Dict[str, Tuple[float, float]]:
        """Calculate confidence intervals."""
        alpha = 1 - confidence_level
        lower_percentile = (alpha / 2) * 100
        upper_percentile = (1 - alpha / 2) * 100
        
        lower_bound = np.percentile(data, lower_percentile)
        upper_bound = np.percentile(data, upper_percentile)
        
        return {
            "confidence_interval": (float(lower_bound), float(upper_bound)),
            "interquartile_range": (float(np.percentile(data, 25)), float(np.percentile(data, 75)))
        }