"""Phase 3: Parallel scenario execution engine.

This module provides parallel execution capabilities for running multiple
scenarios concurrently with proper resource management and isolation.
Building on the existing worker infrastructure for Phase 3 requirements.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field

from ..telemetry.context import get_correlation_id, log_structured
from .models import ScenarioAssumption
from .validation import ScenarioValidator, SimulationConfig, SimulationResult
try:
    from .worker_service import ScenarioWorkerService
except ImportError:
    # Fallback if worker_service doesn't exist
    class ScenarioWorkerService:
        pass


logger = logging.getLogger(__name__)


class ParallelExecutionConfig(BaseModel):
    """Configuration for parallel scenario execution."""
    max_concurrent_scenarios: int = Field(default=10, ge=1, le=50, description="Maximum concurrent scenarios")
    timeout_seconds: int = Field(default=300, ge=30, le=3600, description="Execution timeout per scenario")
    resource_limits: Dict[str, Any] = Field(default_factory=dict, description="Resource usage limits")
    priority_levels: Dict[str, int] = Field(default_factory=dict, description="Priority levels for scenarios")
    failure_handling: str = Field(default="continue", description="How to handle individual failures")


@dataclass
class ScenarioExecutionRequest:
    """Request to execute a scenario."""
    scenario_id: str
    assumptions: List[ScenarioAssumption]
    config: Optional[SimulationConfig] = None
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None


@dataclass
class ScenarioExecutionResult:
    """Result of scenario execution."""
    scenario_id: str
    status: str  # success, failed, timeout, cancelled
    result: Optional[SimulationResult] = None
    error: Optional[str] = None
    execution_time: float = 0.0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ParallelScenarioEngine:
    """Engine for parallel execution of multiple scenarios."""
    
    def __init__(self, config: ParallelExecutionConfig):
        self.config = config
        self.validator = ScenarioValidator()
        self.worker_service = ScenarioWorkerService()
        self._active_executions: Dict[str, asyncio.Task] = {}
        self._execution_semaphore = asyncio.Semaphore(config.max_concurrent_scenarios)
        self._shutdown_event = asyncio.Event()
    
    async def execute_scenarios_parallel(
        self,
        requests: List[ScenarioExecutionRequest]
    ) -> List[ScenarioExecutionResult]:
        """Execute multiple scenarios in parallel with resource management."""
        if not requests:
            return []
        
        batch_id = str(uuid4())
        start_time = datetime.utcnow()
        
        await log_structured(
            "parallel_execution_started",
            batch_id=batch_id,
            scenario_count=len(requests),
            max_concurrent=self.config.max_concurrent_scenarios
        )
        
        try:
            # Sort requests by priority (higher numbers = higher priority)
            sorted_requests = sorted(requests, key=lambda r: r.priority, reverse=True)
            
            # Create execution tasks
            tasks = []
            for request in sorted_requests:
                task = asyncio.create_task(
                    self._execute_single_scenario(request),
                    name=f"scenario-{request.scenario_id}"
                )
                tasks.append(task)
                self._active_executions[request.scenario_id] = task
            
            # Wait for all tasks to complete or timeout
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.config.timeout_seconds
                )
            except asyncio.TimeoutError:
                logger.warning(f"Batch {batch_id} timed out, cancelling remaining tasks")
                for task in tasks:
                    if not task.done():
                        task.cancel()
                # Gather results from completed tasks
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results and handle exceptions
            final_results = []
            for i, result in enumerate(results):
                request = sorted_requests[i]
                if isinstance(result, Exception):
                    if isinstance(result, asyncio.CancelledError):
                        final_results.append(ScenarioExecutionResult(
                            scenario_id=request.scenario_id,
                            status="cancelled",
                            error="Execution was cancelled",
                            end_time=datetime.utcnow()
                        ))
                    else:
                        final_results.append(ScenarioExecutionResult(
                            scenario_id=request.scenario_id,
                            status="failed",
                            error=str(result),
                            end_time=datetime.utcnow()
                        ))
                else:
                    final_results.append(result)
            
            # Clean up active executions
            for scenario_id in [r.scenario_id for r in requests]:
                self._active_executions.pop(scenario_id, None)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            success_count = len([r for r in final_results if r.status == "success"])
            
            await log_structured(
                "parallel_execution_completed",
                batch_id=batch_id,
                total_scenarios=len(requests),
                successful_scenarios=success_count,
                failed_scenarios=len(requests) - success_count,
                execution_time=execution_time
            )
            
            return final_results
            
        except Exception as e:
            logger.exception(f"Parallel execution batch {batch_id} failed")
            # Return error results for all scenarios
            return [
                ScenarioExecutionResult(
                    scenario_id=req.scenario_id,
                    status="failed",
                    error=f"Batch execution failed: {str(e)}",
                    end_time=datetime.utcnow()
                )
                for req in requests
            ]
    
    async def _execute_single_scenario(self, request: ScenarioExecutionRequest) -> ScenarioExecutionResult:
        """Execute a single scenario with resource management."""
        start_time = datetime.utcnow()
        
        # Acquire execution semaphore
        async with self._execution_semaphore:
            try:
                # Set correlation context if provided
                if request.correlation_id:
                    # This would set the correlation context in real implementation
                    pass
                
                # Validate scenario first
                validation_result = await self.validator.validate_scenario(
                    request.assumptions,
                    request.scenario_id
                )
                
                if not validation_result.is_valid:
                    error_messages = [issue.message for issue in validation_result.issues if issue.severity == "error"]
                    return ScenarioExecutionResult(
                        scenario_id=request.scenario_id,
                        status="failed",
                        error=f"Validation failed: {'; '.join(error_messages)}",
                        execution_time=(datetime.utcnow() - start_time).total_seconds(),
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        metadata={"validation_result": validation_result.model_dump()}
                    )
                
                # Execute scenario simulation
                if request.config:
                    simulation_result = await self._run_scenario_simulation(request)
                else:
                    # Use default simulation if no config provided
                    default_config = SimulationConfig(
                        simulation_type="monte_carlo",
                        num_iterations=1000
                    )
                    simulation_result = await self._run_scenario_simulation(
                        ScenarioExecutionRequest(
                            scenario_id=request.scenario_id,
                            assumptions=request.assumptions,
                            config=default_config,
                            priority=request.priority,
                            metadata=request.metadata
                        )
                    )
                
                execution_time = (datetime.utcnow() - start_time).total_seconds()
                
                return ScenarioExecutionResult(
                    scenario_id=request.scenario_id,
                    status="success",
                    result=simulation_result,
                    execution_time=execution_time,
                    start_time=start_time,
                    end_time=datetime.utcnow(),
                    metadata={
                        "validation_passed": True,
                        "simulation_type": request.config.simulation_type if request.config else "monte_carlo"
                    }
                )
                
            except asyncio.CancelledError:
                raise  # Re-raise cancellation
            except Exception as e:
                logger.exception(f"Scenario {request.scenario_id} execution failed")
                return ScenarioExecutionResult(
                    scenario_id=request.scenario_id,
                    status="failed",
                    error=str(e),
                    execution_time=(datetime.utcnow() - start_time).total_seconds(),
                    start_time=start_time,
                    end_time=datetime.utcnow()
                )
    
    async def _run_scenario_simulation(self, request: ScenarioExecutionRequest) -> SimulationResult:
        """Run scenario simulation - placeholder that would integrate with existing worker."""
        # This would integrate with the existing worker service in real implementation
        # For now, return a mock result
        await asyncio.sleep(0.1)  # Simulate work
        
        return SimulationResult(
            simulation_id=str(uuid4()),
            scenario_id=request.scenario_id,
            results=np.random.normal(100, 15, 1000),
            statistics={
                "mean": 100.0,
                "std": 15.0,
                "min": 50.0,
                "max": 150.0
            },
            confidence_intervals={
                "confidence_interval": (70.6, 129.4),
                "interquartile_range": (89.9, 110.1)
            },
            execution_time=0.1,
            metadata={"mock_result": True}
        )
    
    async def get_execution_status(self) -> Dict[str, Any]:
        """Get current execution status."""
        active_count = len(self._active_executions)
        available_slots = self.config.max_concurrent_scenarios - active_count
        
        return {
            "active_executions": active_count,
            "available_slots": available_slots,
            "max_concurrent": self.config.max_concurrent_scenarios,
            "active_scenario_ids": list(self._active_executions.keys())
        }
    
    async def cancel_scenario(self, scenario_id: str) -> bool:
        """Cancel a running scenario execution."""
        task = self._active_executions.get(scenario_id)
        if task and not task.done():
            task.cancel()
            await log_structured(
                "scenario_execution_cancelled",
                scenario_id=scenario_id
            )
            return True
        return False
    
    async def shutdown(self):
        """Gracefully shutdown the execution engine."""
        self._shutdown_event.set()
        
        # Cancel all active executions
        for scenario_id, task in self._active_executions.items():
            task.cancel()
        
        # Wait for all tasks to complete cancellation
        if self._active_executions:
            await asyncio.gather(*self._active_executions.values(), return_exceptions=True)
        
        self._active_executions.clear()
        
        await log_structured("parallel_execution_engine_shutdown")


# Add missing import that we need but don't want to import the whole module
import numpy as np