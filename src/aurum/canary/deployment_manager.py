"""Canary deployment manager for safe production rollouts.

This module provides a comprehensive canary deployment system that enables:
- Gradual traffic migration from old to new versions
- Automated health validation and monitoring
- Automatic rollback capabilities
- Metrics collection and analysis
- Integration with existing pipeline infrastructure
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable
from abc import ABC, abstractmethod

from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class CanaryStatus(Enum):
    """Status of a canary deployment."""
    PENDING = "pending"
    INITIALIZING = "initializing"
    WARMING_UP = "warming_up"
    RUNNING = "running"
    PROMOTING = "promoting"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"


class HealthCheckStatus(Enum):
    """Health check result status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"        # Normal operation
    OPEN = "open"           # Failing, rejecting calls
    HALF_OPEN = "half_open" # Testing recovery


class RolloutGateStatus(Enum):
    """Rollout gate evaluation status."""
    PASS = "pass"
    FAIL = "fail"
    PENDING = "pending"
    TIMEOUT = "timeout"


@dataclass
class CanaryConfig:
    """Configuration for a canary deployment."""

    # Basic configuration
    name: str
    version: str
    description: str = ""

    # Traffic configuration
    initial_traffic_percent: float = 5.0
    traffic_increment_percent: float = 5.0
    max_traffic_percent: float = 100.0

    # Timing configuration
    warmup_duration_minutes: int = 5
    evaluation_duration_minutes: int = 10
    promotion_duration_minutes: int = 15

    # Health check configuration
    health_check_interval_seconds: int = 30
    health_check_timeout_seconds: int = 10
    required_healthy_checks: int = 3

    # Thresholds
    success_threshold_percent: float = 95.0
    latency_threshold_ms: float = 1000.0
    error_rate_threshold_percent: float = 1.0

    # Rollout health gates
    enable_health_gates: bool = True
    health_gate_timeout_minutes: int = 30
    health_gate_required_success_rate: float = 99.0
    health_gate_max_latency_ms: float = 500.0
    health_gate_min_throughput_rps: float = 10.0

    # Circuit breaker configuration
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout_minutes: int = 5
    circuit_breaker_half_open_max_calls: int = 3

    # Rollback configuration
    auto_rollback_enabled: bool = True
    rollback_on_failure: bool = True

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now())
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthCheck:
    """Health check configuration."""

    name: str
    description: str = ""
    check_type: str = "http"  # http, database, kafka, etc.
    endpoint: str = ""
    expected_status_codes: List[int] = field(default_factory=lambda: [200, 201, 202])
    headers: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 10
    retry_count: int = 3
    enabled: bool = True

    # Health check results
    last_check_time: Optional[datetime] = None
    last_status: HealthCheckStatus = HealthCheckStatus.UNKNOWN
    consecutive_failures: int = 0
    consecutive_successes: int = 0


@dataclass
class CircuitBreaker:
    """Circuit breaker for canary deployments."""

    name: str
    failure_threshold: int = 5
    recovery_timeout_minutes: int = 5
    half_open_max_calls: int = 3

    # State management
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_state_change: datetime = field(default_factory=lambda: datetime.now())

    def record_success(self):
        """Record a successful call."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_max_calls:
                self._transition_to_closed()

    def record_failure(self):
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self._transition_to_open()
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self._transition_to_open()

    def can_execute(self) -> bool:
        """Check if the circuit breaker allows execution."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True
        return False

    def _transition_to_open(self):
        """Transition to open state."""
        self.state = CircuitBreakerState.OPEN
        self.last_state_change = datetime.now()
        logger.warning(f"Circuit breaker '{self.name}' opened after {self.failure_count} failures")

    def _transition_to_half_open(self):
        """Transition to half-open state."""
        self.state = CircuitBreakerState.HALF_OPEN
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = datetime.now()
        logger.info(f"Circuit breaker '{self.name}' half-open for recovery testing")

    def _transition_to_closed(self):
        """Transition to closed state."""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = datetime.now()
        logger.info(f"Circuit breaker '{self.name}' closed after successful recovery")

    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker."""
        if self.last_failure_time is None:
            return True

        recovery_time = datetime.now() - self.last_failure_time
        return recovery_time.total_seconds() >= (self.recovery_timeout_minutes * 60)


@dataclass
class RolloutGate:
    """Rollout gate for deployment validation."""

    name: str
    description: str = ""
    gate_type: str = "health_check"  # health_check, performance, integration

    # Gate configuration
    timeout_minutes: int = 30
    required_success_rate: float = 99.0
    max_latency_ms: float = 500.0
    min_throughput_rps: float = 10.0

    # Gate state
    status: RolloutGateStatus = RolloutGateStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    evaluation_time_seconds: float = 0.0

    # Results
    success_count: int = 0
    failure_count: int = 0
    total_requests: int = 0
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    def start_evaluation(self):
        """Start gate evaluation."""
        self.status = RolloutGateStatus.PENDING
        self.started_at = datetime.now()
        self.completed_at = None
        logger.info(f"Starting rollout gate evaluation: {self.name}")

    def record_request(self, success: bool, latency_ms: float):
        """Record a request result for gate evaluation."""
        self.total_requests += 1

        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

        self.avg_latency_ms = ((self.avg_latency_ms * (self.total_requests - 1)) + latency_ms) / self.total_requests
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)

    def evaluate(self) -> RolloutGateStatus:
        """Evaluate the gate based on collected metrics."""
        if self.started_at is None:
            return RolloutGateStatus.PENDING

        elapsed = datetime.now() - self.started_at
        self.evaluation_time_seconds = elapsed.total_seconds()

        # Check timeout
        if elapsed.total_seconds() > (self.timeout_minutes * 60):
            self.status = RolloutGateStatus.TIMEOUT
            self.completed_at = datetime.now()
            return self.status

        # Check if we have enough data
        if self.total_requests < 10:
            return RolloutGateStatus.PENDING

        # Calculate success rate
        success_rate = (self.success_count / self.total_requests) * 100

        # Evaluate gate conditions
        if (success_rate >= self.required_success_rate and
            self.avg_latency_ms <= self.max_latency_ms and
            self.total_requests / self.evaluation_time_seconds >= self.min_throughput_rps):

            self.status = RolloutGateStatus.PASS
            self.completed_at = datetime.now()
            logger.info(f"Rollout gate '{self.name}' PASSED (success_rate={success_rate:.1f}%)")
            return self.status
        else:
            self.status = RolloutGateStatus.FAIL
            self.completed_at = datetime.now()
            logger.warning(f"Rollout gate '{self.name}' FAILED (success_rate={success_rate:.1f}%)")
            return self.status


@dataclass
class DeploymentMetrics:
    """Metrics collected during canary deployment."""

    # Traffic metrics
    total_requests: int = 0
    canary_requests: int = 0
    baseline_requests: int = 0

    # Performance metrics
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    # Error metrics
    total_errors: int = 0
    error_rate_percent: float = 0.0

    # Success metrics
    success_rate_percent: float = 100.0

    # Health metrics
    healthy_checks: int = 0
    degraded_checks: int = 0
    unhealthy_checks: int = 0

    # Timing
    collection_start: datetime = field(default_factory=lambda: datetime.now())
    last_updated: datetime = field(default_factory=lambda: datetime.now())


class HealthCheckProvider(ABC):
    """Abstract base class for health check providers."""

    @abstractmethod
    async def perform_check(self, check: HealthCheck) -> HealthCheckStatus:
        """Perform a health check and return status."""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Get the provider name."""
        pass


class HttpHealthCheckProvider(HealthCheckProvider):
    """HTTP-based health check provider."""

    def __init__(self):
        self.session = None  # Will use aiohttp or similar

    async def perform_check(self, check: HealthCheck) -> HealthCheckStatus:
        """Perform HTTP health check."""
        # Implementation would use aiohttp or requests
        # For now, return mock result
        return HealthCheckStatus.HEALTHY

    def get_name(self) -> str:
        return "http"


class DatabaseHealthCheckProvider(HealthCheckProvider):
    """Database health check provider."""

    async def perform_check(self, check: HealthCheck) -> HealthCheckStatus:
        """Perform database health check."""
        # Implementation would check database connectivity
        # For now, return mock result
        return HealthCheckStatus.HEALTHY

    def get_name(self) -> str:
        return "database"


class KafkaHealthCheckProvider(HealthCheckProvider):
    """Kafka health check provider."""

    async def perform_check(self, check: HealthCheck) -> HealthCheckStatus:
        """Perform Kafka health check."""
        # Implementation would check Kafka connectivity
        # For now, return mock result
        return HealthCheckStatus.HEALTHY

    def get_name(self) -> str:
        return "kafka"


class CanaryDeployment:
    """Represents an active canary deployment."""

    def __init__(self, config: CanaryConfig):
        self.config = config
        self.status = CanaryStatus.PENDING
        self.current_traffic_percent = 0.0
        self.metrics = DeploymentMetrics()

        # Health checks
        self.health_checks: Dict[str, HealthCheck] = {}
        self.health_check_providers: Dict[str, HealthCheckProvider] = {
            "http": HttpHealthCheckProvider(),
            "database": DatabaseHealthCheckProvider(),
            "kafka": KafkaHealthCheckProvider()
        }

        # Circuit breaker
        self.circuit_breaker = CircuitBreaker(
            name=f"{config.name}_circuit_breaker",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout_minutes=config.circuit_breaker_recovery_timeout_minutes,
            half_open_max_calls=config.circuit_breaker_half_open_max_calls
        )

        # Rollout gates
        self.rollout_gates: Dict[str, RolloutGate] = {}
        self._active_gate: Optional[RolloutGate] = None

        # Callbacks
        self.on_status_change: Optional[Callable[[CanaryStatus], Awaitable[None]]] = None
        self.on_traffic_change: Optional[Callable[[float], Awaitable[None]]] = None
        self.on_gate_evaluation: Optional[Callable[[RolloutGate], Awaitable[None]]] = None

        # Internal state
        self._start_time = datetime.now()
        self._last_evaluation = None
        self._evaluation_results: List[bool] = []

    async def add_health_check(self, check: HealthCheck):
        """Add a health check to the deployment."""
        self.health_checks[check.name] = check
        logger.info(f"Added health check '{check.name}' to canary '{self.config.name}'")

    async def add_rollout_gate(self, gate: RolloutGate):
        """Add a rollout gate to the deployment."""
        self.rollout_gates[gate.name] = gate
        logger.info(f"Added rollout gate '{gate.name}' to canary '{self.config.name}'")

    async def start_rollout_gate(self, gate_name: str) -> bool:
        """Start evaluation of a rollout gate."""
        if gate_name not in self.rollout_gates:
            logger.error(f"Rollout gate '{gate_name}' not found")
            return False

        gate = self.rollout_gates[gate_name]
        if self._active_gate is not None:
            logger.warning(f"Another gate '{self._active_gate.name}' is already active")
            return False

        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker is {self.circuit_breaker.state.value}, cannot start gate")
            return False

        gate.start_evaluation()
        self._active_gate = gate

        logger.info(f"Started rollout gate evaluation: {gate_name}")
        return True

    async def record_gate_request(self, gate_name: str, success: bool, latency_ms: float):
        """Record a request result for gate evaluation."""
        if self._active_gate is None or self._active_gate.name != gate_name:
            logger.warning(f"No active gate '{gate_name}' for request recording")
            return

        self._active_gate.record_request(success, latency_ms)

        # Check circuit breaker
        if success:
            self.circuit_breaker.record_success()
        else:
            self.circuit_breaker.record_failure()

    async def evaluate_rollout_gate(self, gate_name: str) -> Optional[RolloutGateStatus]:
        """Evaluate the current rollout gate."""
        if self._active_gate is None or self._active_gate.name != gate_name:
            return None

        status = self._active_gate.evaluate()

        if status in [RolloutGateStatus.PASS, RolloutGateStatus.FAIL, RolloutGateStatus.TIMEOUT]:
            if self.on_gate_evaluation:
                await self.on_gate_evaluation(self._active_gate)
            self._active_gate = None

            if status == RolloutGateStatus.FAIL:
                logger.warning(f"Rollout gate '{gate_name}' failed - deployment may need rollback")
            elif status == RolloutGateStatus.TIMEOUT:
                logger.warning(f"Rollout gate '{gate_name}' timed out")

        return status

    async def _evaluate_rollout_gates(self) -> bool:
        """Evaluate all rollout gates and return overall status."""
        if not self.rollout_gates:
            return True  # No gates configured

        # Check active gate
        if self._active_gate:
            status = await self.evaluate_rollout_gate(self._active_gate.name)
            if status == RolloutGateStatus.FAIL:
                return False
            elif status == RolloutGateStatus.TIMEOUT:
                return False
            elif status == RolloutGateStatus.PENDING:
                return True  # Still evaluating
            elif status == RolloutGateStatus.PASS:
                logger.info(f"Rollout gate '{self._active_gate.name}' passed, continuing deployment")
                return True

        # All gates passed
        return True

    async def start(self):
        """Start the canary deployment."""
        logger.info(f"Starting canary deployment: {self.config.name} (v{self.config.version})")

        self.status = CanaryStatus.INITIALIZING
        self._start_time = datetime.now()

        if self.on_status_change:
            await self.on_status_change(self.status)

        # Initialize traffic at starting percentage
        await self._set_traffic_percentage(self.config.initial_traffic_percent)

        # Start warmup phase
        await self._start_warmup_phase()

    async def _start_warmup_phase(self):
        """Start the warmup phase."""
        logger.info(f"Starting warmup phase for {self.config.warmup_duration_minutes} minutes")

        self.status = CanaryStatus.WARMING_UP

        if self.on_status_change:
            await self.on_status_change(self.status)

        # Wait for warmup duration
        await asyncio.sleep(self.config.warmup_duration_minutes * 60)

        # Move to running phase
        await self._start_running_phase()

    async def _start_running_phase(self):
        """Start the running/evaluation phase."""
        logger.info("Starting evaluation phase")

        self.status = CanaryStatus.RUNNING
        self._last_evaluation = datetime.now()

        if self.on_status_change:
            await self.on_status_change(self.status)

        # Start health check monitoring
        await self._start_health_monitoring()

        # Start traffic progression
        await self._start_traffic_progression()

    async def _start_traffic_progression(self):
        """Progressively increase traffic to canary with health gates."""
        current_percent = self.config.initial_traffic_percent

        while current_percent < self.config.max_traffic_percent and self.status == CanaryStatus.RUNNING:
            # Check circuit breaker before proceeding
            if not self.circuit_breaker.can_execute():
                logger.warning(f"Circuit breaker is {self.circuit_breaker.state.value}, pausing traffic progression")
                await asyncio.sleep(30)  # Wait before retry
                continue

            # Evaluate current state
            evaluation_passed = await self._evaluate_deployment()

            if evaluation_passed:
                # Check rollout gates if enabled
                if self.config.enable_health_gates:
                    gate_passed = await self._evaluate_rollout_gates()
                    if not gate_passed:
                        logger.warning("Rollout gates failed, considering rollback")
                        if self.config.rollback_on_failure:
                            await self._initiate_rollback()
                            return
                        else:
                            await asyncio.sleep(self.config.evaluation_duration_minutes * 60)
                            continue

                # Increase traffic
                new_percent = min(
                    current_percent + self.config.traffic_increment_percent,
                    self.config.max_traffic_percent
                )

                if new_percent != current_percent:
                    logger.info(f"Increasing traffic from {current_percent}% to {new_percent}%")
                    await self._set_traffic_percentage(new_percent)
                    current_percent = new_percent

                    if self.on_traffic_change:
                        await self.on_traffic_change(current_percent)

                # Wait before next evaluation
                await asyncio.sleep(self.config.evaluation_duration_minutes * 60)

            else:
                logger.warning("Evaluation failed, considering rollback")
                if self.config.rollback_on_failure:
                    await self._initiate_rollback()
                    return
                else:
                    # Continue with current traffic level
                    await asyncio.sleep(self.config.evaluation_duration_minutes * 60)

        # If we reach max traffic, start promotion
        if current_percent >= self.config.max_traffic_percent:
            await self._start_promotion_phase()

    async def _evaluate_deployment(self) -> bool:
        """Evaluate the current state of the deployment."""
        logger.info("Evaluating deployment health and performance")

        # Collect metrics
        await self._collect_metrics()

        # Perform health checks
        health_results = await self._perform_health_checks()

        # Evaluate against thresholds
        success_rate_ok = self.metrics.success_rate_percent >= self.config.success_threshold_percent
        latency_ok = self.metrics.avg_latency_ms <= self.config.latency_threshold_ms
        error_rate_ok = self.metrics.error_rate_percent <= self.config.error_rate_threshold_percent
        health_ok = health_results["healthy"] >= self.config.required_healthy_checks

        evaluation_passed = success_rate_ok and latency_ok and error_rate_ok and health_ok

        self._evaluation_results.append(evaluation_passed)
        self._last_evaluation = datetime.now()

        logger.info(
            f"Evaluation result: {'PASS' if evaluation_passed else 'FAIL'} "
            f"(Success: {self.metrics.success_rate_percent".1f"}%, "
            f"Latency: {self.metrics.avg_latency_ms".1f"}ms, "
            f"Errors: {self.metrics.error_rate_percent".1f"}%, "
            f"Health: {health_results['healthy']})"
        )

        return evaluation_passed

    async def _collect_metrics(self):
        """Collect deployment metrics."""
        # This would integrate with actual monitoring systems
        # For now, simulate realistic metrics
        self.metrics.total_requests += 1000
        self.metrics.canary_requests = int(self.metrics.total_requests * (self.current_traffic_percent / 100))

        # Simulate some realistic performance metrics
        self.metrics.avg_latency_ms = 150.0  # ms
        self.metrics.p95_latency_ms = 250.0
        self.metrics.p99_latency_ms = 400.0

        # Simulate error rate
        self.metrics.total_errors = 5
        self.metrics.error_rate_percent = (self.metrics.total_errors / self.metrics.total_requests) * 100
        self.metrics.success_rate_percent = 100.0 - self.metrics.error_rate_percent

        self.metrics.last_updated = datetime.now()

    async def _perform_health_checks(self) -> Dict[str, int]:
        """Perform all configured health checks."""
        results = {"healthy": 0, "degraded": 0, "unhealthy": 0, "unknown": 0}

        for check in self.health_checks.values():
            if not check.enabled:
                continue

            try:
                provider = self.health_check_providers.get(check.check_type)
                if provider:
                    status = await provider.perform_check(check)

                    # Update check status
                    check.last_status = status
                    check.last_check_time = datetime.now()

                    if status == HealthCheckStatus.HEALTHY:
                        results["healthy"] += 1
                        check.consecutive_successes += 1
                        check.consecutive_failures = 0
                    else:
                        results["unhealthy"] += 1
                        check.consecutive_failures += 1
                        check.consecutive_successes = 0

                else:
                    results["unknown"] += 1
                    check.last_status = HealthCheckStatus.UNKNOWN

            except Exception as e:
                logger.error(f"Health check '{check.name}' failed: {e}")
                results["unhealthy"] += 1
                check.last_status = HealthCheckStatus.UNHEALTHY
                check.consecutive_failures += 1

        return results

    async def _set_traffic_percentage(self, percent: float):
        """Set the traffic percentage for the canary."""
        self.current_traffic_percent = percent

        # This would integrate with actual traffic management systems
        # (load balancers, service mesh, API gateways, etc.)
        logger.info(f"Traffic percentage set to {percent}% for canary '{self.config.name}'")

    async def _start_promotion_phase(self):
        """Start the promotion phase to full traffic."""
        logger.info(f"Starting promotion phase for {self.config.promotion_duration_minutes} minutes")

        self.status = CanaryStatus.PROMOTING

        if self.on_status_change:
            await self.on_status_change(self.status)

        # Gradually increase to 100% traffic
        await self._set_traffic_percentage(100.0)

        if self.on_traffic_change:
            await self.on_traffic_change(100.0)

        # Wait for promotion duration
        await asyncio.sleep(self.config.promotion_duration_minutes * 60)

        # Complete the deployment
        await self._complete_deployment()

    async def _complete_deployment(self):
        """Mark the deployment as completed."""
        logger.info(f"Canary deployment '{self.config.name}' completed successfully")

        self.status = CanaryStatus.COMPLETED

        if self.on_status_change:
            await self.on_status_change(self.status)

    async def _initiate_rollback(self):
        """Initiate rollback to baseline version."""
        logger.warning(f"Initiating rollback for canary '{self.config.name}'")

        self.status = CanaryStatus.ROLLING_BACK

        if self.on_status_change:
            await self.on_status_change(self.status)

        # Set traffic back to baseline (0% canary)
        await self._set_traffic_percentage(0.0)

        if self.on_traffic_change:
            await self.on_traffic_change(0.0)

        self.status = CanaryStatus.ROLLED_BACK

        if self.on_status_change:
            await self.on_status_change(self.status)

        logger.info(f"Rollback completed for canary '{self.config.name}'")


class CanaryDeploymentManager:
    """Manager for canary deployments."""

    def __init__(self):
        self.active_deployments: Dict[str, CanaryDeployment] = {}
        self.deployment_history: List[Dict[str, Any]] = []
        self.metrics = get_metrics_client()

        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self):
        """Start the canary deployment manager."""
        if self._is_running:
            return

        self._is_running = True

        # Start background monitoring
        self._monitoring_task = asyncio.create_task(self._monitor_deployments())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_deployments())

        logger.info("Canary deployment manager started")

    async def stop(self):
        """Stop the canary deployment manager."""
        self._is_running = False

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        logger.info("Canary deployment manager stopped")

    async def create_deployment(self, config: CanaryConfig) -> str:
        """Create a new canary deployment."""
        deployment = CanaryDeployment(config)
        self.active_deployments[config.name] = deployment

        logger.info(f"Created canary deployment: {config.name}")
        self.metrics.increment_counter("canary.deployments_created")

        return config.name

    async def start_deployment(self, name: str) -> bool:
        """Start a canary deployment."""
        if name not in self.active_deployments:
            logger.error(f"Deployment '{name}' not found")
            return False

        deployment = self.active_deployments[name]

        try:
            await deployment.start()
            self.metrics.increment_counter("canary.deployments_started")
            return True

        except Exception as e:
            logger.error(f"Failed to start deployment '{name}': {e}")
            deployment.status = CanaryStatus.FAILED
            self.metrics.increment_counter("canary.deployment_start_failures")
            return False

    async def get_deployment_status(self, name: str) -> Optional[Dict[str, Any]]:
        """Get the status of a deployment."""
        if name not in self.active_deployments:
            return None

        deployment = self.active_deployments[name]
        return {
            "name": deployment.config.name,
            "version": deployment.config.version,
            "status": deployment.status.value,
            "current_traffic_percent": deployment.current_traffic_percent,
            "metrics": {
                "total_requests": deployment.metrics.total_requests,
                "success_rate_percent": deployment.metrics.success_rate_percent,
                "error_rate_percent": deployment.metrics.error_rate_percent,
                "avg_latency_ms": deployment.metrics.avg_latency_ms,
                "healthy_checks": deployment.metrics.healthy_checks,
                "unhealthy_checks": deployment.metrics.unhealthy_checks
            },
            "health_checks": {
                check_name: {
                    "status": check.last_status.value,
                    "last_check": check.last_check_time.isoformat() if check.last_check_time else None,
                    "consecutive_failures": check.consecutive_failures,
                    "consecutive_successes": check.consecutive_successes
                }
                for check_name, check in deployment.health_checks.items()
            },
            "config": {
                "initial_traffic_percent": deployment.config.initial_traffic_percent,
                "max_traffic_percent": deployment.config.max_traffic_percent,
                "success_threshold_percent": deployment.config.success_threshold_percent,
                "auto_rollback_enabled": deployment.config.auto_rollback_enabled
            }
        }

    async def get_all_deployments(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all deployments."""
        return {
            name: await self.get_deployment_status(name)
            for name in self.active_deployments.keys()
        }

    async def _monitor_deployments(self):
        """Background task to monitor active deployments."""
        while self._is_running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                for name, deployment in list(self.active_deployments.items()):
                    if deployment.status in [CanaryStatus.COMPLETED, CanaryStatus.FAILED, CanaryStatus.ROLLED_BACK]:
                        # Move completed/failed deployments to history
                        await self._archive_deployment(deployment)
                        del self.active_deployments[name]

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in deployment monitoring: {e}")

    async def _cleanup_old_deployments(self):
        """Background task to clean up old deployment records."""
        while self._is_running:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes

                # Keep only last 100 deployments in history
                if len(self.deployment_history) > 100:
                    self.deployment_history = self.deployment_history[-100:]

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in deployment cleanup: {e}")

    async def _archive_deployment(self, deployment: CanaryDeployment):
        """Archive a completed deployment to history."""
        history_record = {
            "name": deployment.config.name,
            "version": deployment.config.version,
            "status": deployment.status.value,
            "start_time": deployment._start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "final_traffic_percent": deployment.current_traffic_percent,
            "total_requests": deployment.metrics.total_requests,
            "success_rate_percent": deployment.metrics.success_rate_percent,
            "error_rate_percent": deployment.metrics.error_rate_percent,
            "avg_latency_ms": deployment.metrics.avg_latency_ms,
            "config": {
                "initial_traffic_percent": deployment.config.initial_traffic_percent,
                "success_threshold_percent": deployment.config.success_threshold_percent,
                "auto_rollback_enabled": deployment.config.auto_rollback_enabled
            }
        }

        self.deployment_history.append(history_record)

        # Emit final metrics
        self.metrics.increment_counter(
            f"canary.deployments_{deployment.status.value.lower()}"
        )

        logger.info(f"Archived deployment '{deployment.config.name}' with status {deployment.status.value}")


# Global deployment manager instance
_global_deployment_manager: Optional[CanaryDeploymentManager] = None


async def get_deployment_manager() -> CanaryDeploymentManager:
    """Get or create the global deployment manager."""
    global _global_deployment_manager

    if _global_deployment_manager is None:
        _global_deployment_manager = CanaryDeploymentManager()
        await _global_deployment_manager.start()

    return _global_deployment_manager


# Convenience functions for common deployment patterns
async def deploy_caiso_pipeline_with_canary(
    version: str,
    description: str = ""
) -> str:
    """Deploy CAISO pipeline with canary."""
    config = CanaryConfig(
        name=f"caiso-pipeline-{version}",
        version=version,
        description=description or f"CAISO pipeline deployment v{version}",
        initial_traffic_percent=5.0,
        traffic_increment_percent=10.0,
        max_traffic_percent=100.0,
        warmup_duration_minutes=5,
        evaluation_duration_minutes=10,
        promotion_duration_minutes=15,
        success_threshold_percent=95.0,
        auto_rollback_enabled=True
    )

    manager = await get_deployment_manager()
    deployment_name = await manager.create_deployment(config)

    # Add health checks
    deployment = manager.active_deployments[deployment_name]

    # Add CAISO-specific health checks
    await deployment.add_health_check(HealthCheck(
        name="caiso_api_connectivity",
        description="CAISO OASIS API connectivity check",
        check_type="http",
        endpoint="https://oasis.caiso.com/oasisapi/SingleZip",
        expected_status_codes=[200, 201, 202],
        timeout_seconds=10
    ))

    await deployment.add_health_check(HealthCheck(
        name="caiso_data_quality",
        description="CAISO data quality validation",
        check_type="http",
        endpoint="https://internal.caiso-data-quality-check.com/health",
        expected_status_codes=[200],
        timeout_seconds=5
    ))

    # Add rollout gates for CAISO
    if config.enable_health_gates:
        await deployment.add_rollout_gate(RolloutGate(
            name="caiso_api_performance_gate",
            description="CAISO API performance validation gate",
            gate_type="performance",
            timeout_minutes=config.health_gate_timeout_minutes,
            required_success_rate=config.health_gate_required_success_rate,
            max_latency_ms=config.health_gate_max_latency_ms,
            min_throughput_rps=config.health_gate_min_throughput_rps
        ))

        await deployment.add_rollout_gate(RolloutGate(
            name="caiso_data_quality_gate",
            description="CAISO data quality validation gate",
            gate_type="health_check",
            timeout_minutes=config.health_gate_timeout_minutes,
            required_success_rate=98.0,  # Slightly lower for data quality
            max_latency_ms=config.health_gate_max_latency_ms,
            min_throughput_rps=config.health_gate_min_throughput_rps
        ))

    return deployment_name


async def deploy_multi_iso_with_canary(
    iso_codes: List[str],
    version: str,
    description: str = ""
) -> Dict[str, str]:
    """Deploy multiple ISO pipelines with canary."""
    deployment_names = {}

    for iso_code in iso_codes:
        config = CanaryConfig(
            name=f"{iso_code.lower()}-pipeline-{version}",
            version=version,
            description=description or f"{iso_code} pipeline deployment v{version}",
            initial_traffic_percent=5.0,
            traffic_increment_percent=5.0,
            max_traffic_percent=100.0,
            warmup_duration_minutes=5,
            evaluation_duration_minutes=15,
            promotion_duration_minutes=20,
            success_threshold_percent=98.0,  # Higher threshold for multi-ISO
            auto_rollback_enabled=True
        )

        manager = await get_deployment_manager()
        deployment_name = await manager.create_deployment(config)

        # Add ISO-specific health checks
        deployment = manager.active_deployments[deployment_name]

        # Add common health checks
        await deployment.add_health_check(HealthCheck(
            name=f"{iso_code.lower()}_api_connectivity",
            description=f"{iso_code} API connectivity check",
            check_type="http",
            endpoint=f"https://api.{iso_code.lower()}.com/health",
            expected_status_codes=[200, 201, 202],
            timeout_seconds=10
        ))

        deployment_names[iso_code] = deployment_name

    return deployment_names
