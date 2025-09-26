"""Canary deployment orchestrator.

This module provides the main orchestration layer for canary deployments,
coordinating deployment management, traffic routing, and health monitoring.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable

from .deployment_manager import CanaryDeploymentManager, CanaryConfig, get_deployment_manager
from .traffic_manager import TrafficManager, get_traffic_manager, TrafficEndpoint
from .health_monitor import get_health_monitor
from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class DeploymentDecision(Enum):
    """Decisions that can be made about a deployment."""
    CONTINUE = "continue"
    PROMOTE = "promote"
    ROLLBACK = "rollback"
    PAUSE = "pause"
    STOP = "stop"


@dataclass
class DeploymentContext:
    """Context for deployment decision making."""

    deployment_name: str
    start_time: datetime = field(default_factory=lambda: datetime.now())

    # Traffic information
    current_traffic_percent: float = 0.0
    target_traffic_percent: float = 100.0

    # Performance thresholds
    max_latency_ms: float = 1000.0
    max_error_rate_percent: float = 1.0
    min_success_rate_percent: float = 95.0

    # Health requirements
    min_healthy_endpoints: int = 1
    max_unhealthy_endpoints: int = 0

    # Decision history
    decisions: List[Dict[str, Any]] = field(default_factory=list)
    last_decision_time: Optional[datetime] = None


class CanaryDeploymentOrchestrator:
    """Main orchestrator for canary deployments."""

    def __init__(self):
        self.deployment_manager = None
        self.traffic_manager = None
        self.health_monitor = None
        self.metrics = get_metrics_client()

        # Active deployments
        self.active_orchestrations: Dict[str, DeploymentContext] = {}

        # Decision policies
        self.decision_policies: Dict[str, Callable] = {
            "performance_based": self._performance_based_decision,
            "health_based": self._health_based_decision,
            "composite": self._composite_decision
        }

        # Configuration
        self.default_decision_policy = "composite"
        self.decision_interval_minutes = 5
        self.max_deployment_time_hours = 8

        # Background tasks
        self._orchestration_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self):
        """Start the orchestrator."""
        if self._is_running:
            return

        # Initialize components
        self.deployment_manager = await get_deployment_manager()
        self.traffic_manager = await get_traffic_manager()
        self.health_monitor = await get_health_monitor()

        self._is_running = True
        self._orchestration_task = asyncio.create_task(self._run_orchestration_loop())

        logger.info("Canary deployment orchestrator started")

    async def stop(self):
        """Stop the orchestrator."""
        self._is_running = False

        if self._orchestration_task:
            self._orchestration_task.cancel()
            try:
                await self._orchestration_task
            except asyncio.CancelledError:
                pass

        logger.info("Canary deployment orchestrator stopped")

    async def deploy_with_canary(
        self,
        config: CanaryConfig,
        baseline_endpoints: List[TrafficEndpoint],
        canary_endpoints: List[TrafficEndpoint],
        decision_policy: str = None
    ) -> str:
        """Deploy an application using canary strategy."""
        policy = decision_policy or self.default_decision_policy

        if policy not in self.decision_policies:
            raise ValueError(f"Unknown decision policy: {policy}")

        # Create deployment
        deployment_name = await self.deployment_manager.create_deployment(config)

        # Set up traffic management
        await self._setup_traffic_management(
            deployment_name,
            config,
            baseline_endpoints,
            canary_endpoints
        )

        # Set up health monitoring
        await self._setup_health_monitoring(deployment_name, config)

        # Create deployment context
        context = DeploymentContext(
            deployment_name=deployment_name,
            current_traffic_percent=config.initial_traffic_percent,
            target_traffic_percent=config.max_traffic_percent
        )

        self.active_orchestrations[deployment_name] = context

        # Start the deployment
        await self.deployment_manager.start_deployment(deployment_name)

        logger.info(f"Started canary deployment: {deployment_name}")

        return deployment_name

    async def _setup_traffic_management(
        self,
        deployment_name: str,
        config: CanaryConfig,
        baseline_endpoints: List[TrafficEndpoint],
        canary_endpoints: List[TrafficEndpoint]
    ):
        """Set up traffic management for the deployment."""
        # Add endpoints
        for endpoint in baseline_endpoints + canary_endpoints:
            await self.traffic_manager.add_endpoint(endpoint)

        # Create routing rule
        from .traffic_manager import RoutingRule, TrafficStrategy

        rule = RoutingRule(
            name=f"{deployment_name}_traffic",
            strategy=TrafficStrategy.WEIGHTED,
            baseline_endpoints=baseline_endpoints,
            canary_endpoints=canary_endpoints,
            canary_traffic_percent=config.initial_traffic_percent
        )

        await self.traffic_manager.add_routing_rule(rule)

    async def _setup_health_monitoring(self, deployment_name: str, config: CanaryConfig):
        """Set up health monitoring for the deployment."""
        # Set up endpoints for monitoring
        endpoints = {
            "canary_api": "http://canary-service:8080/health",
            "baseline_api": "http://baseline-service:8080/health",
            "database": "postgresql://prod-db:5432/health",
            "kafka": "kafka://prod-kafka:9092/health"
        }

        health_checks = [
            {
                "name": "api_connectivity",
                "provider": "http",
                "target": "http://canary-service:8080/health"
            },
            {
                "name": "database_connectivity",
                "provider": "database",
                "target": "postgresql://prod-db:5432/aurum"
            },
            {
                "name": "kafka_connectivity",
                "provider": "kafka",
                "target": "kafka://prod-kafka:9092"
            }
        ]

        await self.health_monitor.add_deployment_monitoring(
            deployment_name,
            endpoints,
            health_checks
        )

    async def get_deployment_status(self, deployment_name: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive status of a deployment."""
        if deployment_name not in self.active_orchestrations:
            return None

        context = self.active_orchestrations[deployment_name]

        # Get deployment status
        deployment_status = await self.deployment_manager.get_deployment_status(deployment_name)

        # Get traffic stats
        traffic_stats = await self.traffic_manager.get_traffic_stats(f"{deployment_name}_traffic")

        # Get health status
        health_report = await self.health_monitor.get_deployment_health(deployment_name)

        return {
            "deployment_name": deployment_name,
            "status": deployment_status.get("status", "unknown"),
            "current_traffic_percent": context.current_traffic_percent,
            "target_traffic_percent": context.target_traffic_percent,
            "start_time": context.start_time.isoformat(),
            "duration_minutes": (datetime.now() - context.start_time).total_seconds() / 60,
            "deployment_metrics": deployment_status.get("metrics", {}),
            "traffic_stats": traffic_stats,
            "health_status": health_report.overall_status.value if health_report else "unknown",
            "health_confidence": health_report.confidence_score if health_report else 0.0,
            "last_decision": context.last_decision_time.isoformat() if context.last_decision_time else None,
            "decisions_made": len(context.decisions)
        }

    async def make_deployment_decision(self, deployment_name: str) -> DeploymentDecision:
        """Make a decision about the deployment based on current state."""
        if deployment_name not in self.active_orchestrations:
            return DeploymentDecision.STOP

        context = self.active_orchestrations[deployment_name]
        policy = self.decision_policies[self.default_decision_policy]

        # Check if deployment has exceeded max time
        if (datetime.now() - context.start_time) > timedelta(hours=self.max_deployment_time_hours):
            logger.warning(f"Deployment {deployment_name} has exceeded maximum time")
            return DeploymentDecision.ROLLBACK

        try:
            decision = await policy(context)

            # Record the decision
            context.decisions.append({
                "decision": decision.value,
                "timestamp": datetime.now(),
                "reason": "Policy evaluation"
            })
            context.last_decision_time = datetime.now()

            logger.info(f"Deployment decision for {deployment_name}: {decision.value}")
            return decision

        except Exception as e:
            logger.error(f"Error making deployment decision for {deployment_name}: {e}")
            return DeploymentDecision.PAUSE

    async def _performance_based_decision(self, context: DeploymentContext) -> DeploymentDecision:
        """Make decision based on performance metrics."""
        # Get current metrics
        status = await self.get_deployment_status(context.deployment_name)

        if not status:
            return DeploymentDecision.PAUSE

        metrics = status.get("deployment_metrics", {})

        # Check performance thresholds
        error_rate = metrics.get("error_rate_percent", 0)
        success_rate = metrics.get("success_rate_percent", 100)
        avg_latency = metrics.get("avg_latency_ms", 0)

        if error_rate > context.max_error_rate_percent:
            logger.warning(
                f"High error rate detected: {error_rate:.1f}% > {context.max_error_rate_percent}%"
            )
            return DeploymentDecision.ROLLBACK

        if avg_latency > context.max_latency_ms:
            logger.warning(
                f"High latency detected: {avg_latency:.1f}ms > {context.max_latency_ms}ms"
            )
            return DeploymentDecision.ROLLBACK

        if success_rate < context.min_success_rate_percent:
            logger.warning(
                f"Low success rate detected: {success_rate:.1f}% < {context.min_success_rate_percent}%"
            )
            return DeploymentDecision.ROLLBACK

        # If current traffic is at target and metrics are good, promote
        if (context.current_traffic_percent >= context.target_traffic_percent and
            error_rate <= context.max_error_rate_percent * 0.5 and
            success_rate >= context.min_success_rate_percent and
            avg_latency <= context.max_latency_ms * 0.8):
            return DeploymentDecision.PROMOTE

        return DeploymentDecision.CONTINUE

    async def _health_based_decision(self, context: DeploymentContext) -> DeploymentDecision:
        """Make decision based on health checks."""
        health_report = await self.health_monitor.get_deployment_health(context.deployment_name)

        if not health_report:
            return DeploymentDecision.PAUSE

        # Check overall health
        if health_report.overall_status.name == "CRITICAL":
            logger.error("Critical health issues detected")
            return DeploymentDecision.ROLLBACK

        if health_report.overall_status.name == "DEGRADED":
            logger.warning("Health degraded, monitoring closely")
            # Continue but don't promote
            return DeploymentDecision.CONTINUE

        if health_report.overall_status.name == "HEALTHY":
            # Check if we can promote
            if context.current_traffic_percent >= context.target_traffic_percent:
                return DeploymentDecision.PROMOTE
            return DeploymentDecision.CONTINUE

        return DeploymentDecision.CONTINUE

    async def _composite_decision(self, context: DeploymentContext) -> DeploymentDecision:
        """Make decision based on combined performance and health metrics."""
        # First check health - if unhealthy, rollback immediately
        health_decision = await self._health_based_decision(context)

        if health_decision == DeploymentDecision.ROLLBACK:
            return DeploymentDecision.ROLLBACK

        if health_decision == DeploymentDecision.PAUSE:
            return DeploymentDecision.PAUSE

        # If healthy, use performance-based decision
        performance_decision = await self._performance_based_decision(context)

        # Combine decisions - health takes precedence
        if health_decision == DeploymentDecision.PROMOTE:
            return DeploymentDecision.PROMOTE
        elif performance_decision == DeploymentDecision.ROLLBACK:
            return DeploymentDecision.ROLLBACK
        else:
            return performance_decision

    async def execute_decision(
        self,
        deployment_name: str,
        decision: DeploymentDecision
    ) -> bool:
        """Execute a deployment decision."""
        logger.info(f"Executing decision {decision.value} for deployment {deployment_name}")

        try:
            if decision == DeploymentDecision.PROMOTE:
                return await self._promote_deployment(deployment_name)
            elif decision == DeploymentDecision.ROLLBACK:
                return await self._rollback_deployment(deployment_name)
            elif decision == DeploymentDecision.PAUSE:
                return await self._pause_deployment(deployment_name)
            elif decision == DeploymentDecision.STOP:
                return await self._stop_deployment(deployment_name)
            else:
                logger.info(f"No action needed for decision: {decision.value}")
                return True

        except Exception as e:
            logger.error(f"Error executing decision {decision.value} for {deployment_name}: {e}")
            return False

    async def _promote_deployment(self, deployment_name: str) -> bool:
        """Promote deployment to full traffic."""
        context = self.active_orchestrations.get(deployment_name)
        if not context:
            return False

        # Increase traffic to 100%
        await self.traffic_manager.update_traffic_percentage(
            f"{deployment_name}_traffic",
            100.0
        )

        context.current_traffic_percent = 100.0

        logger.info(f"Promoted deployment {deployment_name} to 100% traffic")
        return True

    async def _rollback_deployment(self, deployment_name: str) -> bool:
        """Rollback deployment."""
        context = self.active_orchestrations.get(deployment_name)
        if not context:
            return False

        # Reduce traffic to 0% (rollback to baseline)
        await self.traffic_manager.update_traffic_percentage(
            f"{deployment_name}_traffic",
            0.0
        )

        context.current_traffic_percent = 0.0

        logger.warning(f"Rolled back deployment {deployment_name}")
        return True

    async def _pause_deployment(self, deployment_name: str) -> bool:
        """Pause a deployment."""
        # In a pause scenario, we might stop increasing traffic but keep current level
        logger.info(f"Paused deployment {deployment_name}")
        return True

    async def _stop_deployment(self, deployment_name: str) -> bool:
        """Stop a deployment."""
        if deployment_name in self.active_orchestrations:
            del self.active_orchestrations[deployment_name]

        logger.info(f"Stopped deployment {deployment_name}")
        return True

    async def _run_orchestration_loop(self):
        """Main orchestration loop."""
        while self._is_running:
            try:
                await asyncio.sleep(self.decision_interval_minutes * 60)

                # Process all active orchestrations
                for deployment_name in list(self.active_orchestrations.keys()):
                    try:
                        decision = await self.make_deployment_decision(deployment_name)

                        if decision != DeploymentDecision.CONTINUE:
                            await self.execute_decision(deployment_name, decision)

                            # Remove completed deployments
                            if decision in [DeploymentDecision.PROMOTE, DeploymentDecision.ROLLBACK]:
                                if deployment_name in self.active_orchestrations:
                                    del self.active_orchestrations[deployment_name]

                    except Exception as e:
                        logger.error(f"Error orchestrating deployment {deployment_name}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in orchestration loop: {e}")


# Global orchestrator instance
_global_orchestrator: Optional[CanaryDeploymentOrchestrator] = None


async def get_canary_orchestrator() -> CanaryDeploymentOrchestrator:
    """Get or create the global canary orchestrator."""
    global _global_orchestrator

    if _global_orchestrator is None:
        _global_orchestrator = CanaryDeploymentOrchestrator()
        await _global_orchestrator.start()

    return _global_orchestrator


# Convenience functions for common deployment patterns
async def deploy_caiso_with_canary(
    version: str,
    baseline_endpoints: List[TrafficEndpoint],
    canary_endpoints: List[TrafficEndpoint],
    description: str = ""
) -> str:
    """Deploy CAISO with canary strategy."""
    orchestrator = await get_canary_orchestrator()

    config = CanaryConfig(
        name=f"caiso-deployment-{version}",
        version=version,
        description=description or f"CAISO deployment v{version}",
        initial_traffic_percent=5.0,
        traffic_increment_percent=10.0,
        max_traffic_percent=100.0,
        warmup_duration_minutes=5,
        evaluation_duration_minutes=10,
        promotion_duration_minutes=15,
        success_threshold_percent=95.0,
        auto_rollback_enabled=True
    )

    return await orchestrator.deploy_with_canary(
        config,
        baseline_endpoints,
        canary_endpoints,
        "composite"
    )


async def deploy_multi_iso_with_canary(
    iso_codes: List[str],
    version: str,
    baseline_endpoints_dict: Dict[str, List[TrafficEndpoint]],
    canary_endpoints_dict: Dict[str, List[TrafficEndpoint]],
    description: str = ""
) -> Dict[str, str]:
    """Deploy multiple ISOs with coordinated canary strategy."""
    orchestrator = await get_canary_orchestrator()

    deployment_names = {}

    for iso_code in iso_codes:
        baseline_endpoints = baseline_endpoints_dict.get(iso_code, [])
        canary_endpoints = canary_endpoints_dict.get(iso_code, [])

        config = CanaryConfig(
            name=f"{iso_code.lower()}-deployment-{version}",
            version=version,
            description=description or f"{iso_code} deployment v{version}",
            initial_traffic_percent=5.0,
            traffic_increment_percent=5.0,
            max_traffic_percent=100.0,
            warmup_duration_minutes=5,
            evaluation_duration_minutes=15,
            promotion_duration_minutes=20,
            success_threshold_percent=98.0,  # Higher threshold for multi-ISO
            auto_rollback_enabled=True
        )

        deployment_name = await orchestrator.deploy_with_canary(
            config,
            baseline_endpoints,
            canary_endpoints,
            "composite"
        )

        deployment_names[iso_code] = deployment_name

    return deployment_names


async def get_deployment_status(deployment_name: str) -> Optional[Dict[str, Any]]:
    """Get comprehensive deployment status."""
    orchestrator = await get_canary_orchestrator()
    return await orchestrator.get_deployment_status(deployment_name)
