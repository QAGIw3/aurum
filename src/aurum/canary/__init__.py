"""Canary deployment system for safe production rollouts.

This module provides a comprehensive canary deployment system that enables:
- Gradual traffic migration from baseline to canary versions
- Automated health validation and performance monitoring
- Intelligent decision-making for promotion or rollback
- Traffic splitting and routing capabilities
- Integration with existing pipeline infrastructure

Key Components:
- CanaryDeploymentOrchestrator: Main orchestration layer
- CanaryDeploymentManager: Individual deployment lifecycle management
- TrafficManager: Traffic splitting and routing
- HealthMonitor: Health checks and performance monitoring

Usage:
    from aurum.canary import deploy_caiso_with_canary, get_deployment_status

    # Deploy CAISO with canary strategy
    deployment_name = await deploy_caiso_with_canary(
        version="v2.1.0",
        baseline_endpoints=[...],
        canary_endpoints=[...]
    )

    # Monitor deployment
    status = await get_deployment_status(deployment_name)
    print(f"Deployment status: {status['status']}")
"""

from .orchestrator import (
    CanaryDeploymentOrchestrator,
    get_canary_orchestrator,
    deploy_caiso_with_canary,
    deploy_multi_iso_with_canary,
    get_deployment_status,
    DeploymentContext,
    DeploymentDecision
)

from .deployment_manager import (
    CanaryDeploymentManager,
    CanaryConfig,
    CanaryDeployment,
    get_deployment_manager,
    CanaryStatus,
    HealthCheckStatus
)

from .traffic_manager import (
    TrafficManager,
    get_traffic_manager,
    TrafficEndpoint,
    TrafficStrategy,
    RoutingRule,
    setup_caiso_canary_traffic,
    update_canary_traffic_percentage,
    get_traffic_distribution
)

from .health_monitor import (
    HealthMonitor,
    get_health_monitor,
    monitor_caiso_deployment,
    get_deployment_health_status,
    add_health_alert_callback,
    HealthStatus,
    AlertSeverity,
    DeploymentHealthReport
)

__all__ = [
    # Orchestrator
    "CanaryDeploymentOrchestrator",
    "get_canary_orchestrator",
    "deploy_caiso_with_canary",
    "deploy_multi_iso_with_canary",
    "get_deployment_status",
    "DeploymentContext",
    "DeploymentDecision",

    # Deployment Manager
    "CanaryDeploymentManager",
    "CanaryConfig",
    "CanaryDeployment",
    "get_deployment_manager",
    "CanaryStatus",
    "HealthCheckStatus",

    # Traffic Manager
    "TrafficManager",
    "get_traffic_manager",
    "TrafficEndpoint",
    "TrafficStrategy",
    "RoutingRule",
    "setup_caiso_canary_traffic",
    "update_canary_traffic_percentage",
    "get_traffic_distribution",

    # Health Monitor
    "HealthMonitor",
    "get_health_monitor",
    "monitor_caiso_deployment",
    "get_deployment_health_status",
    "add_health_alert_callback",
    "HealthStatus",
    "AlertSeverity",
    "DeploymentHealthReport",
]