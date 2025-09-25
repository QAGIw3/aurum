"""Feature-flagged consolidated monitoring and alerting system.

Supports gradual migration from basic monitoring to comprehensive observability:
- Legacy mode: Basic metrics and logging
- Enhanced mode: Comprehensive observability with SLOs and alerting
- Unified mode: Single pane of glass with predictive monitoring

Migration phases:
1. Basic monitoring (current state)
2. Enhanced observability with SLOs
3. Predictive monitoring and alerting
"""

import os
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json

# Feature flags for monitoring consolidation
MONITORING_FEATURE_FLAGS = {
    "use_enhanced_observability": os.getenv("AURUM_USE_ENHANCED_OBSERVABILITY", "true").lower() == "true",
    "monitoring_migration_phase": os.getenv("AURUM_MONITORING_MIGRATION_PHASE", "2"),
    "enable_slo_monitoring": os.getenv("AURUM_ENABLE_SLO_MONITORING", "true").lower() == "true",
    "enable_predictive_alerting": os.getenv("AURUM_ENABLE_PREDICTIVE_ALERTING", "false").lower() == "true",
    "enable_unified_dashboard": os.getenv("AURUM_ENABLE_UNIFIED_DASHBOARD", "false").lower() == "true",
    "alert_webhook_url": os.getenv("AURUM_ALERT_WEBHOOK_URL", ""),
    "grafana_url": os.getenv("AURUM_GRAFANA_URL", "http://grafana.aurum-dev.svc.cluster.local:3000"),
    "prometheus_url": os.getenv("AURUM_PROMETHEUS_URL", "http://prometheus.aurum-dev.svc.cluster.local:9090"),
}

class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class MonitoringComponent(str, Enum):
    """Components that can be monitored."""
    API = "api"
    DATABASE = "database"
    CACHE = "cache"
    WORKER = "worker"
    PIPELINE = "pipeline"
    INFRASTRUCTURE = "infrastructure"

class SLOMetric(str, Enum):
    """SLO metrics to track."""
    API_AVAILABILITY = "api_availability"
    API_LATENCY = "api_latency"
    SCENARIO_SUCCESS = "scenario_success"
    DATA_FRESHNESS = "data_freshness"
    CACHE_HIT_RATE = "cache_hit_rate"
    ERROR_RATE = "error_rate"

@dataclass
class AlertRule:
    """Alert rule configuration."""
    name: str
    description: str
    component: MonitoringComponent
    severity: AlertSeverity
    condition: str
    threshold: float
    evaluation_period: int  # seconds
    cooldown_period: int  # seconds
    channels: List[str] = field(default_factory=list)
    enabled: bool = True

@dataclass
class SLODefinition:
    """Service Level Objective definition."""
    name: str
    metric: SLOMetric
    target: float
    window: int  # seconds
    description: str
    alert_threshold: float = 0.95
    enabled: bool = True

@dataclass
class MonitoringMetrics:
    """Current monitoring metrics."""
    api_requests_total: int = 0
    api_errors_total: int = 0
    api_latency_p95: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    database_connections: int = 0
    active_scenarios: int = 0
    failed_scenarios: int = 0
    data_freshness_minutes: float = 0.0
    system_cpu_usage: float = 0.0
    system_memory_usage: float = 0.0

class ConsolidatedMonitoringManager:
    """Centralized monitoring and alerting system with feature flags."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = MonitoringMetrics()
        self.alert_rules: Dict[str, AlertRule] = {}
        self.slo_definitions: Dict[str, SLODefinition] = {}
        self.active_alerts: Dict[str, Any] = {}
        self.dashboard_config: Dict[str, Any] = {}

        # Initialize based on feature flags
        self._initialize_system()

    def _initialize_system(self):
        """Initialize monitoring system based on feature flags."""
        phase = MONITORING_FEATURE_FLAGS["monitoring_migration_phase"]
        enhanced = MONITORING_FEATURE_FLAGS["use_enhanced_observability"]
        slo_enabled = MONITORING_FEATURE_FLAGS["enable_slo_monitoring"]
        predictive = MONITORING_FEATURE_FLAGS["enable_predictive_alerting"]

        self.logger.info(f"Initializing monitoring system: Phase {phase}, Enhanced: {enhanced}")

        if enhanced:
            self._setup_enhanced_monitoring()
        else:
            self._setup_basic_monitoring()

        if slo_enabled:
            self._setup_slo_monitoring()

        if predictive:
            self._setup_predictive_monitoring()

    def _setup_basic_monitoring(self):
        """Setup basic monitoring configuration."""
        self.logger.info("Setting up basic monitoring")

        # Basic alert rules
        self.alert_rules["api_error_rate"] = AlertRule(
            name="API Error Rate",
            description="API error rate exceeds threshold",
            component=MonitoringComponent.API,
            severity=AlertSeverity.WARNING,
            condition="error_rate > 0.05",
            threshold=0.05,
            evaluation_period=300,
            cooldown_period=600,
            channels=["email", "slack"]
        )

    def _setup_enhanced_monitoring(self):
        """Setup enhanced monitoring with comprehensive observability."""
        self.logger.info("Setting up enhanced monitoring")

        # Enhanced alert rules
        self.alert_rules.update({
            "api_high_latency": AlertRule(
                name="API High Latency",
                description="API P95 latency exceeds 500ms",
                component=MonitoringComponent.API,
                severity=AlertSeverity.WARNING,
                condition="latency_p95 > 500",
                threshold=500,
                evaluation_period=300,
                cooldown_period=600,
                channels=["email", "slack", "grafana"]
            ),
            "cache_low_hit_rate": AlertRule(
                name="Cache Low Hit Rate",
                description="Cache hit rate below 90%",
                component=MonitoringComponent.CACHE,
                severity=AlertSeverity.INFO,
                condition="hit_rate < 0.9",
                threshold=0.9,
                evaluation_period=600,
                cooldown_period=1800,
                channels=["grafana"]
            ),
            "database_connection_pool": AlertRule(
                name="Database Connection Pool",
                description="Database connections approaching limit",
                component=MonitoringComponent.DATABASE,
                severity=AlertSeverity.WARNING,
                condition="connections > 80",
                threshold=80,
                evaluation_period=300,
                cooldown_period=900,
                channels=["email", "slack"]
            )
        })

        # SLO definitions
        self.slo_definitions.update({
            "api_availability": SLODefinition(
                name="API Availability",
                metric=SLOMetric.API_AVAILABILITY,
                target=0.999,
                window=3600,
                description="API availability should be 99.9%",
                alert_threshold=0.995
            ),
            "api_latency": SLODefinition(
                name="API Latency",
                metric=SLOMetric.API_LATENCY,
                target=500,
                window=300,
                description="API P95 latency should be <500ms",
                alert_threshold=450
            ),
            "scenario_success": SLODefinition(
                name="Scenario Success Rate",
                metric=SLOMetric.SCENARIO_SUCCESS,
                target=0.95,
                window=1800,
                description="Scenario execution success rate >95%",
                alert_threshold=0.90
            )
        })

    def _setup_slo_monitoring(self):
        """Setup SLO-based monitoring and alerting."""
        self.logger.info("Setting up SLO monitoring")

        # Add SLO-specific alert rules
        self.alert_rules.update({
            "slo_violation": AlertRule(
                name="SLO Violation",
                description="Service Level Objective violation detected",
                component=MonitoringComponent.INFRASTRUCTURE,
                severity=AlertSeverity.CRITICAL,
                condition="slo_compliance < 0.95",
                threshold=0.95,
                evaluation_period=300,
                cooldown_period=1800,
                channels=["email", "slack", "grafana", "pagerduty"]
            ),
            "error_budget_burn": AlertRule(
                name="Error Budget Burn Rate",
                description="Error budget burning too fast",
                component=MonitoringComponent.INFRASTRUCTURE,
                severity=AlertSeverity.WARNING,
                condition="burn_rate > 2.0",
                threshold=2.0,
                evaluation_period=900,
                cooldown_period=3600,
                channels=["email", "slack"]
            )
        })

    def _setup_predictive_monitoring(self):
        """Setup predictive monitoring and alerting."""
        self.logger.info("Setting up predictive monitoring")

        # Add predictive alert rules
        self.alert_rules.update({
            "predicted_failure": AlertRule(
                name="Predicted System Failure",
                description="Predictive model indicates potential failure",
                component=MonitoringComponent.INFRASTRUCTURE,
                severity=AlertSeverity.CRITICAL,
                condition="failure_probability > 0.8",
                threshold=0.8,
                evaluation_period=600,
                cooldown_period=1800,
                channels=["email", "slack", "grafana", "pagerduty"]
            ),
            "capacity_trend": AlertRule(
                name="Capacity Trend Alert",
                description="Resource usage trending toward capacity",
                component=MonitoringComponent.INFRASTRUCTURE,
                severity=AlertSeverity.INFO,
                condition="capacity_trend > 0.9",
                threshold=0.9,
                evaluation_period=3600,
                cooldown_period=7200,
                channels=["grafana"]
            )
        })

    def update_metrics(self, metrics: Dict[str, Any]):
        """Update monitoring metrics."""
        for key, value in metrics.items():
            if hasattr(self.metrics, key):
                setattr(self.metrics, key, value)

        self.logger.debug(f"Updated metrics: {metrics}")

    def evaluate_alerts(self) -> List[Dict[str, Any]]:
        """Evaluate alert rules and return triggered alerts."""
        triggered_alerts = []

        for rule_name, rule in self.alert_rules.items():
            if not rule.enabled:
                continue

            # Simple alert evaluation (in real implementation, this would query Prometheus)
            if self._evaluate_alert_condition(rule):
                alert = {
                    "rule": rule_name,
                    "severity": rule.severity,
                    "component": rule.component,
                    "description": rule.description,
                    "timestamp": datetime.now().isoformat(),
                    "value": getattr(self.metrics, rule_name.split('_')[0] + '_value', 0),
                    "threshold": rule.threshold
                }

                triggered_alerts.append(alert)

                # Send alert
                self._send_alert(alert)

        return triggered_alerts

    def _evaluate_alert_condition(self, rule: AlertRule) -> bool:
        """Evaluate a single alert condition."""
        # Simplified evaluation - in production this would use Prometheus queries
        condition_mapping = {
            "error_rate > 0.05": self.metrics.api_errors_total / max(self.metrics.api_requests_total, 1) > 0.05,
            "latency_p95 > 500": self.metrics.api_latency_p95 > 500,
            "hit_rate < 0.9": (self.metrics.cache_hits / max(self.metrics.cache_hits + self.metrics.cache_misses, 1)) < 0.9,
            "connections > 80": self.metrics.database_connections > 80,
        }

        return condition_mapping.get(rule.condition, False)

    def _send_alert(self, alert: Dict[str, Any]):
        """Send alert to configured channels."""
        webhook_url = MONITORING_FEATURE_FLAGS["alert_webhook_url"]

        if webhook_url:
            try:
                # In real implementation, this would send HTTP request to webhook
                self.logger.info(f"Sending alert via webhook: {alert}")
            except Exception as e:
                self.logger.error(f"Failed to send alert: {e}")

    def check_slo_compliance(self) -> Dict[str, Any]:
        """Check SLO compliance and return status."""
        slo_status = {}

        for slo_name, slo_def in self.slo_definitions.items():
            if not slo_def.enabled:
                continue

            # Simplified SLO calculation - in production this would use Prometheus queries
            current_value = self._get_slo_current_value(slo_def.metric)
            compliance = current_value / slo_def.target if slo_def.target > 0 else 1.0

            slo_status[slo_name] = {
                "current_value": current_value,
                "target": slo_def.target,
                "compliance": compliance,
                "status": "compliant" if compliance >= slo_def.alert_threshold else "violated"
            }

        return slo_status

    def _get_slo_current_value(self, metric: SLOMetric) -> float:
        """Get current value for SLO metric."""
        metric_mapping = {
            SLOMetric.API_AVAILABILITY: 1.0 - (self.metrics.api_errors_total / max(self.metrics.api_requests_total, 1)),
            SLOMetric.API_LATENCY: self.metrics.api_latency_p95,
            SLOMetric.SCENARIO_SUCCESS: 1.0 - (self.metrics.failed_scenarios / max(self.metrics.active_scenarios, 1)),
            SLOMetric.CACHE_HIT_RATE: self.metrics.cache_hits / max(self.metrics.cache_hits + self.metrics.cache_misses, 1),
            SLOMetric.ERROR_RATE: self.metrics.api_errors_total / max(self.metrics.api_requests_total, 1),
            SLOMetric.DATA_FRESHNESS: self.metrics.data_freshness_minutes,
        }

        return metric_mapping.get(metric, 0.0)

    def get_dashboard_config(self) -> Dict[str, Any]:
        """Get dashboard configuration for Grafana."""
        return {
            "grafana_url": MONITORING_FEATURE_FLAGS["grafana_url"],
            "dashboards": {
                "slo_overview": self._generate_slo_dashboard(),
                "api_performance": self._generate_api_dashboard(),
                "infrastructure": self._generate_infrastructure_dashboard(),
                "business_metrics": self._generate_business_dashboard()
            }
        }

    def _generate_slo_dashboard(self) -> Dict[str, Any]:
        """Generate SLO dashboard configuration."""
        return {
            "title": "SLO Overview",
            "panels": [
                {
                    "title": "API Availability",
                    "type": "stat",
                    "targets": [{"expr": "rate(api_requests_total[5m])"}]
                },
                {
                    "title": "Error Budget Remaining",
                    "type": "graph",
                    "targets": [{"expr": "error_budget_remaining"}]
                }
            ]
        }

    def _generate_api_dashboard(self) -> Dict[str, Any]:
        """Generate API performance dashboard."""
        return {
            "title": "API Performance",
            "panels": [
                {
                    "title": "Request Rate",
                    "type": "graph",
                    "targets": [{"expr": "rate(api_requests_total[5m])"}]
                },
                {
                    "title": "Latency P95",
                    "type": "graph",
                    "targets": [{"expr": "api_latency_p95"}]
                }
            ]
        }

    def _generate_infrastructure_dashboard(self) -> Dict[str, Any]:
        """Generate infrastructure monitoring dashboard."""
        return {
            "title": "Infrastructure",
            "panels": [
                {
                    "title": "CPU Usage",
                    "type": "graph",
                    "targets": [{"expr": "system_cpu_usage"}]
                },
                {
                    "title": "Memory Usage",
                    "type": "graph",
                    "targets": [{"expr": "system_memory_usage"}]
                }
            ]
        }

    def _generate_business_dashboard(self) -> Dict[str, Any]:
        """Generate business metrics dashboard."""
        return {
            "title": "Business Metrics",
            "panels": [
                {
                    "title": "Scenario Success Rate",
                    "type": "stat",
                    "targets": [{"expr": "scenario_success_rate"}]
                },
                {
                    "title": "Cache Hit Rate",
                    "type": "stat",
                    "targets": [{"expr": "cache_hit_rate"}]
                }
            ]
        }

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get overall monitoring system status."""
        return {
            "migration_phase": MONITORING_FEATURE_FLAGS["monitoring_migration_phase"],
            "enhanced_observability": MONITORING_FEATURE_FLAGS["use_enhanced_observability"],
            "slo_monitoring": MONITORING_FEATURE_FLAGS["enable_slo_monitoring"],
            "predictive_alerting": MONITORING_FEATURE_FLAGS["enable_predictive_alerting"],
            "unified_dashboard": MONITORING_FEATURE_FLAGS["enable_unified_dashboard"],
            "active_alert_rules": len([r for r in self.alert_rules.values() if r.enabled]),
            "slo_definitions": len([s for s in self.slo_definitions.values() if s.enabled]),
            "active_alerts": len(self.active_alerts),
            "components_monitored": len(set(r.component for r in self.alert_rules.values())),
        }

# Convenience functions
def get_monitoring_manager() -> ConsolidatedMonitoringManager:
    """Get the consolidated monitoring manager."""
    return ConsolidatedMonitoringManager()

def update_metrics(metrics: Dict[str, Any]):
    """Update monitoring metrics."""
    manager = get_monitoring_manager()
    manager.update_metrics(metrics)

def evaluate_alerts() -> List[Dict[str, Any]]:
    """Evaluate and return triggered alerts."""
    manager = get_monitoring_manager()
    return manager.evaluate_alerts()

def check_slo_compliance() -> Dict[str, Any]:
    """Check SLO compliance status."""
    manager = get_monitoring_manager()
    return manager.check_slo_compliance()

def get_dashboard_config() -> Dict[str, Any]:
    """Get dashboard configuration."""
    manager = get_monitoring_manager()
    return manager.get_dashboard_config()
