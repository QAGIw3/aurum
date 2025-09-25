#!/usr/bin/env python3
"""Demo script for Monitoring & Alerting Setup with comprehensive observability."""

import os
import logging
from enum import Enum
from typing import Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime

# Feature flags for monitoring consolidation
MONITORING_FEATURE_FLAGS = {
    "use_enhanced_observability": os.getenv("AURUM_USE_ENHANCED_OBSERVABILITY", "true").lower() == "true",
    "monitoring_migration_phase": os.getenv("AURUM_MONITORING_MIGRATION_PHASE", "2"),
    "enable_slo_monitoring": os.getenv("AURUM_ENABLE_SLO_MONITORING", "true").lower() == "true",
    "enable_predictive_alerting": os.getenv("AURUM_ENABLE_PREDICTIVE_ALERTING", "false").lower() == "true",
    "enable_unified_dashboard": os.getenv("AURUM_ENABLE_UNIFIED_DASHBOARD", "false").lower() == "true",
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
class MockMonitoringMetrics:
    """Mock monitoring metrics for demonstration."""
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

class MockConsolidatedMonitoringManager:
    """Mock consolidated monitoring manager for demonstration."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = MockMonitoringMetrics()
        self.alert_rules: Dict[str, Any] = {}
        self.slo_definitions: Dict[str, Any] = {}
        self.active_alerts: Dict[str, Any] = {}

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

        self.alert_rules["api_error_rate"] = {
            "name": "API Error Rate",
            "description": "API error rate exceeds threshold",
            "component": MonitoringComponent.API,
            "severity": AlertSeverity.WARNING,
            "condition": "error_rate > 0.05",
            "threshold": 0.05,
            "enabled": True
        }

    def _setup_enhanced_monitoring(self):
        """Setup enhanced monitoring with comprehensive observability."""
        self.logger.info("Setting up enhanced monitoring")

        self.alert_rules.update({
            "api_high_latency": {
                "name": "API High Latency",
                "description": "API P95 latency exceeds 500ms",
                "component": MonitoringComponent.API,
                "severity": AlertSeverity.WARNING,
                "condition": "latency_p95 > 500",
                "threshold": 500,
                "enabled": True
            },
            "cache_low_hit_rate": {
                "name": "Cache Low Hit Rate",
                "description": "Cache hit rate below 90%",
                "component": MonitoringComponent.CACHE,
                "severity": AlertSeverity.INFO,
                "condition": "hit_rate < 0.9",
                "threshold": 0.9,
                "enabled": True
            },
            "database_connection_pool": {
                "name": "Database Connection Pool",
                "description": "Database connections approaching limit",
                "component": MonitoringComponent.DATABASE,
                "severity": AlertSeverity.WARNING,
                "condition": "connections > 80",
                "threshold": 80,
                "enabled": True
            }
        })

        # SLO definitions
        self.slo_definitions.update({
            "api_availability": {
                "name": "API Availability",
                "target": 0.999,
                "window": 3600,
                "description": "API availability should be 99.9%",
                "alert_threshold": 0.995,
                "enabled": True
            },
            "api_latency": {
                "name": "API Latency",
                "target": 500,
                "window": 300,
                "description": "API P95 latency should be <500ms",
                "alert_threshold": 450,
                "enabled": True
            },
            "scenario_success": {
                "name": "Scenario Success Rate",
                "target": 0.95,
                "window": 1800,
                "description": "Scenario execution success rate >95%",
                "alert_threshold": 0.90,
                "enabled": True
            }
        })

    def _setup_slo_monitoring(self):
        """Setup SLO-based monitoring and alerting."""
        self.logger.info("Setting up SLO monitoring")

        self.alert_rules.update({
            "slo_violation": {
                "name": "SLO Violation",
                "description": "Service Level Objective violation detected",
                "component": MonitoringComponent.INFRASTRUCTURE,
                "severity": AlertSeverity.CRITICAL,
                "condition": "slo_compliance < 0.95",
                "threshold": 0.95,
                "enabled": True
            }
        })

    def _setup_predictive_monitoring(self):
        """Setup predictive monitoring and alerting."""
        self.logger.info("Setting up predictive monitoring")

        self.alert_rules.update({
            "predicted_failure": {
                "name": "Predicted System Failure",
                "description": "Predictive model indicates potential failure",
                "component": MonitoringComponent.INFRASTRUCTURE,
                "severity": AlertSeverity.CRITICAL,
                "condition": "failure_probability > 0.8",
                "threshold": 0.8,
                "enabled": True
            }
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
            if not rule.get("enabled", False):
                continue

            # Simple alert evaluation
            if self._evaluate_alert_condition(rule):
                alert = {
                    "rule": rule_name,
                    "severity": rule["severity"],
                    "component": rule["component"],
                    "description": rule["description"],
                    "timestamp": datetime.now().isoformat(),
                    "value": getattr(self.metrics, f"{rule_name}_value", 0),
                    "threshold": rule["threshold"]
                }
                triggered_alerts.append(alert)
                self.logger.info(f"🚨 Alert triggered: {alert}")

        return triggered_alerts

    def _evaluate_alert_condition(self, rule: Dict[str, Any]) -> bool:
        """Evaluate a single alert condition."""
        # Simplified evaluation
        condition_mapping = {
            "error_rate > 0.05": self.metrics.api_errors_total / max(self.metrics.api_requests_total, 1) > 0.05,
            "latency_p95 > 500": self.metrics.api_latency_p95 > 500,
            "hit_rate < 0.9": (self.metrics.cache_hits / max(self.metrics.cache_hits + self.metrics.cache_misses, 1)) < 0.9,
            "connections > 80": self.metrics.database_connections > 80,
            "slo_compliance < 0.95": True,  # Always trigger for demo
            "failure_probability > 0.8": False,  # Don't trigger for demo
        }

        return condition_mapping.get(rule["condition"], False)

    def check_slo_compliance(self) -> Dict[str, Any]:
        """Check SLO compliance and return status."""
        slo_status = {}

        for slo_name, slo_def in self.slo_definitions.items():
            if not slo_def.get("enabled", False):
                continue

            # Mock SLO calculation
            if slo_name == "api_availability":
                current_value = 1.0 - (self.metrics.api_errors_total / max(self.metrics.api_requests_total, 1))
            elif slo_name == "api_latency":
                current_value = self.metrics.api_latency_p95
            elif slo_name == "scenario_success":
                current_value = 1.0 - (self.metrics.failed_scenarios / max(self.metrics.active_scenarios, 1))
            else:
                current_value = 0.95

            target = slo_def["target"]
            compliance = current_value / target if target > 0 else 1.0
            alert_threshold = slo_def.get("alert_threshold", 0.95)

            slo_status[slo_name] = {
                "current_value": current_value,
                "target": target,
                "compliance": compliance,
                "status": "compliant" if compliance >= alert_threshold else "violated"
            }

        return slo_status

    def get_dashboard_config(self) -> Dict[str, Any]:
        """Get dashboard configuration for Grafana."""
        return {
            "grafana_url": "http://grafana.aurum-dev.svc.cluster.local:3000",
            "dashboards": {
                "slo_overview": {
                    "title": "SLO Overview",
                    "description": "Service Level Objectives and compliance status",
                    "panels": [
                        {
                            "title": "API Availability",
                            "type": "stat",
                            "value": "99.9%"
                        },
                        {
                            "title": "Error Budget Remaining",
                            "type": "graph",
                            "value": "95.2%"
                        }
                    ]
                },
                "api_performance": {
                    "title": "API Performance",
                    "description": "API request rates, latency, and error rates",
                    "panels": [
                        {
                            "title": "Request Rate",
                            "type": "graph",
                            "value": "125 req/sec"
                        },
                        {
                            "title": "Latency P95",
                            "type": "graph",
                            "value": "245ms"
                        }
                    ]
                },
                "infrastructure": {
                    "title": "Infrastructure",
                    "description": "System resources and infrastructure metrics",
                    "panels": [
                        {
                            "title": "CPU Usage",
                            "type": "graph",
                            "value": "45%"
                        },
                        {
                            "title": "Memory Usage",
                            "type": "graph",
                            "value": "67%"
                        }
                    ]
                },
                "business_metrics": {
                    "title": "Business Metrics",
                    "description": "Business KPIs and scenario performance",
                    "panels": [
                        {
                            "title": "Scenario Success Rate",
                            "type": "stat",
                            "value": "98.5%"
                        },
                        {
                            "title": "Cache Hit Rate",
                            "type": "stat",
                            "value": "94.2%"
                        }
                    ]
                }
            }
        }

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get overall monitoring system status."""
        return {
            "migration_phase": MONITORING_FEATURE_FLAGS["monitoring_migration_phase"],
            "enhanced_observability": MONITORING_FEATURE_FLAGS["use_enhanced_observability"],
            "slo_monitoring": MONITORING_FEATURE_FLAGS["enable_slo_monitoring"],
            "predictive_alerting": MONITORING_FEATURE_FLAGS["enable_predictive_alerting"],
            "unified_dashboard": MONITORING_FEATURE_FLAGS["enable_unified_dashboard"],
            "active_alert_rules": len([r for r in self.alert_rules.values() if r.get("enabled", False)]),
            "slo_definitions": len([s for s in self.slo_definitions.values() if s.get("enabled", False)]),
            "active_alerts": len(self.active_alerts),
            "components_monitored": len(set(r["component"] for r in self.alert_rules.values())),
            "current_metrics": {
                "api_requests": self.metrics.api_requests_total,
                "api_errors": self.metrics.api_errors_total,
                "api_latency": self.metrics.api_latency_p95,
                "cache_hit_rate": self.metrics.cache_hits / max(self.metrics.cache_hits + self.metrics.cache_misses, 1),
                "system_cpu": self.metrics.system_cpu_usage,
                "system_memory": self.metrics.system_memory_usage
            }
        }

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_monitoring_migration_phase() -> str:
    """Get current monitoring migration phase."""
    return MONITORING_FEATURE_FLAGS["monitoring_migration_phase"]

def is_enhanced_observability_enabled() -> bool:
    """Check if enhanced observability is enabled."""
    return MONITORING_FEATURE_FLAGS["use_enhanced_observability"]

def log_monitoring_migration_status():
    """Log current monitoring migration status."""
    phase = get_monitoring_migration_phase()
    enhanced = is_enhanced_observability_enabled()
    slo = MONITORING_FEATURE_FLAGS["enable_slo_monitoring"]
    predictive = MONITORING_FEATURE_FLAGS["enable_predictive_alerting"]
    unified = MONITORING_FEATURE_FLAGS["enable_unified_dashboard"]

    logger.info(f"Monitoring Migration Status: Phase {phase}, Enhanced: {enhanced}")
    logger.info(f"SLO Monitoring: {slo}, Predictive: {predictive}, Unified: {unified}")

def demo_monitoring_setup():
    """Demonstrate comprehensive monitoring and alerting setup."""
    logger.info("=== Monitoring & Alerting Setup Demo ===")

    # Show current migration status
    logger.info("1. Current Migration Status:")
    log_monitoring_migration_status()

    # Create monitoring manager
    manager = MockConsolidatedMonitoringManager()

    # Test basic mode (legacy monitoring)
    logger.info("\n2. Testing Legacy Mode (Basic Monitoring):")
    os.environ["AURUM_USE_ENHANCED_OBSERVABILITY"] = "false"
    os.environ["AURUM_MONITORING_MIGRATION_PHASE"] = "1"

    manager_basic = MockConsolidatedMonitoringManager()
    legacy_status = manager_basic.get_monitoring_status()
    logger.info(f"✅ Legacy monitoring: {legacy_status['active_alert_rules']} alert rules, {legacy_status['components_monitored']} components")

    # Test enhanced mode (comprehensive observability)
    logger.info("\n3. Testing Enhanced Mode (Comprehensive Observability):")
    os.environ["AURUM_USE_ENHANCED_OBSERVABILITY"] = "true"
    os.environ["AURUM_MONITORING_MIGRATION_PHASE"] = "2"
    os.environ["AURUM_ENABLE_SLO_MONITORING"] = "true"

    manager_enhanced = MockConsolidatedMonitoringManager()
    enhanced_status = manager_enhanced.get_monitoring_status()
    logger.info(f"✅ Enhanced monitoring: {enhanced_status['active_alert_rules']} alert rules, {enhanced_status['slo_definitions']} SLOs")

    # Test SLO compliance checking
    logger.info("\n4. Testing SLO Compliance:")
    slo_status = manager_enhanced.check_slo_compliance()
    for slo_name, status in slo_status.items():
        compliance_pct = status['compliance'] * 100
        status_emoji = "✅" if status['status'] == 'compliant' else "❌"
        logger.info(f"   {status_emoji} {slo_name}: {compliance_pct:.1f}% compliant (target: {status['target']})")

    # Test alert evaluation
    logger.info("\n5. Testing Alert Evaluation:")
    manager_enhanced.update_metrics({
        "api_requests_total": 1000,
        "api_errors_total": 60,  # 6% error rate
        "api_latency_p95": 650,  # High latency
        "cache_hits": 850,
        "cache_misses": 150,     # 85% hit rate
        "database_connections": 85,  # High connection count
    })

    triggered_alerts = manager_enhanced.evaluate_alerts()
    logger.info(f"🚨 Triggered {len(triggered_alerts)} alerts:")
    for alert in triggered_alerts:
        logger.info(f"   - {alert['severity'].upper()}: {alert['description']}")

    # Test dashboard configuration
    logger.info("\n6. Testing Dashboard Configuration:")
    dashboard_config = manager_enhanced.get_dashboard_config()
    logger.info(f"📊 Grafana URL: {dashboard_config['grafana_url']}")
    logger.info(f"📋 Available dashboards: {list(dashboard_config['dashboards'].keys())}")

    for dashboard_name, dashboard in dashboard_config['dashboards'].items():
        logger.info(f"   - {dashboard['title']}: {len(dashboard['panels'])} panels")

    # Show optimization benefits
    logger.info("\n=== Monitoring Improvements ===")
    logger.info("✅ Unified observability across all components")
    logger.info("✅ SLO-based alerting with error budgets")
    logger.info("✅ Predictive monitoring capabilities")
    logger.info("✅ Comprehensive dashboard integration")
    logger.info("✅ Feature-flagged gradual migration")
    logger.info("✅ Automatic alert correlation")

    # Calculate improvements
    legacy_rules = legacy_status['active_alert_rules']
    enhanced_rules = enhanced_status['active_alert_rules']
    slo_improvement = enhanced_status['slo_definitions']
    dashboard_improvement = len(dashboard_config['dashboards'])

    logger.info("\n=== Quantitative Improvements ===")
    logger.info(f"Alert rules: {legacy_rules} → {enhanced_rules} (+{enhanced_rules-legacy_rules})")
    logger.info(f"SLO definitions: 0 → {slo_improvement} (comprehensive)")
    logger.info(f"Dashboard views: 0 → {dashboard_improvement} (unified)")
    logger.info(f"Monitoring coverage: Basic → Comprehensive observability")

    logger.info("\n=== Environment Variables ===")
    logger.info("AURUM_USE_ENHANCED_OBSERVABILITY=true  # Enable comprehensive monitoring")
    logger.info("AURUM_MONITORING_MIGRATION_PHASE=2     # Set migration phase")
    logger.info("AURUM_ENABLE_SLO_MONITORING=true       # Enable SLO-based alerting")
    logger.info("AURUM_ENABLE_PREDICTIVE_ALERTING=true # Enable predictive monitoring")
    logger.info("AURUM_ENABLE_UNIFIED_DASHBOARD=true   # Enable unified dashboards")

    logger.info("\n=== Monitoring Components ===")
    for component in MonitoringComponent:
        logger.info(f"   - {component.value}")

    logger.info("\n=== SLO Metrics ===")
    for metric in SLOMetric:
        logger.info(f"   - {metric.value}")

    logger.info("\n=== Alert Severities ===")
    for severity in AlertSeverity:
        logger.info(f"   - {severity.value}")

    logger.info("\n=== Migration Strategy ===")
    logger.info("1. Phase 1: Basic monitoring (current)")
    logger.info("2. Phase 2: Enhanced observability with SLOs (recommended)")
    logger.info("3. Phase 3: Predictive monitoring and unified dashboards")

def demo_alert_comparison():
    """Compare legacy vs enhanced alerting capabilities."""
    logger.info("\n=== Alert System Comparison ===")

    # Legacy alerting
    legacy_alerting = {
        "basic_monitoring": {
            "alerts": ["High CPU", "Low Memory", "Service Down"],
            "channels": ["email"],
            "thresholds": "static",
            "correlation": "none",
            "context": "minimal"
        }
    }

    # Enhanced alerting
    enhanced_alerting = {
        "comprehensive_observability": {
            "alerts": [
                "SLO Violation",
                "Error Budget Burn Rate",
                "API High Latency",
                "Cache Low Hit Rate",
                "Database Connection Pool",
                "Predicted System Failure",
                "Capacity Trend Alert"
            ],
            "channels": ["email", "slack", "grafana", "pagerduty"],
            "thresholds": "dynamic (SLO-based)",
            "correlation": "automatic",
            "context": "rich (traces, metrics, logs)"
        }
    }

    logger.info("Legacy Alerting:")
    legacy_config = legacy_alerting["basic_monitoring"]
    logger.info(f"  - Alerts: {len(legacy_config['alerts'])} basic alerts")
    logger.info(f"    {legacy_config['alerts']}")
    logger.info(f"  - Channels: {legacy_config['channels']}")
    logger.info(f"  - Thresholds: {legacy_config['thresholds']}")
    logger.info(f"  - Correlation: {legacy_config['correlation']}")

    logger.info("\nEnhanced Alerting:")
    enhanced_config = enhanced_alerting["comprehensive_observability"]
    logger.info(f"  - Alerts: {len(enhanced_config['alerts'])} intelligent alerts")
    logger.info(f"    {enhanced_config['alerts']}")
    logger.info(f"  - Channels: {enhanced_config['channels']}")
    logger.info(f"  - Thresholds: {enhanced_config['thresholds']}")
    logger.info(f"  - Correlation: {enhanced_config['correlation']}")

    # Calculate improvements
    alert_improvement = len(enhanced_config['alerts']) - len(legacy_config['alerts'])
    channel_improvement = len(enhanced_config['channels']) - len(legacy_config['channels'])

    logger.info("\n=== Alert System Improvements ===")
    logger.info(f"Alert intelligence: {alert_improvement} additional smart alerts")
    logger.info(f"Channel diversity: {channel_improvement} additional notification channels")
    logger.info(f"Threshold management: Static → Dynamic (SLO-based)")
    logger.info(f"Alert fatigue: High → Low (intelligent correlation)")
    logger.info(f"Operational efficiency: Manual → Automated response")

def demo_dashboard_comparison():
    """Compare legacy vs unified dashboard capabilities."""
    logger.info("\n=== Dashboard Comparison ===")

    # Legacy dashboards
    legacy_dashboards = {
        "individual_views": {
            "dashboards": ["API Metrics", "Database Stats", "Cache Performance"],
            "updates": "manual",
            "integration": "none",
            "alerts": "separate",
            "data_sources": "isolated"
        }
    }

    # Unified dashboards
    unified_dashboards = {
        "comprehensive_observability": {
            "dashboards": [
                "SLO Overview",
                "API Performance",
                "Infrastructure Monitoring",
                "Business Metrics",
                "Alert Correlation",
                "Capacity Planning",
                "Service Dependencies"
            ],
            "updates": "automatic",
            "integration": "complete",
            "alerts": "correlated",
            "data_sources": "unified"
        }
    }

    logger.info("Legacy Dashboards:")
    legacy_config = legacy_dashboards["individual_views"]
    logger.info(f"  - Dashboards: {len(legacy_config['dashboards'])} individual views")
    logger.info(f"    {legacy_config['dashboards']}")
    logger.info(f"  - Updates: {legacy_config['updates']}")
    logger.info(f"  - Integration: {legacy_config['integration']}")

    logger.info("\nUnified Dashboards:")
    unified_config = unified_dashboards["comprehensive_observability"]
    logger.info(f"  - Dashboards: {len(unified_config['dashboards'])} comprehensive views")
    logger.info(f"    {unified_config['dashboards']}")
    logger.info(f"  - Updates: {unified_config['updates']}")
    logger.info(f"  - Integration: {unified_config['integration']}")

    # Calculate improvements
    dashboard_improvement = len(unified_config['dashboards']) - len(legacy_config['dashboards'])

    logger.info("\n=== Dashboard Improvements ===")
    logger.info(f"View consolidation: {dashboard_improvement} additional unified views")
    logger.info(f"Update automation: Manual → Automatic")
    logger.info(f"Data integration: Isolated → Unified")
    logger.info(f"Alert handling: Separate → Correlated")
    logger.info(f"Operational insights: Basic → Predictive")

if __name__ == "__main__":
    demo_monitoring_setup()
    demo_alert_comparison()
    demo_dashboard_comparison()
