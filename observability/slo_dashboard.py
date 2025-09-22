"""Service Level Objectives (SLO) and Service Level Indicators (SLI) dashboards.

This module provides:
- SLO/SLI definitions for key business metrics
- Dashboard configurations for monitoring
- Alert rules for SLO violations
- Performance baseline tracking
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any


class SLOCategory(Enum):
    """Categories of Service Level Objectives."""
    AVAILABILITY = "availability"
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    DATA_FRESHNESS = "data_freshness"


@dataclass
class ServiceLevelIndicator:
    """Service Level Indicator definition."""
    name: str
    description: str
    query: str
    category: SLOCategory
    target: float  # Target value (e.g., 0.99 for 99% availability)
    window: str = "28d"  # Time window for measurement
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class ServiceLevelObjective:
    """Service Level Objective definition."""
    name: str
    description: str
    indicators: List[ServiceLevelIndicator]
    target: float  # Overall SLO target
    window: str = "28d"
    alert_threshold: float = 0.95  # Alert when SLI drops below this


@dataclass
class AlertRule:
    """Alert rule for SLO violations."""
    name: str
    description: str
    slo_name: str
    condition: str  # Prometheus query condition
    threshold: float
    duration: str = "5m"  # How long condition must be true
    severity: str = "warning"
    channels: List[str] = field(default_factory=lambda: ["alerts"])


class AurumSLOs:
    """Service Level Objectives for Aurum platform."""

    # API Availability SLO
    API_AVAILABILITY = ServiceLevelObjective(
        name="api_availability",
        description="API endpoints should be available 99.9% of the time",
        target=0.999,
        indicators=[
            ServiceLevelIndicator(
                name="api_requests_success_rate",
                description="Percentage of successful API requests",
                query='rate(aurum_api_requests_total{status=~"2..|3.."}[5m]) / rate(aurum_api_requests_total[5m])',
                category=SLOCategory.AVAILABILITY,
                target=0.999
            )
        ]
    )

    # API Latency SLO
    API_LATENCY = ServiceLevelObjective(
        name="api_latency",
        description="API requests should respond within 500ms P95",
        target=0.95,
        indicators=[
            ServiceLevelIndicator(
                name="api_p95_latency",
                description="95th percentile API request latency",
                query='histogram_quantile(0.95, rate(aurum_api_request_duration_seconds_bucket[5m]))',
                category=SLOCategory.LATENCY,
                target=0.5  # 500ms
            )
        ]
    )

    # Scenario Creation SLO
    SCENARIO_CREATION = ServiceLevelObjective(
        name="scenario_creation_success",
        description="Scenario creation should succeed 99.5% of the time",
        target=0.995,
        indicators=[
            ServiceLevelIndicator(
                name="scenario_creation_success_rate",
                description="Success rate of scenario creation operations",
                query='rate(aurum_business_transactions_total{transaction_type="scenario_create",status="success"}[5m]) / rate(aurum_business_transactions_total{transaction_type="scenario_create"}[5m])',
                category=SLOCategory.AVAILABILITY,
                target=0.995
            )
        ]
    )

    # Scenario Run SLO
    SCENARIO_RUN_SUCCESS = ServiceLevelObjective(
        name="scenario_run_success",
        description="Scenario runs should succeed 95% of the time",
        target=0.95,
        indicators=[
            ServiceLevelIndicator(
                name="scenario_run_success_rate",
                description="Success rate of scenario run executions",
                query='rate(aurum_business_transactions_total{transaction_type="scenario_run",status="success"}[5m]) / rate(aurum_business_transactions_total{transaction_type="scenario_run"}[5m])',
                category=SLOCategory.AVAILABILITY,
                target=0.95
            )
        ]
    )

    # Data Freshness SLO
    DATA_FRESHNESS = ServiceLevelObjective(
        name="data_freshness",
        description="Data should be updated within 1 hour",
        target=0.99,
        indicators=[
            ServiceLevelIndicator(
                name="staleness_check",
                description="Percentage of data sources updated within freshness window",
                query='aurum_staleness_last_update_seconds < 3600',  # Less than 1 hour
                category=SLOCategory.DATA_FRESHNESS,
                target=0.99
            )
        ]
    )

    # Rate Limiting SLO
    RATE_LIMIT_AVAILABILITY = ServiceLevelObjective(
        name="rate_limit_availability",
        description="Rate limiting should be available 99.99% of the time",
        target=0.9999,
        indicators=[
            ServiceLevelIndicator(
                name="rate_limit_circuit_state",
                description="Rate limit circuit breaker should be closed",
                query='aurum_api_tenant_ratelimit_circuit_state{state="closed"}',
                category=SLOCategory.AVAILABILITY,
                target=1.0
            )
        ]
    )

    # Cache Performance SLO
    CACHE_HIT_RATE = ServiceLevelObjective(
        name="cache_hit_rate",
        description="Cache hit rate should be above 90%",
        target=0.90,
        indicators=[
            ServiceLevelIndicator(
                name="cache_hit_rate",
                description="Overall cache hit rate",
                query='rate(aurum_cache_hits_total[5m]) / (rate(aurum_cache_hits_total[5m]) + rate(aurum_cache_misses_total[5m]))',
                category=SLOCategory.THROUGHPUT,
                target=0.90
            )
        ]
    )


class AurumAlerts:
    """Alert rules for SLO violations and system health."""

    # API Availability Alerts
    API_AVAILABILITY_ALERT = AlertRule(
        name="APIHighErrorRate",
        description="API error rate is above 1%",
        slo_name="api_availability",
        condition='rate(aurum_api_requests_total{status=~"5.."}[5m]) / rate(aurum_api_requests_total[5m]) > 0.01',
        threshold=0.01,
        duration="5m",
        severity="critical"
    )

    # API Latency Alerts
    API_LATENCY_ALERT = AlertRule(
        name="APIHighLatency",
        description="API P95 latency is above 1 second",
        slo_name="api_latency",
        condition='histogram_quantile(0.95, rate(aurum_api_request_duration_seconds_bucket[5m])) > 1.0',
        threshold=1.0,
        duration="5m",
        severity="warning"
    )

    # Scenario Creation Alerts
    SCENARIO_CREATION_ALERT = AlertRule(
        name="ScenarioCreationFailures",
        description="Scenario creation failure rate is above 5%",
        slo_name="scenario_creation_success",
        condition='rate(aurum_business_transactions_total{transaction_type="scenario_create",status="error"}[5m]) / rate(aurum_business_transactions_total{transaction_type="scenario_create"}[5m]) > 0.05',
        threshold=0.05,
        duration="10m",
        severity="critical"
    )

    # Rate Limit Circuit Breaker Alert
    RATE_LIMIT_CIRCUIT_ALERT = AlertRule(
        name="RateLimitCircuitOpen",
        description="Rate limit circuit breaker is open",
        slo_name="rate_limit_availability",
        condition='aurum_api_tenant_ratelimit_circuit_state{state="open"} > 0',
        threshold=0,
        duration="1m",
        severity="warning"
    )

    # Database Connection Alerts
    DB_CONNECTION_POOL_ALERT = AlertRule(
        name="DatabaseConnectionPoolExhausted",
        description="Database connection pool is near exhaustion",
        slo_name="",
        condition='aurum_db_connections_active / aurum_db_connections_total > 0.9',
        threshold=0.9,
        duration="2m",
        severity="critical"
    )

    # Cache Performance Alert
    CACHE_PERFORMANCE_ALERT = AlertRule(
        name="CacheHitRateLow",
        description="Cache hit rate has dropped below 80%",
        slo_name="cache_hit_rate",
        condition='rate(aurum_cache_hits_total[5m]) / (rate(aurum_cache_hits_total[5m]) + rate(aurum_cache_misses_total[5m])) < 0.8',
        threshold=0.8,
        duration="10m",
        severity="warning"
    )

    @classmethod
    def get_all_alerts(cls) -> List[AlertRule]:
        """Get all alert rules."""
        return [
            cls.API_AVAILABILITY_ALERT,
            cls.API_LATENCY_ALERT,
            cls.SCENARIO_CREATION_ALERT,
            cls.RATE_LIMIT_CIRCUIT_ALERT,
            cls.DB_CONNECTION_POOL_ALERT,
            cls.CACHE_PERFORMANCE_ALERT,
        ]


class DashboardConfig:
    """Configuration for monitoring dashboards."""

    def __init__(self):
        self.slos = AurumSLOs()
        self.alerts = AurumAlerts()

    def get_slo_panels(self) -> List[Dict[str, Any]]:
        """Get dashboard panels for SLOs."""
        panels = []

        for slo in [self.slos.API_AVAILABILITY, self.slos.API_LATENCY,
                   self.slos.SCENARIO_CREATION, self.slos.SCENARIO_RUN_SUCCESS,
                   self.slos.DATA_FRESHNESS, self.slos.RATE_LIMIT_AVAILABILITY,
                   self.slos.CACHE_HIT_RATE]:
            for indicator in slo.indicators:
                panels.append({
                    "title": f"{slo.name} - {indicator.name}",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": indicator.query,
                            "legendFormat": indicator.name
                        }
                    ],
                    "thresholds": [
                        {
                            "value": slo.target,
                            "colorMode": "critical" if slo.target > 0.99 else "warning",
                            "op": "lt"
                        }
                    ]
                })

        return panels

    def get_business_metrics_panels(self) -> List[Dict[str, Any]]:
        """Get dashboard panels for business metrics."""
        return [
            {
                "title": "Scenario Operations",
                "type": "graph",
                "targets": [
                    {
                        "expr": 'rate(aurum_business_transactions_total{transaction_type="scenario_create"}[5m])',
                        "legendFormat": "Scenario Creation Rate"
                    },
                    {
                        "expr": 'rate(aurum_business_transactions_total{transaction_type="scenario_run"}[5m])',
                        "legendFormat": "Scenario Run Rate"
                    }
                ]
            },
            {
                "title": "Scenario Success Rates",
                "type": "stat",
                "targets": [
                    {
                        "expr": 'rate(aurum_business_transactions_total{transaction_type="scenario_create",status="success"}[5m]) / rate(aurum_business_transactions_total{transaction_type="scenario_create"}[5m])',
                        "legendFormat": "Scenario Creation Success Rate"
                    },
                    {
                        "expr": 'rate(aurum_business_transactions_total{transaction_type="scenario_run",status="success"}[5m]) / rate(aurum_business_transactions_total{transaction_type="scenario_run"}[5m])',
                        "legendFormat": "Scenario Run Success Rate"
                    }
                ]
            }
        ]

    def get_infrastructure_panels(self) -> List[Dict[str, Any]]:
        """Get dashboard panels for infrastructure metrics."""
        return [
            {
                "title": "Database Connection Pool",
                "type": "graph",
                "targets": [
                    {
                        "expr": "aurum_db_connections_active",
                        "legendFormat": "Active Connections"
                    },
                    {
                        "expr": "aurum_db_connections_idle",
                        "legendFormat": "Idle Connections"
                    },
                    {
                        "expr": "aurum_db_connections_total",
                        "legendFormat": "Total Connections"
                    }
                ]
            },
            {
                "title": "Cache Performance",
                "type": "graph",
                "targets": [
                    {
                        "expr": "rate(aurum_cache_hits_total[5m])",
                        "legendFormat": "Cache Hits"
                    },
                    {
                        "expr": "rate(aurum_cache_misses_total[5m])",
                        "legendFormat": "Cache Misses"
                    },
                    {
                        "expr": "rate(aurum_cache_hits_total[5m]) / (rate(aurum_cache_hits_total[5m]) + rate(aurum_cache_misses_total[5m]))",
                        "legendFormat": "Hit Rate"
                    }
                ]
            },
            {
                "title": "Rate Limiting",
                "type": "graph",
                "targets": [
                    {
                        "expr": "rate(aurum_api_tenant_ratelimit_total{result=\"allowed\"}[5m])",
                        "legendFormat": "Requests Allowed"
                    },
                    {
                        "expr": "rate(aurum_api_tenant_ratelimit_total{result=\"blocked\"}[5m])",
                        "legendFormat": "Requests Blocked"
                    },
                    {
                        "expr": "aurum_api_tenant_ratelimit_circuit_state",
                        "legendFormat": "Circuit Breaker State"
                    }
                ]
            }
        ]

    def get_performance_panels(self) -> List[Dict[str, Any]]:
        """Get dashboard panels for performance monitoring."""
        return [
            {
                "title": "API Request Latency",
                "type": "heatmap",
                "targets": [
                    {
                        "expr": "sum(rate(aurum_api_request_duration_seconds_bucket[5m])) by (le)",
                        "legendFormat": "Request Latency Distribution"
                    }
                ]
            },
            {
                "title": "Error Rate by Endpoint",
                "type": "graph",
                "targets": [
                    {
                        "expr": "rate(aurum_api_requests_total{status=~\"5..\"}[5m]) / rate(aurum_api_requests_total[5m])",
                        "legendFormat": "{{path}}"
                    }
                ]
            }
        ]

    def get_alert_panels(self) -> List[Dict[str, Any]]:
        """Get dashboard panels for alert status."""
        panels = []

        for alert in self.alerts.get_all_alerts():
            panels.append({
                "title": f"Alert: {alert.name}",
                "type": "stat",
                "targets": [
                    {
                        "expr": f"({alert.condition}) * 100",
                        "legendFormat": alert.description
                    }
                ],
                "thresholds": [
                    {
                        "value": alert.threshold * 100,
                        "colorMode": "critical",
                        "op": "gt"
                    }
                ],
                "links": [
                    {
                        "title": "View Alert Rule",
                        "url": f"/alerting/rule/{alert.name}"
                    }
                ]
            })

        return panels

    def generate_grafana_dashboard(self) -> Dict[str, Any]:
        """Generate a Grafana dashboard configuration."""
        return {
            "dashboard": {
                "title": "Aurum Platform - SLO Dashboard",
                "description": "Service Level Objectives and Key Performance Indicators",
                "tags": ["aurum", "slo", "monitoring"],
                "timezone": "UTC",
                "panels": [
                    # SLO Overview
                    {
                        "title": "SLO Overview",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "count(aurum_api_requests_total > 0)",
                                "legendFormat": "API Requests"
                            }
                        ],
                        "gridPos": {"x": 0, "y": 0, "w": 24, "h": 3}
                    },
                    # SLO Panels
                    *self.get_slo_panels(),
                    # Business Metrics
                    *self.get_business_metrics_panels(),
                    # Infrastructure
                    *self.get_infrastructure_panels(),
                    # Performance
                    *self.get_performance_panels(),
                    # Alerts
                    *self.get_alert_panels()
                ],
                "time": {
                    "from": "now-6h",
                    "to": "now"
                },
                "refresh": "5m"
            }
        }


# Global dashboard configuration
dashboard_config = DashboardConfig()


def get_slo_dashboard_config() -> DashboardConfig:
    """Get the global SLO dashboard configuration."""
    return dashboard_config


def check_slo_status() -> Dict[str, Any]:
    """Check current SLO status and return report."""
    # This would integrate with Prometheus to check current SLO status
    # For now, return a basic status
    return {
        "timestamp": time.time(),
        "slos": {
            "api_availability": {
                "current": 0.998,
                "target": 0.999,
                "status": "warning"
            },
            "api_latency": {
                "current": 0.450,
                "target": 0.500,
                "status": "ok"
            },
            "scenario_creation": {
                "current": 0.992,
                "target": 0.995,
                "status": "warning"
            },
            "scenario_run_success": {
                "current": 0.945,
                "target": 0.95,
                "status": "warning"
            }
        },
        "overall_status": "warning"
    }


def get_sli_values() -> Dict[str, float]:
    """Get current Service Level Indicator values."""
    # This would query Prometheus for current SLI values
    # For now, return mock values
    return {
        "api_requests_success_rate": 0.998,
        "api_p95_latency": 0.450,
        "scenario_creation_success_rate": 0.992,
        "scenario_run_success_rate": 0.945,
        "cache_hit_rate": 0.89,
        "data_freshness": 0.95,
        "rate_limit_availability": 0.9999
    }
