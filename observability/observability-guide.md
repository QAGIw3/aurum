# Aurum Observability and SLO Guide

This guide provides comprehensive information about Aurum's observability stack, Service Level Objectives (SLOs), and monitoring capabilities.

## Overview

Aurum's observability system is built on several key components:

- **Metrics Collection**: Prometheus-based metrics with comprehensive coverage
- **SLO Monitoring**: Service Level Objective tracking with automated alerting
- **Alerting**: Multi-channel alerting with intelligent routing
- **Dashboards**: Grafana dashboards for visualization
- **Fault Injection**: Testing capabilities for observability validation

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Applications  │───▶│  Metrics        │───▶│  Prometheus     │
│                 │    │  Collection     │    │  Server         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SLO Monitor   │    │  Alert Manager  │    │  Grafana        │
│                 │    │                 │    │  Dashboards     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Alert Routing  │    │  Notification   │    │  External       │
│  & Processing   │    │  Channels       │    │  Systems        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Metrics

### API Performance Metrics

**Request Metrics:**
- `aurum_api_requests_total`: Total API requests by method, path, status
- `aurum_api_request_duration_seconds`: API request duration histogram
- `aurum_api_request_duration_percentiles_seconds`: Request duration percentiles
- `aurum_api_request_size_bytes`: Request size histogram
- `aurum_api_response_size_bytes`: Response size histogram

**Database Pool Metrics:**
- `aurum_db_pool_connections_active_current`: Active connections
- `aurum_db_pool_connections_idle_current`: Idle connections
- `aurum_db_pool_connections_total_current`: Total connections
- `aurum_db_pool_acquire_time_seconds`: Connection acquisition time
- `aurum_db_pool_connection_timeouts_total`: Connection timeouts
- `aurum_db_pool_connection_errors_total`: Connection errors

**Cache Metrics:**
- `aurum_cache_hit_ratio`: Cache hit ratio
- `aurum_cache_efficiency`: Cache efficiency score
- `aurum_cache_memory_usage_bytes`: Memory usage
- `aurum_cache_invalidation_total`: Cache invalidations
- `aurum_cache_stale_hits_total`: Stale cache hits

### SLO Metrics

**SLO Compliance:**
- `aurum_slo_compliance_total`: SLO compliance counter
- `aurum_slo_violation_duration_seconds`: Violation duration
- `aurum_slo_availability`: Availability percentage
- `aurum_slo_latency_p50_seconds`: P50 latency
- `aurum_slo_latency_p95_seconds`: P95 latency
- `aurum_slo_latency_p99_seconds`: P99 latency
- `aurum_slo_error_rate`: Error rate percentage

### Data Quality Metrics

**Data Freshness:**
- `aurum_data_freshness_hours`: Data age in hours
- `aurum_data_freshness_score`: Freshness score (0-1)
- `aurum_data_staleness_violations_total`: Staleness violations

**Great Expectations:**
- `aurum_ge_validation_runs_total`: Validation runs
- `aurum_ge_validation_duration_seconds`: Validation duration
- `aurum_ge_validation_expectations_total`: Validation expectations
- `aurum_ge_validation_quality_score`: Quality score

**Canary Metrics:**
- `aurum_canary_execution_status_total`: Execution status
- `aurum_canary_execution_duration_seconds`: Execution duration
- `aurum_canary_api_health_score`: API health score
- `aurum_canary_data_quality_score`: Data quality score

### System Health Metrics

**Health Scores:**
- `aurum_system_health_score`: Overall system health
- `aurum_component_health_status`: Component health status
- `aurum_system_uptime_seconds_total`: System uptime

**Alerting Metrics:**
- `aurum_alerts_fired_total`: Alerts fired
- `aurum_alerts_suppressed_total`: Alerts suppressed
- `aurum_alert_processing_duration_seconds`: Alert processing time

## Service Level Objectives (SLOs)

### Default SLOs

**API Availability SLO:**
- **Target**: 99% availability
- **Window**: 30 days
- **Measurement**: HTTP 2xx/3xx responses
- **Alert**: When availability drops below 99%

**API Latency SLO:**
- **Target**: 1 second p95 latency
- **Window**: 30 days
- **Measurement**: Request duration
- **Alert**: When p95 latency exceeds 1 second

**Error Rate SLO:**
- **Target**: < 1% error rate
- **Window**: 30 days
- **Measurement**: 5xx responses
- **Alert**: When error rate exceeds 1%

**Data Freshness SLO:**
- **Target**: 95% of data < 24 hours old
- **Window**: 7 days
- **Measurement**: Data age vs. expected frequency
- **Alert**: When freshness drops below 95%

**Cache Hit Rate SLO:**
- **Target**: > 90% cache hit rate
- **Window**: 1 day
- **Measurement**: Cache hit ratio
- **Alert**: When hit rate drops below 90%

### Custom SLOs

You can define custom SLOs using the SLOMonitor class:

```python
from aurum.observability.slo_monitor import SLOMonitor, SLOType
from datetime import timedelta

monitor = SLOMonitor()

# Add custom availability SLO
monitor.add_availability_slo(
    name="custom_endpoint_availability",
    target=0.995,  # 99.5% availability
    window=timedelta(days=7),
    description="Custom endpoint should be available 99.5% of the time"
)

# Add custom latency SLO
monitor.add_latency_slo(
    name="custom_operation_latency",
    target=0.5,  # 500ms target
    window=timedelta(days=30),
    description="Custom operation should complete within 500ms (p95)"
)

# Record measurements
await monitor.record_measurement(
    "custom_endpoint_availability",
    1.0 if is_success else 0.0,
    compliant=is_success
)
```

## Alerting

### Alert Rules

Alert rules are defined in `config/grafana_dashboards.json` and automatically processed by the AlertManager.

**Example Alert Rule:**
```json
{
  "api_high_error_rate": {
    "name": "API High Error Rate",
    "expr": "rate(aurum_api_requests_total{status=~\"5..\"}[5m]) / rate(aurum_api_requests_total[5m]) > 0.05",
    "for": "5m",
    "labels": {
      "severity": "critical"
    },
    "annotations": {
      "summary": "API error rate is above 5%",
      "description": "API endpoints are returning 5xx errors at a rate > 5%"
    }
  }
}
```

### Alert Channels

**Slack Integration:**
```python
from aurum.observability.alert_manager import AlertManager, AlertChannel

manager = AlertManager()
manager.add_notification_channel(
    AlertChannel.SLACK,
    slack_notification_handler
)
```

**PagerDuty Integration:**
```python
manager.add_notification_channel(
    AlertChannel.PAGERDUTY,
    pagerduty_notification_handler
)
```

**Email Integration:**
```python
manager.add_notification_channel(
    AlertChannel.EMAIL,
    email_notification_handler
)
```

### Alert Processing

```python
from aurum.observability.alert_manager import get_alert_manager

manager = get_alert_manager()

# Process alerts based on current conditions
context = {
    "status_code": 500,
    "duration_seconds": 2.5,
    "dataset": "test_data",
    "hours_old": 30
}

alerts = await manager.process_alerts(context)

for alert in alerts:
    print(f"Alert fired: {alert.rule.name}")
    print(f"Severity: {alert.rule.severity.value}")
    print(f"Channels: {[c.value for c in alert.rule.channels]}")
```

## Dashboards

### API Overview Dashboard

**Location**: `config/grafana_dashboards.json` → `aurum-api-overview`

**Panels:**
- API Request Rate (requests/second)
- API Latency (p50/p95/p99)
- Error Rate by Endpoint
- Database Connection Pool Usage
- Cache Performance
- Active Connections

### SLO Dashboard

**Location**: `config/grafana_dashboards.json` → `aurum-slo-dashboard`

**Panels:**
- SLO Compliance Status (table)
- SLO Availability Trends
- SLO Latency Trends
- SLO Error Rate Trends
- SLO Violation History

### External Data Dashboard

**Location**: `config/grafana_dashboards.json` → `aurum-external-data`

**Panels:**
- Data Freshness by Dataset
- Great Expectations Validation Results
- Canary Execution Status
- Staleness Monitoring
- External API Performance

### System Health Dashboard

**Location**: `config/grafana_dashboards.json` → `aurum-system-health`

**Panels:**
- Overall System Health Score
- Component Health Status (table)
- System Uptime
- Alert Status and History
- Resource Usage (CPU/Memory)

## Fault Injection

### Running Fault Injection

```bash
# Inject API errors for 10 minutes
python scripts/ops/fault_injection.py api_high_error_rate \
    --duration 10 \
    --parameter error_rate=0.1 \
    --parameter method=POST \
    --parameter path=/api/test

# Inject high latency for 5 minutes
python scripts/ops/fault_injection.py api_high_latency \
    --duration 5 \
    --parameter latency_seconds=2.0

# Run combined scenario
python scripts/ops/fault_injection.py combined_scenario \
    --duration 15

# Run full test suite
python scripts/ops/fault_injection.py test_suite
```

### Available Scenarios

- `api_high_error_rate`: Inject API 5xx errors
- `api_high_latency`: Inject high request latency
- `data_staleness`: Simulate stale data
- `ge_validation_failure`: Simulate validation failures
- `canary_failure`: Simulate canary failures
- `system_health_degradation`: Degrade system health
- `combined_scenario`: Run comprehensive fault scenario
- `test_suite`: Run all fault injection tests

### Monitoring Fault Injection

During fault injection, monitor:

1. **Alert Firing**: Check that alerts fire correctly
2. **Metrics**: Verify metrics are recorded properly
3. **Dashboards**: Confirm dashboards show correct status
4. **SLO Compliance**: Check SLO violation tracking
5. **Recovery**: Ensure system recovers when faults stop

## Configuration

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'aurum-api'
    static_configs:
      - targets: ['aurum-api:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s

  - job_name: 'aurum-external'
    static_configs:
      - targets: ['aurum-external:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### Grafana Configuration

```json
{
  "dashboards": {
    "aurum-overview": {
      "title": "Aurum Overview",
      "refresh": "30s",
      "panels": [...]
    }
  }
}
```

### AlertManager Configuration

```yaml
# alertmanager.yml
global:
  smtp_smarthost: 'smtp.example.com:587'
  smtp_from: 'alerts@aurum.com'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'aurum-team'

receivers:
  - name: 'aurum-team'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/...'
        channel: '#aurum-alerts'
        title: '{{ .GroupLabels.alertname }}'
        text: '{{ .CommonAnnotations.summary }}'
```

## Best Practices

### Metric Collection

1. **Use Appropriate Labels**: Include relevant dimensions (tenant_id, user_id, etc.)
2. **Set Reasonable Buckets**: Choose histogram buckets based on expected ranges
3. **Use Semantic Names**: Follow OpenMetrics conventions
4. **Add Units**: Include units in metric names (seconds, bytes, etc.)

### SLO Definition

1. **Start Simple**: Begin with basic availability and latency SLOs
2. **Use Appropriate Windows**: 30 days for most SLOs, 7 days for data freshness
3. **Set Realistic Targets**: Base targets on current performance
4. **Monitor Compliance**: Track SLO compliance over time

### Alerting

1. **Use Cooldowns**: Prevent alert spam with appropriate cooldown periods
2. **Multi-Channel Routing**: Route critical alerts to multiple channels
3. **Include Context**: Provide actionable information in alert messages
4. **Test Regularly**: Use fault injection to test alerting

### Dashboards

1. **Focus on Key Metrics**: Show the most important KPIs first
2. **Use Appropriate Time Ranges**: Default to 1-7 days
3. **Add Thresholds**: Visual indicators for normal/warning/critical levels
4. **Version Dashboards**: Track dashboard changes over time

## Troubleshooting

### Common Issues

**Metrics Not Appearing:**
- Check Prometheus scraping configuration
- Verify application is exposing `/metrics` endpoint
- Confirm metric names match expected format

**Alerts Not Firing:**
- Check alert rules in Grafana
- Verify alert conditions are met
- Confirm notification channels are configured

**SLOs Not Tracking:**
- Verify SLO definitions in code
- Check that measurements are being recorded
- Confirm Prometheus gauges are being updated

**Dashboards Not Loading:**
- Verify Grafana data sources
- Check query syntax in panels
- Confirm time ranges are appropriate

### Debug Commands

```bash
# Check Prometheus metrics
curl http://localhost:9090/api/v1/query?query=aurum_api_requests_total

# Check alert status
curl http://localhost:9093/api/v1/alerts

# View Grafana dashboard JSON
curl http://localhost:3000/api/dashboards/uid/aurum-api-overview

# Check SLO compliance
python -c "
from aurum.observability.slo_monitor import get_slo_monitor
monitor = get_slo_monitor()
print(monitor.get_slo_summary())
"
```

## Maintenance

### Regular Tasks

1. **Update SLO Targets**: Adjust targets based on performance improvements
2. **Review Alert Rules**: Tune alert thresholds as needed
3. **Update Dashboards**: Add new panels for new metrics
4. **Clean Up Metrics**: Remove unused metrics periodically
5. **Test Alerting**: Run fault injection tests monthly

### Monitoring the Monitoring

- **Alert on AlertManager**: Monitor AlertManager health
- **Dashboard Availability**: Track Grafana uptime
- **Metric Collection**: Monitor Prometheus scraping
- **SLO Compliance**: Track SLO monitoring effectiveness

## Support

For observability and monitoring issues:

1. Check this documentation first
2. Review logs in the observability components
3. Run fault injection tests to verify functionality
4. Contact the observability team for complex issues

---

*This guide is maintained by the Observability team. Last updated: 2024-01-01*
