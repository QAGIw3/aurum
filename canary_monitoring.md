# Canary Dataset Monitoring

This document describes the canary dataset monitoring system for detecting upstream API breaks and data quality issues across all data sources in the Aurum ingestion pipeline.

## Overview

The canary monitoring system provides early detection of:
- Upstream API failures or outages
- Changes in API response formats
- Data quality degradation
- Authentication issues
- Network connectivity problems
- Rate limiting or quota issues

## Architecture

The canary system consists of several key components:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Canary DAG    │───▶│ Canary Manager   │───▶│ API Health      │
│                 │    │                  │    │ Checker         │
│ (Airflow)       │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                               │
                               ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Alert Manager   │    │ Canary Runner    │    │ Data Validation │
│                 │    │                  │    │                 │
│ (Email/Slack/   │◀───┤                  │◀───┤                 │
│  PagerDuty)     │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Components

#### 1. Canary Manager (`aurum.canary.canary_manager`)
- Central registry for all canary datasets
- Manages canary lifecycle and status tracking
- Provides health summaries and critical canary identification

#### 2. API Health Checker (`aurum.canary.api_health_checker`)
- Tests API endpoints for availability and response quality
- Validates response formats and required fields
- Measures response times and detects performance degradation

#### 3. Canary Runner (`aurum.canary.canary_runner`)
- Executes canary checks according to their schedules
- Processes results and determines overall canary health
- Updates canary status and metrics

#### 4. Alert Manager (`aurum.canary.alert_manager`)
- Evaluates canary results against alert rules
- Sends notifications via configured channels
- Tracks alert history and cooldown periods

## Configuration

### Canary Configuration

Each canary is configured with the following parameters:

```python
CanaryConfig(
    name="eia_electricity_canary",
    source="eia",
    dataset="electricity_prices",
    api_endpoint="https://api.eia.gov/v2/electricity/retail-sales/data/",
    api_params={
        "api_key": "DEMO_KEY",
        "frequency": "monthly",
        "data[0]": "price",
        "facets[stateid][]": "CA",
        "start": "2023-01",
        "end": "2023-01",
        "length": "1"
    },
    expected_response_format="json",
    expected_fields=["response", "data"],
    timeout_seconds=30,
    description="EIA electricity prices API health check"
)
```

### Data Sources

The system supports canary monitoring for the following data sources:

| Source | API Endpoint | Sample Dataset | Schedule |
|--------|--------------|----------------|----------|
| EIA | `https://api.eia.gov/v2` | Electricity prices, Petroleum prices | Every 15 minutes |
| FRED | `https://api.stlouisfed.org/fred` | Unemployment rate, GDP | Every 15 minutes |
| CPI | `https://api.stlouisfed.org/fred` | All Items index | Every 15 minutes |
| NOAA | `https://www.ncei.noaa.gov/access/services/data/v1` | Weather data | Every 15 minutes |
| NYISO | `http://mis.nyiso.com/public/api/v1` | LMP data | Every 15 minutes |
| PJM | `https://api.pjm.com/api/v1` | LMP data | Every 15 minutes |
| CAISO | `http://oasis.caiso.com/oasisapi` | OASIS data | Every 15 minutes |

## Usage

### Running Canary Checks

#### Via Airflow DAG
The main canary monitoring runs as an Airflow DAG:

```bash
# Enable the DAG in Airflow
airflow dags unpause canary_dataset_monitor
```

#### Manual Execution
Run canary checks manually for testing:

```bash
# Run all canaries
python scripts/canary/run_canary_checks.py

# Run specific canary
python scripts/canary/run_canary_checks.py test_eia_canary

# Generate comprehensive report
python scripts/canary/run_canary_checks.py --report

# List available test canaries
python scripts/canary/run_canary_checks.py --list
```

### Generating Canary Configurations

Generate canary configurations from data source catalogs:

```bash
# Generate configs for all sources
python scripts/canary/generate_canary_config.py

# Generate configs for specific source
python scripts/canary/generate_canary_config.py --source eia

# Output to specific file
python scripts/canary/generate_canary_config.py --output config/custom_canaries.json
```

## Monitoring and Alerting

### Alert Rules

The system includes several built-in alert rules:

1. **Critical Failures**: Multiple consecutive canary failures
2. **API Timeouts**: Canary execution timeouts
3. **API Unhealthy**: API health check failures
4. **Performance Degradation**: Slow responses with warnings
5. **Recovery**: Canary recovery from previous failures

### Alert Channels

Alerts can be sent via multiple channels:
- Email (default)
- Slack
- PagerDuty (for critical issues)
- Custom webhooks

### Alert Escalation

- **Single Failure**: Email notification
- **Multiple Failures**: Email + Slack
- **Critical Issues**: Email + Slack + PagerDuty

## Data Flow

1. **Schedule**: Airflow DAG triggers canary checks
2. **Execution**: Canary runner executes API health checks
3. **Validation**: Response format and data quality validation
4. **Alerting**: Alert manager evaluates rules and sends notifications
5. **Storage**: Results stored in Kafka for analysis
6. **Monitoring**: Integration with SLA monitoring system

## Troubleshooting

### Common Issues

#### Canary Failing Unexpectedly
1. Check API endpoint availability
2. Verify API credentials and quotas
3. Review response format changes
4. Check network connectivity

#### False Positives
1. Temporary API maintenance
2. Rate limiting
3. Network latency
4. API response format changes

#### Missing Alerts
1. Check alert rule conditions
2. Verify notification channel configuration
3. Review cooldown periods
4. Check alert manager logs

### Debugging

Enable verbose logging for detailed debugging:

```bash
export CANARY_LOG_LEVEL=DEBUG
python scripts/canary/run_canary_checks.py --verbose test_eia_canary
```

### Health Check

Monitor the canary system itself:

```bash
# Check canary manager status
curl http://localhost:8080/api/v1/dags/canary_dataset_monitor/dagRuns

# View recent canary results
python scripts/canary/generate_canary_report.py
```

## Integration

### With SLA Monitoring
Canary failures automatically integrate with the SLA monitoring system to provide comprehensive data pipeline health monitoring.

### With Data Quality Checks
Canary results feed into data quality validation pipelines to detect issues before they affect production data.

### With Alerting Systems
Canary alerts integrate with existing alerting infrastructure for unified incident response.

## Best Practices

### Canary Design
1. **Representative Endpoints**: Choose API endpoints that are representative of typical usage
2. **Minimal Data**: Use small data requests to minimize costs and API quotas
3. **Stable References**: Use well-established datasets that rarely change
4. **Multiple Checks**: Implement multiple canaries per source for redundancy

### Monitoring
1. **Frequent Checks**: Run canaries every 15 minutes for critical APIs
2. **Alert Thresholds**: Set appropriate consecutive failure thresholds
3. **Response Time Monitoring**: Track API response time degradation
4. **Data Validation**: Validate both format and content of responses

### Maintenance
1. **Regular Review**: Review canary configurations quarterly
2. **API Updates**: Update canary endpoints when APIs change
3. **Credential Rotation**: Update API keys as needed
4. **Performance Tuning**: Adjust timeouts and retry settings based on performance

## API Reference

### Canary Manager

```python
# Register a canary
manager = CanaryManager()
config = CanaryConfig(...)
canary = manager.register_canary(config)

# Get canary status
health_summary = manager.get_canary_health_summary()
unhealthy = manager.get_unhealthy_canaries()
```

### API Health Checker

```python
# Run health check
checker = APIHealthChecker()
result = checker.check_api_health("canary_name")

# Batch checks
all_results = checker.check_all_apis()
```

### Alert Manager

```python
# Process results
alert_manager = CanaryAlertManager()
alert_manager.process_canary_result("canary_name", result)

# Get active alerts
active_alerts = alert_manager.get_active_canary_alerts()
```

## Contributing

### Adding New Data Sources

1. Update `CanaryConfigGenerator._load_data_source_configs()`
2. Add sample datasets for the new source
3. Update the Airflow DAG with new canary configurations
4. Add tests for the new canaries

### Custom Alert Rules

1. Extend `CanaryAlertRule` with custom conditions
2. Implement custom alert channels
3. Add rule evaluation logic to `AlertManager`

### Metrics and Dashboards

1. Canary results are published to Kafka topic `aurum.canary.events`
2. Create dashboards to visualize canary health over time
3. Set up monitoring for canary system performance

## Security

- API keys are stored in Vault and not logged
- Canary requests use demo/minimal credentials when possible
- Network traffic is monitored for anomalies
- Alert content is sanitized before sending

## Performance

- Concurrent execution of multiple canaries
- Efficient API connection pooling
- Minimal data transfer for health checks
- Configurable timeouts and retry logic

## Cost Optimization

- Use demo API keys when available
- Minimal data requests (limit=1)
- Efficient batch processing
- Configurable check frequencies per source
