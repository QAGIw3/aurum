# External Data Production Runbook

This runbook provides comprehensive operational procedures for managing external data collection, processing, and monitoring in production.

## Overview

The external data pipeline consists of the following components:

- **Data Collection**: FRED, EIA, NOAA, and WorldBank data collectors
- **Data Processing**: Backfill and incremental processing pipelines
- **Data Validation**: Great Expectations-based quality checks
- **Data Lineage**: Tracking data flow and transformations
- **Monitoring**: Health checks, SLA monitoring, and alerting

## Architecture

```
External APIs → Collectors → Kafka → Processing → Validation → Storage → Monitoring
```

- **Collectors**: Python-based services that fetch data from external APIs
- **Kafka**: Message bus for data streaming and buffering
- **Processing**: Airflow DAGs for backfill and incremental processing
- **Validation**: Great Expectations for data quality checks
- **Storage**: PostgreSQL for checkpoints, Iceberg for data lake
- **Monitoring**: Prometheus metrics, Grafana dashboards, Slack alerts

## Quick Start

### 1. Environment Setup

```bash
# Activate virtual environment
source .venv/bin/activate

# Set required environment variables
export AURUM_APP_DB_DSN="postgresql://aurum:aurum@postgres:5432/aurum"
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
export SCHEMA_REGISTRY_URL="http://schema-registry:8081"
export VAULT_ADDR="http://vault:8200"
export VAULT_TOKEN="aurum-dev-token"

# API Keys (via Vault or environment)
export FRED_API_KEY="your-fred-api-key"
export EIA_API_KEY="your-eia-api-key"
export NOAA_GHCND_TOKEN="your-noaa-token"
```

### 2. Start Services

```bash
# Start external data collectors
python -m aurum.external.runner --providers fred,eia,noaa --loop --interval 3600

# Or run specific provider
python -m aurum.external.runner --providers fred --catalog-only
```

### 3. Monitor Pipeline

```bash
# Check pipeline health
curl http://localhost:8080/health/external

# Check SLA compliance
curl http://localhost:8080/sla/external
```

## Operational Procedures

### Starting the Pipeline

#### 1. Pre-Flight Checks

```bash
# Check database connectivity
psql $AURUM_APP_DB_DSN -c "SELECT 1"

# Check Kafka connectivity
kafka-console-producer --broker-list $KAFKA_BOOTSTRAP_SERVERS --topic test

# Check schema registry
curl $SCHEMA_REGISTRY_URL/subjects

# Check API key validity
curl -H "api_key: $FRED_API_KEY" "https://api.stlouisfed.org/fred/series?series_id=DEXUSEU"
```

#### 2. Start Collectors

```bash
# Start all providers in loop mode
python -m aurum.external.runner \
    --providers fred,eia,noaa,worldbank \
    --loop \
    --interval 3600 \
    --log-level INFO

# Start individual provider for debugging
python -m aurum.external.runner \
    --providers fred \
    --catalog-only \
    --log-level DEBUG
```

#### 3. Start Airflow DAGs

```bash
# Trigger incremental DAG
airflow dags trigger external_incremental

# Trigger backfill DAG (manual only)
airflow dags trigger external_backfill \
    -c '{"providers": [{"name": "fred", "datasets": ["DEXUSEU"]}]}'
```

### Monitoring Pipeline Health

#### Health Check Endpoints

```bash
# Overall pipeline health
curl http://localhost:8080/v1/admin/external/health

# Provider-specific health
curl http://localhost:8080/v1/admin/external/health/fred
curl http://localhost:8080/v1/admin/external/health/eia
```

#### Metrics Dashboard

Access Grafana at `http://localhost:3000` and navigate to:

- **External Data Overview**: `aurum-external-overview`
- **Provider Metrics**: `aurum-external-providers`
- **Data Quality**: `aurum-external-quality`
- **SLA Compliance**: `aurum-external-sla`

#### Log Monitoring

```bash
# Follow collector logs
journalctl -u aurum-external-collector -f

# Check Airflow logs
airflow dags list-runs external_incremental
```

### Troubleshooting

#### Common Issues

##### 1. API Rate Limiting

**Symptoms**: HTTP 429 errors, slow data collection

**Solution**:
```bash
# Check current rate limits
curl http://localhost:8080/v1/admin/external/ratelimits

# Adjust rate limits if needed
python -c "
from aurum.external.providers.noaa import NoaaRateLimiter
limiter = NoaaRateLimiter(rate_per_sec=2)
# Adjust rate as needed
"
```

##### 2. Data Quality Failures

**Symptoms**: Great Expectations validation failures

**Solution**:
```bash
# Check validation results
curl http://localhost:8080/v1/admin/external/validation/fred

# Review failed expectations
airflow tasks test external_incremental validate_incremental_fred 2024-01-01

# Fix data issues or update expectations
python -c "
from aurum.external.ge_validation import GreatExpectationsValidator
validator = GreatExpectationsValidator()
# Update expectation suites as needed
"
```

##### 3. Kafka Backpressure

**Symptoms**: Slow message publishing, high latency

**Solution**:
```bash
# Check Kafka lag
kafka-consumer-groups --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --group aurum-external-consumers \
    --describe

# Scale Kafka if needed
kubectl scale statefulset kafka --replicas=5
```

##### 4. SLA Violations

**Symptoms**: Data freshness > 24 hours, processing delays

**Solution**:
```bash
# Check SLA status
curl http://localhost:8080/v1/admin/external/sla

# Investigate root cause
airflow dags list-runs external_incremental
journalctl -u aurum-external-collector --since "24 hours ago"

# Trigger manual run if needed
airflow dags trigger external_incremental
```

### Performance Tuning

#### Rate Limiting Configuration

```python
# Adjust provider-specific rate limits
RATE_LIMITS = {
    "fred": {"requests_per_second": 10, "daily_quota": 1000},
    "eia": {"requests_per_second": 5, "daily_quota": 1000},
    "noaa": {"requests_per_second": 2, "daily_quota": 1000},
    "worldbank": {"requests_per_second": 5, "daily_quota": None}
}

# Apply rate limits
for provider, limits in RATE_LIMITS.items():
    if limits["daily_quota"]:
        quota = DailyQuota(limit=limits["daily_quota"])
    else:
        quota = None
    rate_limiter = RateLimiter(rate=limits["requests_per_second"])
```

#### Batch Processing Configuration

```python
# Adjust batch sizes for optimal throughput
BATCH_CONFIG = {
    "fred": {"batch_size": 500, "max_workers": 4},
    "eia": {"batch_size": 1000, "max_workers": 2},
    "noaa": {"batch_size": 200, "max_workers": 8},
    "worldbank": {"batch_size": 500, "max_workers": 4}
}
```

#### Memory and CPU Tuning

```yaml
# Kubernetes resource limits for collectors
resources:
  limits:
    cpu: "2000m"
    memory: "4Gi"
  requests:
    cpu: "100m"
    memory: "512Mi"

# Kafka partition configuration
kafka:
  partitions: 12
  replication_factor: 3
```

### Data Quality Management

#### Great Expectations Setup

```bash
# Initialize Great Expectations context
great_expectations init

# Create expectation suites
python -c "
from aurum.external.ge_validation import GreatExpectationsValidator
validator = GreatExpectationsValidator()
# Create suites for each data source
"

# Run validation tests
python -c "
import asyncio
from aurum.external.ge_validation import run_ge_validation

async def test():
    result = await run_ge_validation(
        table_name='external.timeseries_observation_fred',
        expectation_suite='external_timeseries_obs',
        vault_addr='http://vault:8200',
        vault_token='token'
    )
    print(result)

asyncio.run(test())
"
```

#### Quality Score Monitoring

```python
# Monitor quality scores programmatically
from aurum.external.monitoring import get_external_monitor

async def check_quality():
    monitor = get_external_monitor()

    # Check quality for each provider
    providers = ["fred", "eia", "noaa", "worldbank"]
    for provider in providers:
        score = await monitor._quality_monitor.get_quality_score(provider, "all")
        acceptable = await monitor._quality_monitor.is_quality_acceptable(provider, "all")
        print(f"{provider}: score={score:.3f}, acceptable={acceptable}")

asyncio.run(check_quality())
```

### Emergency Procedures

#### Circuit Breaker

```python
# Enable circuit breaker for failing providers
CIRCUIT_BREAKER_CONFIG = {
    "fred": {"failure_threshold": 5, "timeout_minutes": 30},
    "eia": {"failure_threshold": 3, "timeout_minutes": 60},
    "noaa": {"failure_threshold": 10, "timeout_minutes": 15},
    "worldbank": {"failure_threshold": 5, "timeout_minutes": 30}
}
```

#### Data Recovery

```bash
# Manual data recovery from backups
psql $AURUM_APP_DB_DSN -c "
    RESTORE TABLE external.timeseries_observation_fred
    FROM 's3://aurum-backups/external/fred/2024-01-01/timeseries_observation.sql'
"

# Re-run failed processing
airflow dags backfill external_incremental \
    --start-date 2024-01-01 \
    --end-date 2024-01-02
```

#### Provider Failover

```python
# Switch to backup providers if primary fails
BACKUP_PROVIDERS = {
    "fred": "worldbank",
    "eia": "fred",
    "noaa": "eia",
    "worldbank": "fred"
}

# Implement failover logic
if primary_provider_fails:
    switch_to_backup_provider(BACKUP_PROVIDERS[provider])
```

### Security and Compliance

#### API Key Management

```bash
# Rotate API keys
vault kv put secret/external/fred api_key=new_fred_key
vault kv put secret/external/eia api_key=new_eia_key

# Audit key usage
vault audit enable file file_path=/var/log/vault/external-keys.log
```

#### Data Retention

```sql
-- Clean up old data
DELETE FROM external.timeseries_observation_fred
WHERE timestamp < NOW() - INTERVAL '5 years';

-- Vacuum tables
VACUUM ANALYZE external.timeseries_observation_fred;
```

#### Compliance Checks

```bash
# Run compliance checks
python -c "
from aurum.external.monitoring import run_compliance_check
result = run_compliance_check('fred')
print('Compliance:', result)
"
```

## Contact Information

- **On-call Engineer**: @external-data-team
- **Slack Channel**: #external-data
- **Email**: external-data@aurum.com
- **Runbook Owner**: data-engineering@aurum.com

## Version History

- **v1.0.0**: Initial production deployment
- **v1.1.0**: Added Great Expectations validation
- **v1.2.0**: Enhanced monitoring and alerting
- **v1.3.0**: Improved error handling and recovery

---

*This runbook is maintained by the Data Engineering team. Last updated: 2024-01-01*
