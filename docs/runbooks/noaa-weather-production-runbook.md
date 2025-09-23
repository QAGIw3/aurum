# NOAA Weather Data Pipeline Production Runbook

## 🎯 Overview

This runbook provides comprehensive production deployment and operations guidance for the NOAA Weather Data Pipeline, including enhanced data quality checks, robust retry mechanisms, and cost controls.

### Key Improvements in v2.0.0
- **Enhanced Data Quality**: Comprehensive Great Expectations validation with 20+ checks
- **Advanced Retry Logic**: Circuit breaker pattern, exponential backoff, adaptive rate limiting
- **Cost Controls**: $5,000 monthly budget, request quotas, usage tracking
- **Production Monitoring**: Real-time alerts, performance metrics, SLA compliance
- **High Availability**: Multi-region deployment, failover capabilities

## 🚀 Production Deployment

### Prerequisites

**Required Infrastructure**:
- ✅ Kubernetes cluster (EKS/GKE) with 3+ nodes
- ✅ Kafka cluster with Schema Registry (Confluent/Strimzi)
- ✅ TimescaleDB cluster (3+ replicas)
- ✅ Airflow cluster (CeleryExecutor with 5+ workers)
- ✅ Monitoring stack (Prometheus, Grafana, AlertManager)
- ✅ Secret management (Vault/ASM)

**Required Credentials**:
- NOAA CDO Web API token (50,000 daily request limit)
- AWS IAM roles for EKS/Kafka/Timescale
- Vault authentication

### 1. Environment Setup

#### Airflow Variables Configuration

```bash
# Core NOAA configuration
airflow variables set aurum_noaa_api_token "your_production_token_here"
airflow variables set aurum_environment "prod"
airflow variables set aurum_region "us-east-1"
airflow variables set aurum_deployment_id "prod-001"

# Infrastructure endpoints
airflow variables set aurum_kafka_bootstrap_servers "kafka-prod:9092"
airflow variables set aurum_schema_registry "http://schema-registry-prod:8081"
airflow variables set aurum_timescale_jdbc "jdbc:postgresql://timescale-prod:5432/timeseries"

# NOAA-specific settings
airflow variables set aurum_noaa_timescale_table "noaa_weather_timeseries"
airflow variables set aurum_noaa_dlq_topic "aurum.ref.noaa.weather.dlq.v1"
airflow variables set aurum_noaa_daily_topic "aurum.ref.noaa.weather.ghcnd.daily.v1"
airflow variables set aurum_noaa_hourly_topic "aurum.ref.noaa.weather.ghcnd.hourly.v1"
airflow variables set aurum_noaa_monthly_topic "aurum.ref.noaa.weather.gsom.monthly.v1"
```

#### Resource Pools

```bash
# NOAA API pool with cost controls
airflow pools set api_noaa 5 "NOAA API rate limit pool - Production"
airflow pools set api_noaa_slots 1 "NOAA API slots per task"
```

### 2. Kubernetes Deployment

#### NOAA Worker Configuration

```yaml
# k8s/noaa-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: noaa-weather-collector
  namespace: aurum-prod
  labels:
    app: noaa-weather-collector
    environment: prod
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 1
  selector:
    matchLabels:
      app: noaa-weather-collector
  template:
    metadata:
      labels:
        app: noaa-weather-collector
        environment: prod
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: aurum-noaa-sa
      containers:
      - name: noaa-collector
        image: ghcr.io/aurum/noaa-collector:v2.0.0
        imagePullPolicy: Always
        env:
        - name: NOAA_API_TOKEN
          valueFrom:
            secretKeyRef:
              name: noaa-secrets
              key: api_token
        - name: ENVIRONMENT
          value: "prod"
        - name: DAILY_REQUEST_LIMIT
          value: "50000"
        - name: HOURLY_REQUEST_LIMIT
          value: "10000"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 9090
          name: metrics
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
```

#### NOAA API Service

```yaml
# k8s/noaa-api-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: noaa-weather-collector
  namespace: aurum-prod
  labels:
    app: noaa-weather-collector
spec:
  type: ClusterIP
  ports:
  - port: 8080
    targetPort: 8080
    protocol: TCP
    name: http
  - port: 9090
    targetPort: 9090
    protocol: TCP
    name: metrics
  selector:
    app: noaa-weather-collector
```

### 3. Database Setup

#### TimescaleDB Table Schema

```sql
-- Production NOAA weather table with partitioning
CREATE TABLE IF NOT EXISTS noaa_weather_timeseries (
    tenant_id VARCHAR(20) NOT NULL DEFAULT 'aurum-prod',
    station_id VARCHAR(20) NOT NULL,
    station_name VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    elevation_m DOUBLE PRECISION,
    dataset VARCHAR(50) NOT NULL,
    observation_date DATE NOT NULL,
    element VARCHAR(20) NOT NULL,
    value DOUBLE PRECISION,
    raw_value VARCHAR(50),
    unit VARCHAR(20),
    observation_time BIGINT,
    measurement_flag VARCHAR(5),
    quality_flag VARCHAR(5),
    source_flag VARCHAR(5),
    attributes TEXT,
    ingest_ts BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000000,
    ingest_job_id VARCHAR(100),
    ingest_run_id VARCHAR(100),
    ingest_batch_id VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, station_id, observation_date, element, dataset)
) PARTITION BY RANGE (observation_date);

-- Create partitions for the next 5 years
SELECT create_hypertable(
    'noaa_weather_timeseries',
    'observation_date',
    partitioning_column => 'observation_date',
    number_partitions => 60,
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- Create indexes for production queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_noaa_station_date
ON noaa_weather_timeseries (station_id, observation_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_noaa_element
ON noaa_weather_timeseries (element, observation_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_noaa_dataset
ON noaa_weather_timeseries (dataset, observation_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_noaa_ingest_ts
ON noaa_weather_timeseries (ingest_ts DESC);

-- Retention policy (keep 10 years of data)
SELECT add_retention_policy('noaa_weather_timeseries', INTERVAL '10 years');

-- Compression policy (compress data older than 1 year)
SELECT add_compression_policy('noaa_weather_timeseries',
    compress_after => INTERVAL '1 year',
    if_not_exists => TRUE);

-- Continuous aggregates for performance
CREATE MATERIALIZED VIEW noaa_daily_aggregates
WITH (timescaledb.continuous) AS
SELECT
    station_id,
    dataset,
    element,
    time_bucket('1 day', observation_date) AS bucket,
    COUNT(*) as observation_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value
FROM noaa_weather_timeseries
GROUP BY station_id, dataset, element, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('noaa_daily_aggregates',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);
```

### 4. Monitoring & Observability

#### Prometheus Metrics

```yaml
# k8s/noaa-metrics-service.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: noaa-weather-collector
  namespace: aurum-prod
  labels:
    app: noaa-weather-collector
spec:
  selector:
    matchLabels:
      app: noaa-weather-collector
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
```

#### Alert Rules

```yaml
# k8s/noaa-alert-rules.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: noaa-weather-alerts
  namespace: aurum-prod
spec:
  groups:
  - name: noaa_weather_alerts
    rules:
    # Cost Control Alerts
    - alert: NOAAQuotaExceeded
      expr: noaa_requests_total > 45000
      for: 5m
      labels:
        severity: critical
        service: noaa-weather
        category: cost
      annotations:
        summary: "NOAA API quota nearly exceeded"
        description: "NOAA requests: {{ $value }} / 50,000 daily limit"

    - alert: NOAAHighCostRate
      expr: rate(noaa_requests_total[1h]) > 8000
      for: 10m
      labels:
        severity: warning
        service: noaa-weather
        category: cost
      annotations:
        summary: "High NOAA API request rate"
        description: "Current rate: {{ $value }}/hour"

    # Performance Alerts
    - alert: NOAAHighErrorRate
      expr: rate(noaa_errors_total[5m]) / rate(noaa_requests_total[5m]) > 0.05
      for: 5m
      labels:
        severity: critical
        service: noaa-weather
        category: reliability
      annotations:
        summary: "High NOAA API error rate"
        description: "Error rate: {{ $value | humanizePercentage }}"

    - alert: NOAALatencySpike
      expr: noaa_request_duration_p95 > 15
      for: 5m
      labels:
        severity: warning
        service: noaa-weather
        category: performance
      annotations:
        summary: "NOAA API latency spike"
        description: "95th percentile: {{ $value }} seconds"

    # Data Quality Alerts
    - alert: NOAADataStaleness
      expr: time() - noaa_last_ingestion_timestamp > 7200
      for: 5m
      labels:
        severity: critical
        service: noaa-weather
        category: data_quality
      annotations:
        summary: "NOAA data ingestion stalled"
        description: "No data for {{ $value | humanizeDuration }}"

    - alert: NOAAQualityDrop
      expr: noaa_quality_score < 0.95
      for: 10m
      labels:
        severity: warning
        service: noaa-weather
        category: data_quality
      annotations:
        summary: "NOAA data quality below threshold"
        description: "Quality score: {{ $value | humanizePercentage }}"
```

## 📊 Production DAGs

### Core Ingestion DAGs

1. **`noaa_data_ingestion`** - Manual trigger with dataset configuration
2. **`noaa_ghcnd_daily_ingest`** - Daily at 6 AM UTC
3. **`noaa_ghcnd_hourly_ingest`** - Every 6 hours
4. **`noaa_gsom_monthly_ingest`** - Monthly on 1st at 8 AM UTC

### Monitoring DAGs

5. **`noaa_data_monitoring`** - Every 15 minutes
6. **`noaa_data_monitoring_daily`** - Daily at 9 AM UTC
7. **`noaa_cost_monitoring`** - Every 6 hours (NEW)

## 🔧 Configuration Management

### NOAA Dataset Configuration

The production configuration includes:

```json
{
  "cost_controls": {
    "daily_request_limit": 50000,
    "hourly_request_limit": 10000,
    "monthly_budget_usd": 5000.00,
    "cost_per_1000_requests": 0.01,
    "enable_dynamic_throttling": true
  },
  "alerting": {
    "cost_alerts": {
      "daily_budget_warning_percent": 75,
      "daily_budget_critical_percent": 90,
      "hourly_rate_spike_percent": 150
    },
    "performance_alerts": {
      "average_response_time_ms_threshold": 10000,
      "error_rate_threshold_percent": 5,
      "retry_rate_threshold_percent": 20
    }
  }
}
```

## 🛠️ Troubleshooting

### Common Production Issues

#### 1. NOAA API Rate Limiting
```bash
# Check current quota usage
curl -H "token: $NOAA_TOKEN" "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets" | jq '.metadata'

# Enable adaptive rate limiting
airflow variables set aurum_noaa_enable_adaptive_rate_limiting "true"
```

#### 2. Cost Overages
```bash
# Check cost metrics
curl http://prometheus-prod:9090/api/v1/query?query=noaa_cost_total

# Adjust budget
airflow variables set aurum_noaa_monthly_budget "3000.00"
```

#### 3. Circuit Breaker Tripped
```bash
# Check circuit breaker status
curl http://noaa-weather-collector:8080/health

# Reset circuit breaker
curl -X POST http://noaa-weather-collector:8080/admin/reset-circuit-breaker
```

#### 4. Data Quality Issues
```bash
# Run quality checks manually
airflow tasks test noaa_data_monitoring check_data_quality 2024-01-01

# View Great Expectations reports
cat /opt/airflow/logs/noaa_data_monitoring/check_data_quality/*.log
```

### Emergency Procedures

#### Stop All NOAA DAGs
```bash
airflow dags pause noaa_data_ingestion
airflow dags pause noaa_ghcnd_daily_ingest
airflow dags pause noaa_ghcnd_hourly_ingest
airflow dags pause noaa_gsom_monthly_ingest
```

#### Scale Down NOAA Workers
```bash
kubectl scale deployment noaa-weather-collector --replicas=0 -n aurum-prod
```

#### Reset Quota Tracking
```bash
# Clear quota counters in Redis
redis-cli DEL "noaa:quota:daily:2024-01-01"
redis-cli DEL "noaa:quota:hourly:2024-01-01-06"
```

## 📈 Performance Tuning

### Production Optimization Settings

#### SeaTunnel Configuration
```properties
# Enhanced SeaTunnel settings for production
seatunnel.engine.checkpoint.interval=30000
seatunnel.engine.checkpoint.timeout=60000
seatunnel.engine.checkpoint.storage.type=hdfs
seatunnel.engine.checkpoint.storage.hdfs.path=hdfs://hdfs-prod:9000/checkpoints/noaa
seatunnel.engine.checkpoint.storage.hdfs.user=aurum
```

#### Kafka Producer Settings
```properties
# Optimized for NOAA throughput
kafka.producer.acks=all
kafka.producer.compression.type=snappy
kafka.producer.batch.size=32768
kafka.producer.linger.ms=20
kafka.producer.buffer.memory=67108864
kafka.producer.max.in.flight.requests.per.connection=5
```

### Resource Allocation

| Component | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-----------|-------------|-----------|----------------|--------------|
| NOAA Worker | 250m | 1000m | 512Mi | 2Gi |
| Airflow Worker | 500m | 2000m | 1Gi | 4Gi |
| Kafka Broker | 1000m | 4000m | 2Gi | 8Gi |
| TimescaleDB | 2000m | 8000m | 4Gi | 16Gi |

## 🔐 Security Considerations

### API Token Management
- Rotate NOAA tokens quarterly
- Store in Vault with 1-year TTL
- Monitor token usage patterns
- Alert on unusual access patterns

### Data Encryption
- All NOAA data encrypted in transit (TLS 1.3)
- Data at rest encrypted in TimescaleDB
- Kafka messages encrypted end-to-end

### Access Controls
- NOAA workers run with minimal privileges
- Network policies restrict API access
- Audit logging enabled for all operations

## 📋 Production Checklist

### Pre-Deployment
- [ ] NOAA API token configured and tested
- [ ] Cost controls implemented and validated
- [ ] Data quality checks passing
- [ ] Monitoring and alerting configured
- [ ] Circuit breaker patterns tested
- [ ] Emergency procedures documented
- [ ] Team training completed

### Post-Deployment
- [ ] All DAGs running successfully
- [ ] Data flowing to TimescaleDB
- [ ] Cost tracking operational
- [ ] Quality metrics above 95%
- [ ] Alerts firing correctly
- [ ] Documentation updated
- [ ] Stakeholder communication sent

## 🎯 SLA & KPIs

### Service Level Agreements
- **Data Freshness**: 95th percentile < 2 hours
- **API Availability**: 99.9% uptime
- **Data Quality**: >95% validation pass rate
- **Cost Control**: Within 10% of budget

### Key Performance Indicators
- **Request Success Rate**: >98%
- **Average Response Time**: <5 seconds
- **Data Completeness**: >99%
- **Cost Efficiency**: <0.01 USD per 1000 requests

## 📞 Support Contacts

### Primary
- **Data Engineering**: data-team@aurum.com
- **Platform SRE**: sre-team@aurum.com
- **NOAA Liaison**: weather-data@aurum.com

### Emergency
- **On-call Data Engineer**: +1-555-DATA-911
- **NOAA API Support**: cdo_help@ncdc.noaa.gov

## 📝 Change Log

### v2.0.0 (2024-09-23)
- ✅ Enhanced data quality with 20+ Great Expectations checks
- ✅ Implemented circuit breaker and adaptive rate limiting
- ✅ Added comprehensive cost controls ($5K monthly budget)
- ✅ Production-grade monitoring and alerting
- ✅ High availability configuration
- ✅ Emergency procedures and troubleshooting guides

---

**Last Updated**: 2024-09-23
**Version**: 2.0.0
**Status**: Production Ready
**Owner**: Data Engineering Team
