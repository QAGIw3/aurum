# FRED Economic Data Pipeline Production Runbook

## 🎯 Overview

This runbook provides comprehensive production deployment and operations guidance for the FRED Economic Data Pipeline, including enhanced data quality checks, robust retry mechanisms, and cost controls.

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
- FRED API key (50,000 daily request limit)
- AWS IAM roles for EKS/Kafka/Timescale
- Vault authentication

### 1. Environment Setup

#### Airflow Variables Configuration

```bash
# Core FRED configuration
airflow variables set aurum_fred_api_key "your_production_fred_key_here"
airflow variables set aurum_environment "prod"
airflow variables set aurum_region "us-east-1"
airflow variables set aurum_deployment_id "prod-001"

# Infrastructure endpoints
airflow variables set aurum_kafka_bootstrap_servers "kafka-prod:9092"
airflow variables set aurum_schema_registry "http://schema-registry-prod:8081"
airflow variables set aurum_timescale_jdbc "jdbc:postgresql://timescale-prod:5432/timeseries"

# FRED-specific settings
airflow variables set aurum_fred_timescale_table "fred_series_timeseries"
airflow variables set aurum_fred_dlq_topic "aurum.ref.fred.series.dlq.v1"
airflow variables set aurum_fred_series_topic "aurum.ref.fred.series.v1"

# Cost control settings
airflow variables set aurum_fred_daily_request_limit "50000"
airflow variables set aurum_fred_hourly_request_limit "10000"
airflow variables set aurum_fred_monthly_budget "5000.00"
airflow variables set aurum_fred_cost_per_1000_requests "0.005"
```

#### Resource Pools

```bash
# FRED API pool with cost controls
airflow pools set api_fred 2 "FRED API rate limit pool - Production"
airflow pools set api_fred_slots 1 "FRED API slots per task"
```

### 2. Kubernetes Deployment

#### FRED Worker Configuration

```yaml
# k8s/fred-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fred-economic-collector
  namespace: aurum-prod
  labels:
    app: fred-economic-collector
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
      app: fred-economic-collector
  template:
    metadata:
      labels:
        app: fred-economic-collector
        environment: prod
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: aurum-fred-sa
      containers:
      - name: fred-collector
        image: ghcr.io/aurum/fred-collector:v2.0.0
        imagePullPolicy: Always
        env:
        - name: FRED_API_KEY
          valueFrom:
            secretKeyRef:
              name: fred-secrets
              key: api_key
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

#### FRED API Service

```yaml
# k8s/fred-api-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: fred-economic-collector
  namespace: aurum-prod
  labels:
    app: fred-economic-collector
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
    app: fred-economic-collector
```

### 3. Database Setup

#### TimescaleDB Table Schema

```sql
-- Production FRED series table with partitioning
CREATE TABLE IF NOT EXISTS fred_series_timeseries (
    tenant_id VARCHAR(20) NOT NULL DEFAULT 'aurum-prod',
    series_id VARCHAR(20) NOT NULL,
    obs_date DATE NOT NULL,
    obs_month INTEGER,
    frequency VARCHAR(20) NOT NULL,
    seasonal_adjustment VARCHAR(50),
    value DOUBLE PRECISION,
    raw_value VARCHAR(100),
    units VARCHAR(50),
    title VARCHAR(255),
    notes TEXT,
    metadata JSONB,
    ingest_ts BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000000,
    ingest_job_id VARCHAR(100),
    ingest_run_id VARCHAR(100),
    ingest_batch_id VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, series_id, obs_date)
) PARTITION BY RANGE (obs_date);

-- Create partitions for the next 10 years
SELECT create_hypertable(
    'fred_series_timeseries',
    'obs_date',
    partitioning_column => 'obs_date',
    number_partitions => 120,
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- Create indexes for production queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fred_series_date
ON fred_series_timeseries (series_id, obs_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fred_frequency
ON fred_series_timeseries (frequency, obs_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fred_seasonal_adjustment
ON fred_series_timeseries (seasonal_adjustment, obs_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fred_ingest_ts
ON fred_series_timeseries (ingest_ts DESC);

-- Retention policy (keep 15 years of data)
SELECT add_retention_policy('fred_series_timeseries', INTERVAL '15 years');

-- Compression policy (compress data older than 2 years)
SELECT add_compression_policy('fred_series_timeseries',
    compress_after => INTERVAL '2 years',
    if_not_exists => TRUE);

-- Continuous aggregates for performance
CREATE MATERIALIZED VIEW fred_monthly_aggregates
WITH (timescaledb.continuous) AS
SELECT
    series_id,
    frequency,
    seasonal_adjustment,
    time_bucket('1 month', obs_date) AS bucket,
    COUNT(*) as observation_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value,
    SUM(value) as total_value
FROM fred_series_timeseries
GROUP BY series_id, frequency, seasonal_adjustment, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('fred_monthly_aggregates',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);
```

### 4. Monitoring & Observability

#### Prometheus Metrics

```yaml
# k8s/fred-metrics-service.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: fred-economic-collector
  namespace: aurum-prod
  labels:
    app: fred-economic-collector
spec:
  selector:
    matchLabels:
      app: fred-economic-collector
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
```

#### Alert Rules

```yaml
# k8s/fred-alert-rules.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: fred-economic-alerts
  namespace: aurum-prod
spec:
  groups:
  - name: fred_economic_alerts
    rules:
    # Cost Control Alerts
    - alert: FREDQuotaExceeded
      expr: fred_requests_total > 45000
      for: 5m
      labels:
        severity: critical
        service: fred-economic
        category: cost
      annotations:
        summary: "FRED API quota nearly exceeded"
        description: "FRED requests: {{ $value }} / 50,000 daily limit"

    - alert: FREDHighCostRate
      expr: rate(fred_requests_total[1h]) > 8000
      for: 10m
      labels:
        severity: warning
        service: fred-economic
        category: cost
      annotations:
        summary: "High FRED API request rate"
        description: "Current rate: {{ $value }}/hour"

    # Performance Alerts
    - alert: FREDHighErrorRate
      expr: rate(fred_errors_total[5m]) / rate(fred_requests_total[5m]) > 0.05
      for: 5m
      labels:
        severity: critical
        service: fred-economic
        category: reliability
      annotations:
        summary: "High FRED API error rate"
        description: "Error rate: {{ $value | humanizePercentage }}"

    - alert: FREDLatencySpike
      expr: fred_request_duration_p95 > 20
      for: 5m
      labels:
        severity: warning
        service: fred-economic
        category: performance
      annotations:
        summary: "FRED API latency spike"
        description: "95th percentile: {{ $value }} seconds"

    # Data Quality Alerts
    - alert: FREDDataStaleness
      expr: time() - fred_last_ingestion_timestamp > 86400
      for: 5m
      labels:
        severity: critical
        service: fred-economic
        category: data_quality
      annotations:
        summary: "FRED data ingestion stalled"
        description: "No data for {{ $value | humanizeDuration }}"

    - alert: FREDQualityDrop
      expr: fred_quality_score < 0.95
      for: 10m
      labels:
        severity: warning
        service: fred-economic
        category: data_quality
      annotations:
        summary: "FRED data quality below threshold"
        description: "Quality score: {{ $value | humanizePercentage }}"
```

## 📊 Production DAGs

### Core Ingestion DAGs

1. **`ingest_fred_series_timescale`** - Daily at 6 AM UTC (Economic series data)
2. **`fred_data_monitoring`** - Every 30 minutes
3. **`fred_cost_monitoring`** - Every 6 hours (NEW)

### Monitoring DAGs

4. **`fred_data_monitoring_daily`** - Daily at 9 AM UTC
5. **`fred_staleness_monitor`** - Every 15 minutes
6. **`fred_quality_monitor`** - Every hour

## 🔧 Configuration Management

### FRED Dataset Configuration

The production configuration includes:

```json
{
  "cost_controls": {
    "daily_request_limit": 50000,
    "hourly_request_limit": 10000,
    "monthly_budget_usd": 5000.00,
    "cost_per_1000_requests": 0.005,
    "enable_dynamic_throttling": true
  },
  "alerting": {
    "cost_alerts": {
      "daily_budget_warning_percent": 75,
      "daily_budget_critical_percent": 90,
      "hourly_rate_spike_percent": 150
    },
    "performance_alerts": {
      "average_response_time_ms_threshold": 15000,
      "error_rate_threshold_percent": 5,
      "retry_rate_threshold_percent": 20
    }
  }
}
```

## 🛠️ Troubleshooting

### Common Production Issues

#### 1. FRED API Rate Limiting
```bash
# Check current quota usage
curl -H "X-API-Key: $FRED_API_KEY" "https://api.stlouisfed.org/fred/series?series_id=DGS10"

# Enable adaptive rate limiting
airflow variables set aurum_fred_enable_adaptive_rate_limiting "true"
```

#### 2. Cost Overages
```bash
# Check cost metrics
curl http://prometheus-prod:9090/api/v1/query?query=fred_cost_total

# Adjust budget
airflow variables set aurum_fred_monthly_budget "4000.00"
```

#### 3. Circuit Breaker Tripped
```bash
# Check circuit breaker status
curl http://fred-economic-collector:8080/health

# Reset circuit breaker
curl -X POST http://fred-economic-collector:8080/admin/reset-circuit-breaker
```

#### 4. Data Quality Issues
```bash
# Run quality checks manually
airflow tasks test fred_data_monitoring check_data_quality 2024-01-01

# View Great Expectations reports
cat /opt/airflow/logs/fred_data_monitoring/check_data_quality/*.log
```

### Emergency Procedures

#### Stop All FRED DAGs
```bash
airflow dags pause ingest_fred_series_timescale
```

#### Scale Down FRED Workers
```bash
kubectl scale deployment fred-economic-collector --replicas=0 -n aurum-prod
```

#### Reset Quota Tracking
```bash
# Clear quota counters in Redis
redis-cli DEL "fred:quota:daily:2024-01-01"
redis-cli DEL "fred:quota:hourly:2024-01-01-06"
```

## 📈 Performance Tuning

### Production Optimization Settings

#### SeaTunnel Configuration
```properties
# Enhanced SeaTunnel settings for production
seatunnel.engine.checkpoint.interval=30000
seatunnel.engine.checkpoint.timeout=60000
seatunnel.engine.checkpoint.storage.type=hdfs
seatunnel.engine.checkpoint.storage.hdfs.path=hdfs://hdfs-prod:9000/checkpoints/fred
seatunnel.engine.checkpoint.storage.hdfs.user=aurum
```

#### Kafka Producer Settings
```properties
# Optimized for FRED throughput
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
| FRED Worker | 250m | 1000m | 512Mi | 2Gi |
| Airflow Worker | 500m | 2000m | 1Gi | 4Gi |
| Kafka Broker | 1000m | 4000m | 2Gi | 8Gi |
| TimescaleDB | 2000m | 8000m | 4Gi | 16Gi |

## 🔐 Security Considerations

### API Key Management
- Rotate FRED API keys quarterly
- Store in Vault with 1-year TTL
- Monitor key usage patterns
- Alert on unusual access patterns

### Data Encryption
- All FRED data encrypted in transit (TLS 1.3)
- Data at rest encrypted in TimescaleDB
- Kafka messages encrypted end-to-end

### Access Controls
- FRED workers run with minimal privileges
- Network policies restrict API access
- Audit logging enabled for all operations

## 📋 Production Checklist

### Pre-Deployment
- [ ] FRED API key configured and tested
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
- **Data Freshness**: 95th percentile < 24 hours
- **API Availability**: 99.9% uptime
- **Data Quality**: >95% validation pass rate
- **Cost Control**: Within 10% of budget

### Key Performance Indicators
- **Request Success Rate**: >98%
- **Average Response Time**: <10 seconds
- **Data Completeness**: >99%
- **Cost Efficiency**: <0.005 USD per 1000 requests

## 📞 Support Contacts

### Primary
- **Data Engineering**: data-team@aurum.com
- **Platform SRE**: sre-team@aurum.com
- **FRED Liaison**: economic-data@aurum.com

### Emergency
- **On-call Data Engineer**: +1-555-DATA-911
- **FRED API Support**: support@stlouisfed.org

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
