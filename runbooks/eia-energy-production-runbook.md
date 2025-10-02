# EIA Energy Data Pipeline Production Runbook

## 🎯 Overview

This runbook provides comprehensive production deployment and operations guidance for the EIA Energy Data Pipeline, including enhanced data quality checks, robust retry mechanisms, and cost controls.

### Key Improvements in v2.0.0
- **Enhanced Data Quality**: Comprehensive Great Expectations validation with 25+ checks
- **Advanced Retry Logic**: Circuit breaker pattern, exponential backoff, adaptive rate limiting
- **Cost Controls**: $10,000 monthly budget, request quotas, usage tracking
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
- EIA API key (100,000 daily request limit)
- AWS IAM roles for EKS/Kafka/Timescale
- Vault authentication

### 1. Environment Setup

#### Airflow Variables Configuration

```bash
# Core EIA configuration
airflow variables set aurum_eia_api_key "your_production_eia_key_here"
airflow variables set aurum_environment "prod"
airflow variables set aurum_region "us-east-1"
airflow variables set aurum_deployment_id "prod-001"

# Infrastructure endpoints
airflow variables set aurum_kafka_bootstrap_servers "kafka-prod:9092"
airflow variables set aurum_schema_registry "http://schema-registry-prod:8081"
airflow variables set aurum_timescale_jdbc "jdbc:postgresql://timescale-prod:5432/timeseries"

# EIA-specific settings
airflow variables set aurum_eia_timescale_table "eia_series_timeseries"
airflow variables set aurum_eia_dlq_topic "aurum.ref.eia.series.dlq.v1"
airflow variables set aurum_eia_series_topic "aurum.ref.eia.series.v1"
airflow variables set aurum_eia_bulk_topic "aurum.ref.eia.bulk.v1"

# Cost control settings
airflow variables set aurum_eia_daily_request_limit "100000"
airflow variables set aurum_eia_hourly_request_limit "20000"
airflow variables set aurum_eia_monthly_budget "10000.00"
airflow variables set aurum_eia_cost_per_1000_requests "0.005"
```

#### Resource Pools

```bash
# EIA API pool with cost controls
airflow pools set api_eia 3 "EIA API rate limit pool - Production"
airflow pools set api_eia_slots 1 "EIA API slots per task"
```

### 2. Kubernetes Deployment

#### EIA Worker Configuration

```yaml
# k8s/eia-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eia-energy-collector
  namespace: aurum-prod
  labels:
    app: eia-energy-collector
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
      app: eia-energy-collector
  template:
    metadata:
      labels:
        app: eia-energy-collector
        environment: prod
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: aurum-eia-sa
      containers:
      - name: eia-collector
        image: ghcr.io/aurum/eia-collector:v2.0.0
        imagePullPolicy: Always
        env:
        - name: EIA_API_KEY
          valueFrom:
            secretKeyRef:
              name: eia-secrets
              key: api_key
        - name: ENVIRONMENT
          value: "prod"
        - name: DAILY_REQUEST_LIMIT
          value: "100000"
        - name: HOURLY_REQUEST_LIMIT
          value: "20000"
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

#### EIA API Service

```yaml
# k8s/eia-api-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: eia-energy-collector
  namespace: aurum-prod
  labels:
    app: eia-energy-collector
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
    app: eia-energy-collector
```

### 3. Database Setup

#### TimescaleDB Table Schema

```sql
-- Production EIA series table with partitioning
CREATE TABLE IF NOT EXISTS eia_series_timeseries (
    tenant_id VARCHAR(20) NOT NULL DEFAULT 'aurum-prod',
    series_id VARCHAR(50) NOT NULL,
    period DATE NOT NULL,
    period_start DATE,
    period_end DATE,
    frequency VARCHAR(20) NOT NULL,
    value DOUBLE PRECISION,
    raw_value VARCHAR(100),
    unit VARCHAR(50),
    canonical_unit VARCHAR(50),
    canonical_currency VARCHAR(10),
    canonical_value DOUBLE PRECISION,
    conversion_factor DOUBLE PRECISION,
    area VARCHAR(100),
    sector VARCHAR(100),
    seasonal_adjustment VARCHAR(20),
    description TEXT,
    source VARCHAR(50),
    dataset VARCHAR(100),
    metadata JSONB,
    ingest_ts BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000000,
    ingest_job_id VARCHAR(100),
    ingest_run_id VARCHAR(100),
    ingest_batch_id VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, series_id, period)
) PARTITION BY RANGE (period);

-- Create partitions for the next 10 years
SELECT create_hypertable(
    'eia_series_timeseries',
    'period',
    partitioning_column => 'period',
    number_partitions => 120,
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- Create indexes for production queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eia_series_period
ON eia_series_timeseries (series_id, period DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eia_frequency
ON eia_series_timeseries (frequency, period DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eia_sector
ON eia_series_timeseries (sector, period DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eia_area
ON eia_series_timeseries (area, period DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eia_ingest_ts
ON eia_series_timeseries (ingest_ts DESC);

-- Retention policy (keep 15 years of data)
SELECT add_retention_policy('eia_series_timeseries', INTERVAL '15 years');

-- Compression policy (compress data older than 2 years)
SELECT add_compression_policy('eia_series_timeseries',
    compress_after => INTERVAL '2 years',
    if_not_exists => TRUE);

-- Continuous aggregates for performance
CREATE MATERIALIZED VIEW eia_monthly_aggregates
WITH (timescaledb.continuous) AS
SELECT
    series_id,
    frequency,
    sector,
    area,
    time_bucket('1 month', period) AS bucket,
    COUNT(*) as observation_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value,
    SUM(value) as total_value
FROM eia_series_timeseries
GROUP BY series_id, frequency, sector, area, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('eia_monthly_aggregates',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);
```

### 4. Monitoring & Observability

#### Prometheus Metrics

```yaml
# k8s/eia-metrics-service.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: eia-energy-collector
  namespace: aurum-prod
  labels:
    app: eia-energy-collector
spec:
  selector:
    matchLabels:
      app: eia-energy-collector
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
```

#### Alert Rules

```yaml
# k8s/eia-alert-rules.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: eia-energy-alerts
  namespace: aurum-prod
spec:
  groups:
  - name: eia_energy_alerts
    rules:
    # Cost Control Alerts
    - alert: EIAQuotaExceeded
      expr: eia_requests_total > 90000
      for: 5m
      labels:
        severity: critical
        service: eia-energy
        category: cost
      annotations:
        summary: "EIA API quota nearly exceeded"
        description: "EIA requests: {{ $value }} / 100,000 daily limit"

    - alert: EIAHighCostRate
      expr: rate(eia_requests_total[1h]) > 15000
      for: 10m
      labels:
        severity: warning
        service: eia-energy
        category: cost
      annotations:
        summary: "High EIA API request rate"
        description: "Current rate: {{ $value }}/hour"

    # Performance Alerts
    - alert: EIAHighErrorRate
      expr: rate(eia_errors_total[5m]) / rate(eia_requests_total[5m]) > 0.05
      for: 5m
      labels:
        severity: critical
        service: eia-energy
        category: reliability
      annotations:
        summary: "High EIA API error rate"
        description: "Error rate: {{ $value | humanizePercentage }}"

    - alert: EIALatencySpike
      expr: eia_request_duration_p95 > 20
      for: 5m
      labels:
        severity: warning
        service: eia-energy
        category: performance
      annotations:
        summary: "EIA API latency spike"
        description: "95th percentile: {{ $value }} seconds"

    # Data Quality Alerts
    - alert: EIADataStaleness
      expr: time() - eia_last_ingestion_timestamp > 86400
      for: 5m
      labels:
        severity: critical
        service: eia-energy
        category: data_quality
      annotations:
        summary: "EIA data ingestion stalled"
        description: "No data for {{ $value | humanizeDuration }}"

    - alert: EIAQualityDrop
      expr: eia_quality_score < 0.95
      for: 10m
      labels:
        severity: warning
        service: eia-energy
        category: data_quality
      annotations:
        summary: "EIA data quality below threshold"
        description: "Quality score: {{ $value | humanizePercentage }}"
```

## 📊 Production DAGs

### Core Ingestion DAGs

1. **`ingest_eia_series_timescale`** - Daily at 2 AM UTC (Series data)
2. **`ingest_eia_bulk`** - Monthly on 1st at 3 AM UTC (Bulk archives)
3. **`eia_data_monitoring`** - Every 30 minutes
4. **`eia_cost_monitoring`** - Every 6 hours (NEW)

### Monitoring DAGs

5. **`eia_data_monitoring_daily`** - Daily at 9 AM UTC
6. **`eia_staleness_monitor`** - Every 15 minutes
7. **`eia_quality_monitor`** - Every hour

## 🔧 Configuration Management

### EIA Dataset Configuration

The production configuration includes:

```json
{
  "cost_controls": {
    "daily_request_limit": 100000,
    "hourly_request_limit": 20000,
    "monthly_budget_usd": 10000.00,
    "cost_per_1000_requests": 0.005,
    "enable_dynamic_throttling": true
  },
  "alerting": {
    "cost_alerts": {
      "daily_budget_warning_percent": 70,
      "daily_budget_critical_percent": 85,
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

#### 1. EIA API Rate Limiting
```bash
# Check current quota usage
curl -H "X-API-Key: $EIA_API_KEY" "https://api.eia.gov/v2/series/?api_key=$EIA_API_KEY&series_id=PET.W_EPC0_SAX_YCUOK_MBBL.D"

# Enable adaptive rate limiting
airflow variables set aurum_eia_enable_adaptive_rate_limiting "true"
```

#### 2. Cost Overages
```bash
# Check cost metrics
curl http://prometheus-prod:9090/api/v1/query?query=eia_cost_total

# Adjust budget
airflow variables set aurum_eia_monthly_budget "8000.00"
```

#### 3. Circuit Breaker Tripped
```bash
# Check circuit breaker status
curl http://eia-energy-collector:8080/health

# Reset circuit breaker
curl -X POST http://eia-energy-collector:8080/admin/reset-circuit-breaker
```

#### 4. Data Quality Issues
```bash
# Run quality checks manually
airflow tasks test eia_data_monitoring check_data_quality 2024-01-01

# View Great Expectations reports
cat /opt/airflow/logs/eia_data_monitoring/check_data_quality/*.log
```

### Emergency Procedures

#### Stop All EIA DAGs
```bash
airflow dags pause ingest_eia_series_timescale
airflow dags pause ingest_eia_bulk
```

#### Scale Down EIA Workers
```bash
kubectl scale deployment eia-energy-collector --replicas=0 -n aurum-prod
```

#### Reset Quota Tracking
```bash
# Clear quota counters in Redis
redis-cli DEL "eia:quota:daily:2024-01-01"
redis-cli DEL "eia:quota:hourly:2024-01-01-02"
```

## 📈 Performance Tuning

### Production Optimization Settings

#### SeaTunnel Configuration
```properties
# Enhanced SeaTunnel settings for production
seatunnel.engine.checkpoint.interval=30000
seatunnel.engine.checkpoint.timeout=60000
seatunnel.engine.checkpoint.storage.type=hdfs
seatunnel.engine.checkpoint.storage.hdfs.path=hdfs://hdfs-prod:9000/checkpoints/eia
seatunnel.engine.checkpoint.storage.hdfs.user=aurum
```

#### Kafka Producer Settings
```properties
# Optimized for EIA throughput
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
| EIA Worker | 250m | 1000m | 512Mi | 2Gi |
| Airflow Worker | 500m | 2000m | 1Gi | 4Gi |
| Kafka Broker | 1000m | 4000m | 2Gi | 8Gi |
| TimescaleDB | 2000m | 8000m | 4Gi | 16Gi |

## 🔐 Security Considerations

### API Key Management
- Rotate EIA API keys quarterly
- Store in Vault with 1-year TTL
- Monitor key usage patterns
- Alert on unusual access patterns

### Data Encryption
- All EIA data encrypted in transit (TLS 1.3)
- Data at rest encrypted in TimescaleDB
- Kafka messages encrypted end-to-end

### Access Controls
- EIA workers run with minimal privileges
- Network policies restrict API access
- Audit logging enabled for all operations

## 📋 Production Checklist

### Pre-Deployment
- [ ] EIA API key configured and tested
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
- **EIA Liaison**: energy-data@aurum.com

### Emergency
- **On-call Data Engineer**: +1-555-DATA-911
- **EIA API Support**: support@eia.gov

## 📝 Change Log

### v2.0.0 (2024-09-23)
- ✅ Enhanced data quality with 25+ Great Expectations checks
- ✅ Implemented circuit breaker and adaptive rate limiting
- ✅ Added comprehensive cost controls ($10K monthly budget)
- ✅ Production-grade monitoring and alerting
- ✅ High availability configuration
- ✅ Emergency procedures and troubleshooting guides

---

**Last Updated**: 2024-09-23
**Version**: 2.0.0
**Status**: Production Ready
**Owner**: Data Engineering Team
