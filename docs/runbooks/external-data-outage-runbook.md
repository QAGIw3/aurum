# External Data Outage Runbook

This runbook details how to detect, mitigate, and recover from outages that
impact third-party data feeds (EIA, ISO/RTO providers, NOAA, etc.) consumed by
Aurum’s external ingestion pipelines.

## 1. Know The Surface Area

| Component | Where to check | Notes |
| --- | --- | --- |
| Airflow collectors | `airflow webserver` → *External Ingestion* dashboard | DAGs: `external_data_ingest`, `external_backfill` |
| SeaTunnel streaming jobs | `seatunnel/jobs/templates/*.conf.tmpl` + Kubernetes job logs | Handles ISO feeds like ISONE → Kafka |
| Scenario data consumers | API `/v2/scenarios/*` (Trino backing tables) | Downstream freshness SLAs: < 15 minutes |
| Data Quality | Great Expectations checkpoints (`lakefs/actions/data_quality.yaml`) | Alerts to #data-platform when suites fail |

## 2. Detection & Initial Assessment

1. **Alert review** – PagerDuty “External Data Pipeline” service alerts on:
   - Missing DAG heartbeats > 15 minutes
   - Metric `external_data_last_success_timestamp` stale in Prometheus
   - Quality suite failure for `external_dbt_outputs`
2. **Airflow state** – `airflow dags state external_data_ingest $(date -I)`
3. **Provider status** – Check vendor status pages (EIA, ISO-NE, MISO) and our
   cached status in `redis-cli --raw get external:provider_status:<provider>`.
4. **Scope the blast radius** – Identify affected datasets, e.g. `int_external__obs_conformed`
   freshness via `dbt source freshness` (focus on `iceberg_external` sources).

Write a short situation note in #data-platform with:
`[time] provider(s), first alert, affected dashboards, next update time`.

## 3. Stabilise (First 15 Minutes)

1. **Throttle noisy consumers** (API layer)
   ```bash
   kubectl patch configmap/aurum-api-config -n aurum-dev \
     --type merge -p '{"data":{"AURUM_API_RATE_LIMIT_RPS":"50"}}'
   ```
2. **Pause failing DAGs** if provider returns persistent 5xx/timeout:
   ```bash
   airflow dags pause external_data_ingest
   airflow dags pause external_backfill
   ```
   Capture the run IDs for later resume (`airflow dags runs -d external_data_ingest`).
3. **Switch on cached responses** in the API so downstream apps keep serving:
   ```bash
   curl -X POST $AURUM_API/runtime-config/v1/features/external_read_cache \
     -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' \
     -d '{"enabled": true, "configuration": {"ttl_seconds": 900}}'
   ```
4. **Enable fallback data** where defined (NOAA / ISONE have local mirrors):
   ```bash
   airflow dags trigger external_data_fallback --conf '{"provider": "ISO-NE"}'
   ```

## 4. Investigate & Mitigate (15–60 Minutes)

| Checklist | Commands / Notes |
| --- | --- |
| Verify provider behaviour | `python scripts/observability/integrate_openlineage.py --probe iso-ne` (lightweight smoke) |
| Inspect latest logs | `kubectl logs -l app=external-collector --since=30m` |
| Validate credentials | Ensure secrets in `kubernetes secrets external-provider-*` are not rotated/expired |
| Apply workarounds | Reduce batch size via `runtime_config` (`external_ingest.batch_size=500`) |

If only a subset of endpoints is broken, update the allowlist in
`config/external_providers.yml` to temporarily disable affected feeds and reload
Airflow vars: `airflow variables set external_provider_config @config/external_providers.yml`.

## 5. Recovery

1. **Confirm upstream recovery** – two consecutive successful health probes or
   manual test request returning 200.
2. **Resume paused workloads**:
   ```bash
   airflow dags unpause external_data_ingest
   airflow dags unpause external_backfill
   ```
3. **Backfill missed window** – use the orchestrator:
   ```bash
   python scripts/ops/series_curve_mapping_admin.py backfill --start 2024-09-23 --end 2024-09-23
   airflow dags trigger external_data_backfill --conf '{"start_date":"2024-09-23"}'
   ```
4. **Verify data quality** – `dbt test --select mart_external_series_catalog+` and
   run the GE suite:
   ```bash
   great_expectations --v3-api checkpoint run external_dbt_outputs
   ```
5. **Re-enable caching overrides** (if disabled):
   ```bash
   curl -X POST $AURUM_API/runtime-config/v1/features/external_read_cache \
     -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' \
     -d '{"enabled": false}'
   ```

## 6. Communications

- **Initial** (within 15 min): incident start, providers, impact, next update.
- **Hourly**: progress, mitigation applied, ETA if known.
- **Resolution**: cause, customer impact, data backfill status, follow-up actions.

Templates live in `docs/runbooks/templates/incident-updates.md`.

## 7. Post-Incident Checklist

| Task | Owner | Notes |
| --- | --- | --- |
| Root-cause analysis & post-mortem (within 1 business day) | Incident Lead | Use template in `docs/postmortems/external-outage.md` |
| Monitoring review | Platform | Adjust thresholds or add probes if detection lag > 5 min |
| Data quality audit | Data Engineering | Confirm backfill completeness across `mart_external_series_catalog`, `mart_series_curve_mapping` |
| Runbook update | Incident Lead | Capture lessons learned |

## Quick Reference

- **Dashboards**: Grafana folder *External Data*, chart *Pipeline freshness*.
- **Logs**: `kubectl logs -n aurum-dev deployment/external-collector`.
- **Contacts**: External escalation doc `docs/runbooks/external-provider-contacts.md`.
- **Tooling**: `make perf-k6` (confirm API headroom post-outage), runtime-config
  admin API for temporary overrides, `dbt run/test` for marts.

Keep this runbook in sync with the Airflow DAG ownership map and provider
contracts. Update contact info and automation references at least quarterly.

## API Improvements & Security Enhancements

### Enhanced Error Handling & Resilience

#### Circuit Breaker Implementation
```yaml
# Implement circuit breaker for external API calls
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-circuit-breaker
  namespace: aurum-dev
data:
  CIRCUIT_BREAKER_ENABLED: "true"
  CIRCUIT_BREAKER_FAILURE_THRESHOLD: "5"
  CIRCUIT_BREAKER_RECOVERY_TIMEOUT: "300"
  CIRCUIT_BREAKER_EXPECTED_EXCEPTION: "requests.exceptions.RequestException"
  RETRY_ENABLED: "true"
  RETRY_ATTEMPTS: "3"
  RETRY_BACKOFF_FACTOR: "2"
  TIMEOUT_SECONDS: "30"
```

#### Rate Limiting Enhancements
```yaml
# Advanced rate limiting configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-rate-limits
  namespace: aurum-dev
data:
  RATE_LIMIT_REQUESTS: "100"
  RATE_LIMIT_WINDOW: "60"
  RATE_LIMIT_BURST: "20"
  RATE_LIMIT_BY_IP: "true"
  RATE_LIMIT_BY_USER: "true"
  RATE_LIMIT_EXEMPT_IPS: "10.0.0.0/8,192.168.0.0/16"
  RATE_LIMIT_EXEMPT_USERS: "admin,service-account"

  # Per-endpoint limits
  CURVES_ENDPOINT_LIMIT: "1000/hour"
  EXTERNAL_ENDPOINT_LIMIT: "500/hour"
  ANALYTICS_ENDPOINT_LIMIT: "100/hour"
```

#### Security Headers & CORS
```yaml
# Security enhancements
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-security
  namespace: aurum-dev
data:
  SECURITY_HEADERS_ENABLED: "true"
  CORS_ENABLED: "true"
  CORS_ORIGINS: "https://aurum.com,https://app.aurum.com"
  CORS_METHODS: "GET,POST,PUT,DELETE,OPTIONS"
  CORS_HEADERS: "Content-Type,Authorization,X-Requested-With"

  # Content Security Policy
  CSP_DEFAULT_SRC: "'self'"
  CSP_SCRIPT_SRC: "'self' 'unsafe-inline'"
  CSP_STYLE_SRC: "'self' 'unsafe-inline'"

  # HTTPS enforcement
  HTTPS_REDIRECT_ENABLED: "true"
  HSTS_ENABLED: "true"
  HSTS_MAX_AGE: "31536000"

  # API key security
  API_KEY_HEADER: "X-API-Key"
  API_KEY_CACHE_TTL: "300"
```

### Performance Optimizations

#### Caching Strategy
```yaml
# Enhanced caching configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-caching
  namespace: aurum-dev
data:
  REDIS_ENABLED: "true"
  REDIS_HOST: "redis-master.redis.svc.cluster.local"
  REDIS_PORT: "6379"
  REDIS_DB: "0"
  REDIS_PASSWORD: "your-redis-password"

  # Cache TTL settings
  CURVES_CACHE_TTL: "300"      # 5 minutes
  EXTERNAL_CACHE_TTL: "180"    # 3 minutes
  ANALYTICS_CACHE_TTL: "600"   # 10 minutes

  # Cache warming
  CACHE_WARMING_ENABLED: "true"
  CACHE_WARMING_INTERVAL: "300"  # 5 minutes
  CACHE_WARMING_BATCH_SIZE: "100"
```

#### Database Connection Pooling
```yaml
# Database optimization
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-database
  namespace: aurum-dev
data:
  DB_POOL_MIN_SIZE: "5"
  DB_POOL_MAX_SIZE: "20"
  DB_POOL_IDLE_TIMEOUT: "300"
  DB_POOL_CONNECTION_TIMEOUT: "30"
  DB_POOL_RETRY_ATTEMPTS: "3"
  DB_POOL_RETRY_DELAY: "1"

  # Query optimization
  QUERY_TIMEOUT_SECONDS: "30"
  QUERY_POOL_PRE_PING: "true"
  QUERY_POOL_RECYCLE: "3600"
```

#### API Response Compression
```yaml
# Compression configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-compression
  namespace: aurum-dev
data:
  COMPRESSION_ENABLED: "true"
  COMPRESSION_ALGORITHM: "gzip"
  COMPRESSION_LEVEL: "6"
  COMPRESSION_MIN_SIZE: "1024"

  # Response optimization
  JSON_SERIALIZATION_OPTIMIZED: "true"
  PAGINATION_DEFAULT_LIMIT: "100"
  PAGINATION_MAX_LIMIT: "1000"
```

### Monitoring & Observability

#### Enhanced Metrics Collection
```yaml
# Comprehensive metrics configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-metrics
  namespace: aurum-dev
data:
  METRICS_ENABLED: "true"
  METRICS_NAMESPACE: "aurum_api"

  # Request metrics
  REQUEST_LATENCY_BUCKETS: "0.1,0.5,1,2,5,10,30,60,120"
  REQUEST_SIZE_BUCKETS: "100,1000,10000,100000,1000000"

  # Business metrics
  BUSINESS_METRICS_ENABLED: "true"
  DATA_QUALITY_METRICS_ENABLED: "true"
  EXTERNAL_API_METRICS_ENABLED: "true"

  # Custom metrics
  CURVE_REQUESTS_TOTAL: "counter"
  EXTERNAL_DATA_REQUESTS_TOTAL: "counter"
  API_ERROR_RATE: "histogram"
```

#### Distributed Tracing
```yaml
# Tracing configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-tracing
  namespace: aurum-dev
data:
  TRACING_ENABLED: "true"
  TRACING_PROVIDER: "jaeger"
  TRACING_SERVICE_NAME: "aurum-api"

  JAEGER_ENDPOINT: "http://jaeger-collector.jaeger.svc.cluster.local:14268/api/traces"
  JAEGER_SAMPLING_RATE: "0.1"

  # OpenTelemetry integration
  OTEL_ENABLED: "true"
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.observability.svc.cluster.local:4317"
```

### Deployment & Configuration Management

#### Health Checks
```yaml
# Enhanced health checks
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-health
  namespace: aurum-dev
data:
  HEALTH_CHECK_ENABLED: "true"

  # Liveness probe settings
  LIVENESS_INITIAL_DELAY: "30"
  LIVENESS_TIMEOUT: "10"
  LIVENESS_PERIOD: "30"
  LIVENESS_FAILURE_THRESHOLD: "3"

  # Readiness probe settings
  READINESS_INITIAL_DELAY: "5"
  READINESS_TIMEOUT: "5"
  READINESS_PERIOD: "10"
  READINESS_FAILURE_THRESHOLD: "3"

  # Startup probe settings
  STARTUP_INITIAL_DELAY: "10"
  STARTUP_TIMEOUT: "30"
  STARTUP_PERIOD: "10"
  STARTUP_FAILURE_THRESHOLD: "30"
```

#### Feature Flags
```yaml
# Feature flag configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: aurum-api-features
  namespace: aurum-dev
data:
  FEATURE_FLAGS_ENABLED: "true"

  # API features
  ENHANCED_ERROR_HANDLING: "true"
  ADVANCED_RATE_LIMITING: "true"
  COMPREHENSIVE_LOGGING: "true"
  DATA_QUALITY_MONITORING: "true"

  # Security features
  API_KEY_AUTHENTICATION: "true"
  JWT_AUTHENTICATION: "true"
  REQUEST_ENCRYPTION: "true"

  # Performance features
  RESPONSE_CACHING: "true"
  DATABASE_POOLING: "true"
  ASYNC_PROCESSING: "true"
```
