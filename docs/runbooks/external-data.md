# External Data Operations Runbook

This runbook provides step-by-step procedures for common external data operations, troubleshooting, and incident response.

## Table of Contents

1. [Freshness SLO Violations](#freshness-slo-violations)
2. [Ingestion Failures](#ingestion-failures)
3. [GE Validation Failures](#ge-validation-failures)
4. [Mapping Coverage Issues](#mapping-coverage-issues)
5. [Quality Degradation](#quality-degradation)
6. [Rate Limiting](#rate-limiting)
7. [Schema Drift](#schema-drift)
8. [Data Rollback](#data-rollback)
9. [Emergency Procedures](#emergency-procedures)

## Freshness SLO Violations

### Symptoms
- Alert: `AurumExternalDataFreshnessSLOViolation`
- External data freshness exceeds 24 hours

### Diagnosis

1. **Check Provider Status**:
   ```bash
   # Check last successful update
   kubectl logs -f deployment/external-ingestion | grep -i "FRED\|EIA\|BLS\|OECD"

   # Check provider API status
   curl -I https://fred.stlouisfed.org/api/series
   ```

2. **Review Metrics**:
   ```bash
   # Check freshness metrics
   kubectl port-forward svc/prometheus 9090
   curl "http://localhost:9090/api/v1/query?query=aurum_external_data_freshness_seconds"
   ```

### Resolution

1. **Immediate Fix**:
   ```bash
   # Trigger manual sync
   airflow dags trigger external_catalog_sync

   # For specific provider
   airflow tasks run external_catalog_sync sync_FRED_catalog manual
   ```

2. **Long-term Fix**:
   - Review rate limiting configuration
   - Check for API changes from provider
   - Update credentials if expired

## Ingestion Failures

### Symptoms
- Alert: `AurumExternalDataIngestionFailures`
- External data ingestion has failed

### Diagnosis

1. **Check Logs**:
   ```bash
   # Check ingestion logs
   kubectl logs -f deployment/external-ingestion

   # Check for specific errors
   kubectl logs deployment/external-ingestion | grep -i "error\|fail"
   ```

2. **Verify Configuration**:
   ```bash
   # Check Vault secrets
   vault kv get secret/external/fred
   vault kv get secret/external/eia
   ```

### Resolution

1. **Restart Ingestion**:
   ```bash
   kubectl rollout restart deployment/external-ingestion
   ```

2. **Fix Common Issues**:

   **API Key Issues**:
   ```bash
   # Update API keys in Vault
   vault kv put secret/external/fred api_key=new_api_key
   ```

   **Rate Limiting**:
   ```bash
   # Update rate limiting configuration
   vi config/external_providers.json
   # Reduce request frequency
   ```

   **Network Issues**:
   ```bash
   # Check network connectivity
   kubectl exec -it deployment/external-ingestion -- curl -I https://fred.stlouisfed.org
   ```

## GE Validation Failures

### Symptoms
- Alert: `AurumExternalDataGEValidationFailures`
- External data failed Great Expectations validation

### Diagnosis

1. **Check Validation Results**:
   ```bash
   # Run validation manually
   python -c "
   from aurum.external.ge_validation import run_ge_validation
   import asyncio
   asyncio.run(run_ge_validation('external.timeseries_observation', 'external_timeseries_obs'))
   "
   ```

2. **Review Data Quality**:
   ```sql
   -- Check for common issues
   SELECT
     provider,
     COUNT(*) as total_records,
     COUNT(*) FILTER (WHERE value IS NULL) as null_values,
     COUNT(*) FILTER (WHERE value < 0) as negative_values,
     COUNT(*) FILTER (WHERE ts < '2020-01-01') as old_records
   FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= CURRENT_DATE - INTERVAL '1 day'
   GROUP BY provider;
   ```

### Resolution

1. **Update Expectations** (if data is valid):
   ```bash
   # Edit expectation suite
   vi ge/expectations/external_timeseries_obs.json

   # Update validation ranges
   # Update mostly percentages
   ```

2. **Fix Data Issues** (if data is invalid):
   ```sql
   -- Quarantine bad records
   INSERT INTO iceberg.external.timeseries_observation_quarantine
   SELECT * FROM iceberg.external.timeseries_observation
   WHERE value < 0;  -- Example: negative values

   -- Delete bad records
   DELETE FROM iceberg.external.timeseries_observation
   WHERE value < 0;
   ```

## Mapping Coverage Issues

### Symptoms
- Alert: `AurumExternalDataMappingCoverageLow`
- Less than 80% of external series are mapped to curves

### Diagnosis

1. **Check Mapping Statistics**:
   ```sql
   -- Check mapping coverage
   SELECT
     provider,
     COUNT(DISTINCT series_id) as total_series,
     COUNT(DISTINCT scm.series_id) as mapped_series,
     ROUND(
       COUNT(DISTINCT scm.series_id)::DECIMAL / COUNT(DISTINCT series_id),
       3
     ) as mapping_coverage
   FROM iceberg.external.series_catalog esc
   LEFT JOIN iceberg.market.series_curve_map scm
     ON esc.provider = scm.external_provider
     AND esc.series_id = scm.external_series_id
     AND scm.is_active = TRUE
   GROUP BY provider;
   ```

2. **Identify Unmapped Series**:
   ```sql
   -- Find unmapped series
   SELECT
     provider,
     series_id,
     title
   FROM iceberg.external.series_catalog
   WHERE (provider, series_id) NOT IN (
     SELECT external_provider, external_series_id
     FROM iceberg.market.series_curve_map
     WHERE is_active = TRUE
   )
   ORDER BY provider, series_id
   LIMIT 10;
   ```

### Resolution

1. **Add Missing Mappings**:
   ```bash
   # Use admin script to add mappings
   python scripts/ops/series_curve_mapping_admin.py \
     add FRED DGS10 US_TREASURY_10YR_1.0 \
     --confidence 0.95 \
     --method manual \
     --notes "10-year Treasury rate mapping"
   ```

2. **Bulk Import Mappings**:
   ```bash
   # Create CSV with mappings
   cat > mappings.csv << 'EOF'
   external_provider,external_series_id,curve_key,mapping_confidence,mapping_method,mapping_notes
   FRED,DGS10,US_TREASURY_10YR_1.0,0.95,manual,10-year Treasury rate
   FRED,FEDFUNDS,FEDERAL_FUNDS_RATE_1.0,0.95,manual,Federal funds rate
   EOF

   # Import mappings
   python scripts/ops/series_curve_mapping_admin.py \
     import mappings.csv \
     --created-by admin@aurum.local
   ```

## Quality Degradation

### Symptoms
- Alert: `AurumExternalDataQualityDegraded`
- Overall external data quality score below 0.9

### Diagnosis

1. **Check Quality Metrics**:
   ```bash
   # Check quality scores
   kubectl port-forward svc/prometheus 9090
   curl "http://localhost:9090/api/v1/query?query=aurum_external_data_quality_score"
   ```

2. **Review Quality Breakdown**:
   ```sql
   -- Analyze quality by provider and metric
   SELECT
     provider,
     AVG(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) as completeness,
     AVG(CASE WHEN value > 0 THEN 1 ELSE 0 END) as positivity,
     AVG(CASE WHEN ts > CURRENT_DATE - INTERVAL '1 year' THEN 1 ELSE 0 END) as recency,
     COUNT(*) as record_count
   FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= CURRENT_DATE - INTERVAL '1 day'
   GROUP BY provider;
   ```

### Resolution

1. **Improve Data Sources**:
   - Review provider data quality
   - Switch to alternative data sources if available
   - Implement data cleansing rules

2. **Update Validation Rules**:
   ```bash
   # Adjust validation thresholds
   vi ge/expectations/external_timeseries_obs.json
   # Update mostly percentages based on data characteristics
   ```

## Rate Limiting

### Symptoms
- HTTP 429 errors in logs
- Slow ingestion performance
- Alerts about provider API limits

### Diagnosis

1. **Check Rate Limit Status**:
   ```bash
   # Monitor current usage
   kubectl logs deployment/external-ingestion | grep -i "rate\|limit"
   ```

2. **Review Provider Limits**:
   ```bash
   # Check configured limits
   cat config/external_providers.json | jq '.providers[].rate_limits'
   ```

### Resolution

1. **Reduce Request Frequency**:
   ```json
   {
     "providers": [
       {
         "name": "FRED",
         "rate_limits": {
           "requests_per_minute": 5,
           "requests_per_hour": 100
         }
       }
     ]
   }
   ```

2. **Implement Backoff Strategy**:
   ```bash
   # Update ingestion configuration
   vi config/external_incremental_config.json
   # Add exponential backoff settings
   ```

3. **Request Limit Increase**:
   - Contact provider support
   - Request higher rate limits
   - Update configuration with new limits

## Schema Drift

### Symptoms
- Avro deserialization errors
- Missing fields in data
- Type mismatches

### Diagnosis

1. **Check Schema Compatibility**:
   ```bash
   # Compare schemas
   python scripts/schema_registry/compare_schemas.py ExtTimeseriesObsV1
   ```

2. **Review Recent Changes**:
   ```bash
   # Check recent schema updates
   git log --oneline kafka/schemas/ExtTimeseriesObsV1.avsc
   ```

### Resolution

1. **Update Schema**:
   ```bash
   # Update Avro schema
   cp new-schema.avsc kafka/schemas/ExtTimeseriesObsV1.avsc

   # Register with Schema Registry
   python scripts/schema_registry/register_schema.py ExtTimeseriesObsV1
   ```

2. **Deploy Schema Changes**:
   ```bash
   # Update ingestion jobs
   kubectl apply -f k8s/external/ingestion-jobs.yaml

   # Restart consumers
   kubectl rollout restart deployment/external-consumer
   ```

## Data Rollback

### When to Rollback

- Data corruption detected
- Schema changes causing issues
- Provider API changes affecting data quality
- Security incidents requiring data isolation

### Rollback Procedure

1. **Identify Affected Data**:
   ```sql
   -- Find records to rollback
   SELECT COUNT(*) FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

2. **Quarantine Data**:
   ```sql
   -- Move to quarantine table
   INSERT INTO iceberg.external.timeseries_observation_quarantine
   SELECT * FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

3. **Remove Bad Data**:
   ```sql
   -- Delete from main table
   DELETE FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

4. **Verify Rollback**:
   ```sql
   -- Confirm deletion
   SELECT COUNT(*) FROM iceberg.external.timeseries_observation
   WHERE provider = 'FRED';
   ```

## Emergency Procedures

### Stop All External Data Processing

```bash
# Pause all DAGs
airflow dags pause external_catalog_sync
airflow dags pause external_incremental
airflow dags pause external_backfill

# Stop ingestion jobs
kubectl scale deployment external-ingestion --replicas=0

# Stop consumers
kubectl scale deployment external-consumer --replicas=0
```

### Resume Processing

```bash
# Start consumers
kubectl scale deployment external-consumer --replicas=3

# Start ingestion
kubectl scale deployment external-ingestion --replicas=3

# Resume DAGs
airflow dags unpause external_catalog_sync
airflow dags unpause external_incremental
airflow dags unpause external_backfill

# Trigger catch-up
airflow dags trigger external_incremental
```

### Contact Information

- **Data Engineering**: data-eng@aurum.local
- **Platform Operations**: platform-ops@aurum.local
- **Security Team**: security@aurum.local
- **External Providers**:
  - FRED Support: fred-help@stlouisfed.org
  - EIA Support: eia-support@eia.gov
  - BLS Support: bls-data@bls.gov
  - OECD Support: oecd-stat@oecd.org

### Escalation Path

1. **Level 1**: Data Engineering on-call
2. **Level 2**: Platform Operations
3. **Level 3**: Engineering Manager
4. **Level 4**: VP of Engineering

For security incidents, immediately contact security@aurum.local and follow the security incident response plan.
