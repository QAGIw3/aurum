# External Data Integration

This document covers the integration of external data providers (FRED, EIA, BLS, OECD) into the Aurum platform.

## Overview

The external data integration pipeline consists of:
- **Data Sources**: External APIs and data feeds
- **Ingestion**: Kafka-based streaming ingestion
- **Processing**: dbt-based transformation and validation
- **Storage**: Iceberg tables with lineage tracking
- **Governance**: Great Expectations validation and alerting

## Architecture

```
External APIs → Kafka Topics → Iceberg Raw → dbt Staging → dbt Conformance → dbt Curated → Market Integration
```

## Data Flow

### 1. Catalog Sync
- **DAG**: `external_catalog_sync`
- **Schedule**: Daily at 6 AM UTC
- **Purpose**: Sync metadata from external providers
- **Output**: `iceberg.external.series_catalog`

### 2. Incremental Updates
- **DAG**: `external_incremental`
- **Schedule**: Every 4 hours
- **Purpose**: Fetch latest data from providers
- **Output**: `iceberg.external.timeseries_observation`

### 3. Backfill Operations
- **DAG**: `external_backfill`
- **Schedule**: Manual trigger only
- **Purpose**: Historical data ingestion
- **Output**: Historical `timeseries_observation` records

## Provider Configuration

### Supported Providers

| Provider | Data Types | Rate Limits | Authentication |
|----------|------------|-------------|----------------|
| FRED | Economic indicators | 1,000 req/day | API key |
| EIA | Energy data | 10,000 req/day | API key |
| BLS | Labor statistics | 500 req/day | None |
| OECD | Economic data | 20,000 req/day | API key |

### Configuration Files

- `/config/external_providers.json`: Provider settings
- `/config/external_catalog_config.json`: Catalog sync config
- `/config/external_incremental_config.json`: Incremental config
- `/config/external_backfill_config.json`: Backfill config

## Data Quality

### Validation Rules

1. **Schema Validation**: All records must conform to Avro schema
2. **Range Checks**: Values must be within plausible ranges
3. **Freshness**: Data must be current within provider SLAs
4. **Completeness**: Required fields must be present
5. **Uniqueness**: No duplicate records allowed

### Great Expectations Suites

- `external_series_catalog`: Catalog validation
- `external_timeseries_obs`: Observation validation
- `external_obs_conformed`: Conformed data validation
- `external_obs_curated`: Curated data validation

## Operations

### Monitoring

#### Key Metrics

- **Freshness**: Time since last successful update per provider
- **Coverage**: Percentage of series with curve mappings
- **Quality Score**: Overall data quality metric (0-1.0)
- **Ingestion Rate**: Records processed per minute
- **Validation Rate**: Percentage of records passing GE validation

#### Alerts

- **SLO Violations**: Freshness > 24 hours
- **Quality Degradation**: Quality score < 0.9
- **Ingestion Failures**: Any failed ingestions
- **GE Failures**: Validation failures block deployment

### Troubleshooting

#### Common Issues

1. **Rate Limiting**
   - Symptoms: HTTP 429 errors
   - Solution: Check provider rate limits, implement exponential backoff
   - Runbook: See rate limiting section below

2. **Schema Drift**
   - Symptoms: Avro deserialization errors
   - Solution: Update Avro schemas, redeploy ingestion jobs
   - Runbook: See schema management section

3. **Data Quality Issues**
   - Symptoms: GE validation failures
   - Solution: Review validation rules, update expectations
   - Runbook: See data quality section

#### Rate Limiting

Provider rate limits:
- FRED: 1,000 requests per day
- EIA: 10,000 requests per day
- BLS: 500 requests per day
- OECD: 20,000 requests per day

**Actions**:
1. Check current usage: `kubectl logs -f deployment/external-ingestion`
2. Implement backoff: Update ingestion configuration
3. Request limit increase: Contact provider support

#### Schema Management

1. **Update Schema**:
   ```bash
   # Update Avro schema
   cp new-schema.avsc kafka/schemas/ExtTimeseriesObsV1.avsc

   # Register with Schema Registry
   python scripts/schema_registry/register_schema.py ExtTimeseriesObsV1
   ```

2. **Deploy Changes**:
   ```bash
   # Update ingestion jobs
   kubectl apply -f k8s/external/ingestion-jobs.yaml

   # Restart consumers
   kubectl rollout restart deployment/external-consumer
   ```

#### Data Quality

1. **Review Validation Results**:
   ```bash
   # Check GE validation results
   python -c "
   from aurum.external.ge_validation import get_validation_results
   results = get_validation_results('external_timeseries_obs')
   print(results)
   "
   ```

2. **Update Expectations**:
   ```yaml
   # Update ge/expectations/external_timeseries_obs.json
   # Adjust validation rules based on data characteristics
   ```

## Security

### Access Control

- **Service Account**: `external-data-service`
- **Vault Policy**: `external-data-policy`
- **RBAC**: Read-only access to required resources
- **Network**: Egress to external APIs only

### Audit Logging

All external data operations are audited:
- API calls to external providers
- Data access events
- Administrative actions (mapping changes)
- Security violations

**Log Location**: `/var/log/aurum/external_data_audit.log`

**Retention**: 7 years for compliance

### Compliance

- **Data Classification**: External data is classified as "Public"
- **Retention Policy**: 10 years for economic data, 5 years for others
- **Access Controls**: Least privilege principle applied
- **Encryption**: Data encrypted in transit and at rest

## Backfill Procedures

### Initial Setup

1. **Configure Providers**:
   ```bash
   # Update provider configuration
   vi config/external_providers.json
   ```

2. **Set Up Credentials**:
   ```bash
   # Store API keys in Vault
   vault kv put secret/external/fred api_key=your_api_key
   ```

3. **Trigger Backfill**:
   ```bash
   # Manual trigger of backfill DAG
   airflow dags trigger external_backfill
   ```

### Historical Data Ingestion

1. **Identify Date Range**:
   - FRED: 1950-present
   - EIA: 1970-present
   - BLS: 2000-present
   - OECD: 1960-present

2. **Configure Backfill**:
   ```json
   {
     "providers": [
       {
         "name": "FRED",
         "datasets": ["economic", "financial"],
         "start_date": "1950-01-01",
         "end_date": "2024-01-01"
       }
     ]
   }
   ```

3. **Monitor Progress**:
   ```bash
   # Check Airflow logs
   airflow tasks logs external_backfill backfill_FRED_economic

   # Monitor ingestion metrics
   kubectl port-forward svc/prometheus 9090
   curl "http://localhost:9090/api/v1/query?query=aurum_external_ingestion_rate"
   ```

## Rollback Procedures

### Data Rollback

1. **Identify Impacted Data**:
   ```sql
   SELECT COUNT(*) FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

2. **Quarantine Bad Data**:
   ```sql
   INSERT INTO iceberg.external.timeseries_observation_quarantine
   SELECT * FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

3. **Delete Bad Data**:
   ```sql
   DELETE FROM iceberg.external.timeseries_observation
   WHERE ingest_ts >= '2024-01-01 00:00:00'
   AND provider = 'FRED';
   ```

### Configuration Rollback

1. **Revert Configuration**:
   ```bash
   git revert <commit_hash>
   kubectl apply -f config/external_providers.json
   ```

2. **Redeploy Services**:
   ```bash
   kubectl rollout restart deployment/external-ingestion
   kubectl rollout restart deployment/external-consumer
   ```

## Replay Procedures

### Partial Replay

1. **Configure Replay**:
   ```json
   {
     "providers": [
       {
         "name": "FRED",
         "datasets": ["economic"],
         "start_date": "2024-01-01",
         "end_date": "2024-01-07"
       }
     ]
   }
   ```

2. **Run Replay**:
   ```bash
   # Trigger incremental DAG with replay parameters
   airflow dags trigger external_incremental -c '{"replay": true}'
   ```

### Full Replay

1. **Stop Incremental Updates**:
   ```bash
   airflow dags pause external_incremental
   ```

2. **Configure Full Replay**:
   ```json
   {
     "providers": [
       {
         "name": "FRED",
         "datasets": ["economic", "financial"],
         "start_date": "1950-01-01",
         "end_date": "2024-01-01",
         "full_replay": true
       }
     ]
   }
   ```

3. **Trigger Full Replay**:
   ```bash
   airflow dags trigger external_backfill
   ```

4. **Resume Incremental**:
   ```bash
   airflow dags unpause external_incremental
   ```

## Performance Tuning

### Ingestion Performance

- **Batch Size**: 1,000 records per batch
- **Concurrency**: 3 parallel workers per provider
- **Retry Policy**: Exponential backoff (1, 2, 4, 8, 16 minutes)
- **Timeout**: 30 seconds per API call

### Processing Performance

- **dbt Workers**: 4 concurrent workers
- **Memory**: 8GB per worker
- **CPU**: 2 cores per worker
- **Partitioning**: By provider and date

## Cost Optimization

### API Usage

- **FRED**: $0.001 per API call (capped at $100/month)
- **EIA**: $0.0001 per API call (capped at $500/month)
- **BLS**: Free
- **OECD**: $0.0005 per API call (capped at $1,000/month)

**Optimization Strategies**:
1. Cache responses for 24 hours
2. Batch API calls where possible
3. Use incremental sync for recent data
4. Compress request/response payloads

### Storage Costs

- **Iceberg Tables**: ~$0.05/GB/month for S3
- **Kafka Topics**: ~$0.10/GB/month for retention
- **dbt Processing**: ~$0.02/query for compute

**Optimization Strategies**:
1. Partition tables by date and provider
2. Use ZSTD compression for Iceberg
3. Implement data retention policies
4. Optimize dbt incremental models

## Support and Maintenance

### Regular Tasks

1. **Daily**: Monitor freshness and quality metrics
2. **Weekly**: Review mapping coverage and accuracy
3. **Monthly**: Update provider configurations and schemas
4. **Quarterly**: Performance tuning and cost optimization

### Contact Information

- **Team**: Data Engineering
- **Email**: data-eng@aurum.local
- **Slack**: #data-engineering
- **On-call**: data-eng-oncall@aurum.local

### Emergency Contacts

- **Primary**: John Doe (john.doe@aurum.local)
- **Secondary**: Jane Smith (jane.smith@aurum.local)
- **Manager**: Bob Johnson (bob.johnson@aurum.local)

## Appendices

### API Reference

- **FRED API**: https://fred.stlouisfed.org/docs/api/fred/
- **EIA API**: https://www.eia.gov/opendata/documentation.php
- **BLS API**: https://www.bls.gov/developers/api_signature_v2.htm
- **OECD API**: https://stats.oecd.org/SDMX-JSON/

### Schema Definitions

- **Series Catalog**: `kafka/schemas/ExtSeriesCatalogUpsertV1.avsc`
- **Timeseries Observations**: `kafka/schemas/ExtTimeseriesObsV1.avsc`

### Configuration Examples

See `/config/external_*_config.json` files for detailed examples.
