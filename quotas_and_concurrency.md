# Data Source Quotas and Concurrency Management

This document provides comprehensive guidance on managing quotas, rate limits, and concurrency settings for the Aurum data ingestion platform across all supported data sources.

## Overview

Effective quota and concurrency management is critical for:
- Maintaining API compliance and avoiding rate limiting
- Optimizing throughput while respecting data source constraints
- Controlling operational costs
- Ensuring reliable data ingestion at scale

## Data Sources Overview

### EIA (Energy Information Administration)

**API Limits:**
- 10 requests per second
- 600 requests per minute
- 36,000 requests per hour
- 500,000 requests per day

**Optimal Settings:**
- Batch size: 5,000 records
- Max concurrent jobs: 3
- Retry backoff: 2 seconds
- Rate limit sleep: 100ms

**Cost Model:**
- $0.001 per API call
- Estimated monthly cost: $50 (50K calls)

### FRED (Federal Reserve Economic Data)

**API Limits:**
- 100 requests per second
- 6,000 requests per minute
- 360,000 requests per hour
- 2,500,000 requests per day

**Optimal Settings:**
- Batch size: 10,000 records
- Max concurrent jobs: 8
- Retry backoff: 1 second
- Rate limit sleep: 10ms

**Cost Model:**
- $0.0005 per API call
- Estimated monthly cost: $25 (50K calls)

### CPI (Consumer Price Index via FRED)

**API Limits:**
- Same as FRED (served through FRED API)

**Optimal Settings:**
- Batch size: 10,000 records
- Max concurrent jobs: 6
- Retry backoff: 1 second
- Rate limit sleep: 10ms

**Cost Model:**
- $0.0005 per API call
- Estimated monthly cost: $15 (30K calls)

### NOAA (National Oceanic and Atmospheric Administration)

**API Limits:**
- 5 requests per second
- 300 requests per minute
- 18,000 requests per hour
- 100,000 requests per day

**Optimal Settings:**
- Batch size: 1,000 records
- Max concurrent jobs: 2
- Retry backoff: 5 seconds
- Rate limit sleep: 200ms

**Cost Model:**
- $0.0001 per API call
- Estimated monthly cost: $10 (100K calls)

### ISO (Independent System Operators)

**API Limits:**
- 2 requests per second
- 120 requests per minute
- 7,200 requests per hour
- 50,000 requests per day

**Optimal Settings:**
- Batch size: 500 records
- Max concurrent jobs: 1
- Retry backoff: 10 seconds
- Rate limit sleep: 500ms

**Cost Model:**
- $0.002 per API call
- Estimated monthly cost: $100 (50K calls)

## Environment Variables

### Data Source Specific Variables

```bash
# EIA Settings
AURUM_EIA_REQUESTS_PER_SECOND=10
AURUM_EIA_REQUESTS_PER_MINUTE=600
AURUM_EIA_BATCH_SIZE=5000
AURUM_EIA_MAX_CONCURRENT_JOBS=3
AURUM_EIA_RETRY_BACKOFF_SECONDS=2
AURUM_EIA_RATE_LIMIT_SLEEP_MS=100

# FRED Settings
AURUM_FRED_REQUESTS_PER_SECOND=100
AURUM_FRED_REQUESTS_PER_MINUTE=6000
AURUM_FRED_BATCH_SIZE=10000
AURUM_FRED_MAX_CONCURRENT_JOBS=8
AURUM_FRED_RETRY_BACKOFF_SECONDS=1
AURUM_FRED_RATE_LIMIT_SLEEP_MS=10

# CPI Settings
AURUM_CPI_REQUESTS_PER_SECOND=100
AURUM_CPI_REQUESTS_PER_MINUTE=6000
AURUM_CPI_BATCH_SIZE=10000
AURUM_CPI_MAX_CONCURRENT_JOBS=6
AURUM_CPI_RETRY_BACKOFF_SECONDS=1
AURUM_CPI_RATE_LIMIT_SLEEP_MS=10

# NOAA Settings
AURUM_NOAA_REQUESTS_PER_SECOND=5
AURUM_NOAA_REQUESTS_PER_MINUTE=300
AURUM_NOAA_BATCH_SIZE=1000
AURUM_NOAA_MAX_CONCURRENT_JOBS=2
AURUM_NOAA_RETRY_BACKOFF_SECONDS=5
AURUM_NOAA_RATE_LIMIT_SLEEP_MS=200

# ISO Settings
AURUM_ISO_REQUESTS_PER_SECOND=2
AURUM_ISO_REQUESTS_PER_MINUTE=120
AURUM_ISO_BATCH_SIZE=500
AURUM_ISO_MAX_CONCURRENT_JOBS=1
AURUM_ISO_RETRY_BACKOFF_SECONDS=10
AURUM_ISO_RATE_LIMIT_SLEEP_MS=500
```

### Global Settings

```bash
# Global Defaults
AURUM_GLOBAL_DEFAULT_BATCH_SIZE=1000
AURUM_GLOBAL_DEFAULT_MAX_CONCURRENT_JOBS=3
AURUM_GLOBAL_DEFAULT_RETRY_BACKOFF_SECONDS=2
AURUM_GLOBAL_DEFAULT_RATE_LIMIT_SLEEP_MS=100

# Monitoring Thresholds
AURUM_GLOBAL_THROUGHPUT_WARNING_THRESHOLD=100
AURUM_GLOBAL_ERROR_RATE_WARNING_THRESHOLD=0.05
AURUM_GLOBAL_MEMORY_USAGE_WARNING_MB=500
AURUM_GLOBAL_COST_PER_GB_THRESHOLD_USD=0.10
```

## Deployment Profiles

### Development Profile

**Use Case:** Local development and testing
**Multipliers:** 50% of production limits

```bash
# Generated settings for development
AURUM_EIA_REQUESTS_PER_SECOND=5
AURUM_EIA_BATCH_SIZE=2500
AURUM_EIA_MAX_CONCURRENT_JOBS=2
# ... other sources scaled similarly
```

### Staging Profile

**Use Case:** Pre-production testing
**Multipliers:** 80% of production limits

```bash
# Generated settings for staging
AURUM_EIA_REQUESTS_PER_SECOND=8
AURUM_EIA_BATCH_SIZE=4000
AURUM_EIA_MAX_CONCURRENT_JOBS=3
# ... other sources scaled similarly
```

### Production Profile

**Use Case:** Full production workloads
**Multipliers:** 100% of optimal limits

```bash
# Generated settings for production
AURUM_EIA_REQUESTS_PER_SECOND=10
AURUM_EIA_BATCH_SIZE=5000
AURUM_EIA_MAX_CONCURRENT_JOBS=3
# ... other sources at full limits
```

### High Volume Profile

**Use Case:** High-volume processing scenarios
**Multipliers:** 120-150% of standard limits

```bash
# Generated settings for high volume
AURUM_EIA_REQUESTS_PER_SECOND=12
AURUM_EIA_BATCH_SIZE=7500
AURUM_EIA_MAX_CONCURRENT_JOBS=5
# ... other sources scaled up
```

## Configuration Generation

### Using the Configuration Generator

```bash
# Generate environment file for all sources in production
python scripts/config/generate_environment_defaults.py env --all --profile production

# Generate SeaTunnel configs for specific sources
python scripts/config/generate_environment_defaults.py seatunnel --sources eia fred --profile staging

# Generate Airflow config for EIA only
python scripts/config/generate_environment_defaults.py airflow --sources eia --output airflow_config.json

# Generate all configs for development profile
python scripts/config/generate_environment_defaults.py all --profile development
```

### Configuration Files Structure

```
config/
├── data_source_quotas.json          # Master quotas configuration
├── generated/
│   ├── development/
│   │   ├── .env.development         # Environment variables
│   │   ├── seatunnel/               # SeaTunnel job configs
│   │   └── dag_config_development.json
│   ├── staging/
│   └── production/
└── seatunnel/
    └── templates/                   # SeaTunnel job templates
```

## SeaTunnel Configuration

### HTTP Source Configuration

```json
{
  "plugin_name": "Http",
  "result_table_name": "eia_data",
  "url": "${API_BASE_URL}",
  "http_request_config": {
    "connection_timeout": 30000,
    "socket_timeout": 120000,
    "retry": {
      "max_retries": 5,
      "retry_backoff_multiplier_ms": 2000,
      "retry_backoff_max_ms": 10000,
      "rate_limit_sleep_ms": 100
    }
  }
}
```

### Kafka Sink Configuration

```json
{
  "plugin_name": "Kafka",
  "source_table_name": "eia_data",
  "topic": "aurum.eia.data",
  "kafka_config": {
    "acks": "all",
    "retries": 10,
    "max.in.flight.requests.per.connection": 1,
    "compression.type": "snappy"
  }
}
```

## Airflow DAG Configuration

### Pool Configuration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

# Create pools for each data source
eia_pool = Pool(
    pool_name="eia_pool",
    slots=3,  # Max concurrent EIA jobs
    description="EIA data source pool"
)

fred_pool = Pool(
    pool_name="fred_pool",
    slots=8,  # Max concurrent FRED jobs
    description="FRED data source pool"
)
```

### Task Configuration

```python
task = PythonOperator(
    task_id="extract_eia_data",
    python_callable=extract_eia_data,
    pool="eia_pool",
    pool_slots=1,
    task_concurrency=4,
    execution_timeout=timedelta(hours=1),
    sla=timedelta(minutes=30),
    dag=dag
)
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **API Call Rates**
   - Requests per second
   - Requests per minute
   - Rate limit hit rate

2. **Error Rates**
   - HTTP error rate
   - Retry rate
   - Failed job rate

3. **Throughput**
   - Records processed per second
   - Data volume per second
   - Job completion time

4. **Resource Usage**
   - Memory usage per job
   - CPU usage patterns
   - Network I/O rates

### Alert Thresholds

```python
# Configure alerts in monitoring system
ALERTS = {
    "high_error_rate": {
        "threshold": 0.05,  # 5% error rate
        "window": "5min",
        "severity": "warning"
    },
    "low_throughput": {
        "threshold": 100,  # 100 records/second
        "window": "10min",
        "severity": "info"
    },
    "rate_limit_hits": {
        "threshold": 10,  # 10 hits per hour
        "window": "1hour",
        "severity": "warning"
    }
}
```

## Cost Management

### Cost Monitoring

1. **API Call Costs**
   - Track calls per data source
   - Monitor cost per record processed
   - Alert on cost anomalies

2. **Data Transfer Costs**
   - Monitor outbound data transfer
   - Track compression effectiveness
   - Optimize batch sizes for cost efficiency

3. **Compute Costs**
   - Monitor SeaTunnel processing time
   - Track Airflow task execution time
   - Optimize resource allocation

### Cost Optimization Strategies

1. **Batch Size Optimization**
   - Increase batch sizes to reduce API calls
   - Balance batch size vs memory usage
   - Monitor API response times

2. **Concurrency Tuning**
   - Adjust concurrent jobs based on rate limits
   - Monitor queue depths and wait times
   - Scale based on throughput requirements

3. **Retry Strategy Optimization**
   - Minimize unnecessary retries
   - Implement exponential backoff
   - Monitor retry success rates

## Troubleshooting

### Common Issues

#### High Error Rates

**Symptoms:**
- Error rates > 5%
- Frequent 429 (Too Many Requests) errors
- Failed job retries

**Solutions:**
1. Reduce batch sizes
2. Increase retry backoff time
3. Decrease concurrent jobs
4. Implement better error handling

#### Low Throughput

**Symptoms:**
- Processing < 100 records/second
- Long job completion times
- Underutilized resources

**Solutions:**
1. Increase batch sizes
2. Increase concurrent jobs
3. Optimize API call patterns
4. Review rate limiting configuration

#### Rate Limit Violations

**Symptoms:**
- HTTP 429 errors
- API throttling messages
- Inconsistent data ingestion

**Solutions:**
1. Check rate limit configuration
2. Verify API key limits
3. Implement proper backoff
4. Consider API upgrades

### Debugging Tools

#### Environment Validation

```bash
# Check current environment settings
python scripts/config/generate_environment_defaults.py env --sources eia --profile current

# Validate against quotas
python scripts/config/validate_quotas.py
```

#### Performance Profiling

```bash
# Profile current performance
python scripts/cost_profiler/cost_profiler_cli.py analyze /path/to/profiles

# Generate performance report
python scripts/cost_profiler/cost_profiler_cli.py report /path/to/profiles --format markdown
```

## Best Practices

### Development

1. **Start Conservative**
   - Use development profile initially
   - Monitor performance and adjust gradually
   - Test with small data sets first

2. **Monitor Early**
   - Set up monitoring from day one
   - Configure alerts for key metrics
   - Track performance baselines

3. **Test Thoroughly**
   - Test all data sources in staging
   - Validate rate limit handling
   - Verify error recovery

### Production

1. **Gradual Rollout**
   - Start with limited data sources
   - Monitor for 1-2 weeks before full rollout
   - Have rollback procedures ready

2. **Continuous Monitoring**
   - Monitor all key metrics continuously
   - Set up automated alerting
   - Regular performance reviews

3. **Capacity Planning**
   - Plan for growth in data volume
   - Monitor resource utilization
   - Regular cost reviews

### Cost Optimization

1. **Regular Reviews**
   - Monthly cost analysis
   - Quarterly optimization reviews
   - Annual contract renewals

2. **API Efficiency**
   - Optimize API call patterns
   - Use bulk endpoints where available
   - Implement caching where appropriate

3. **Resource Right-Sizing**
   - Monitor resource utilization
   - Adjust instance sizes based on usage
   - Use auto-scaling where appropriate

## API Registration and Keys

### EIA API Registration

1. Visit https://www.eia.gov/opendata/
2. Register for API access
3. Obtain API key
4. Configure in Vault: `eia/api_key`

### FRED API Registration

1. Visit https://fred.stlouisfed.org/docs/api/api_key.html
2. Register for API key
3. No cost for standard usage
4. Configure in Vault: `fred/api_key`

### NOAA API Access

1. Visit https://www.ncei.noaa.gov/access
2. Review data access policies
3. Some datasets require registration
4. Configure in Vault: `noaa/api_token`

### ISO API Access

1. Varies by ISO
2. Some require registration
3. May have different access tiers
4. Configure in Vault: `iso/{iso_name}/api_key`

## Support and Maintenance

### Regular Maintenance

1. **Quarterly Reviews**
   - Review all quotas and limits
   - Update configurations as needed
   - Document any changes

2. **API Updates**
   - Monitor for API changes
   - Update configurations accordingly
   - Test after updates

3. **Performance Optimization**
   - Regular performance testing
   - Identify optimization opportunities
   - Implement improvements

### Getting Help

1. **Documentation**: Check this document first
2. **Configuration Generator**: Use the CLI tool for updates
3. **Monitoring**: Review metrics and alerts
4. **Community**: Check for similar issues in documentation

## Conclusion

Effective quota and concurrency management is essential for reliable, cost-effective data ingestion. This document provides the foundation for configuring and managing these settings across all supported data sources.

Remember to:
- Start conservative and scale gradually
- Monitor continuously and set up alerts
- Regularly review and optimize configurations
- Document changes and rationale

For questions or issues, refer to the troubleshooting section or consult with the data engineering team.
