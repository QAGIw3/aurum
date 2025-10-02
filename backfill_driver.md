# Backfill Driver System

The Backfill Driver provides a comprehensive system for executing historical data ingestion operations with proper concurrency control, error handling, and monitoring.

## Overview

The backfill system consists of several components:

- **BackfillDriver**: Core orchestration engine
- **Airflow DAG**: Orchestration and scheduling
- **SeaTunnel Templates**: Data processing templates
- **Slice Management**: Integration with incremental resume system
- **Monitoring & Reporting**: Comprehensive observability

## Features

### Core Features

- **Date Range Processing**: Shard large date ranges into manageable daily chunks
- **Concurrency Control**: Configurable parallel job execution with rate limiting
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Progress Tracking**: Real-time progress monitoring and reporting
- **Resource Management**: Memory and CPU usage monitoring
- **Cost Optimization**: API quota management and cost tracking

### Integration Features

- **Airflow Integration**: DAG-based orchestration with proper dependencies
- **SeaTunnel Integration**: Optimized templates for historical data processing
- **Slice System**: Integration with incremental resume for failed chunks
- **Monitoring Integration**: Structured logging and metrics collection

## Quick Start

### 1. Basic Backfill Execution

```bash
# Simple backfill for EIA data
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 3 \
  --batch-size 1000

# Dry run first to validate configuration
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --dry-run
```

### 2. Airflow DAG Execution

```bash
# Trigger backfill DAG manually
airflow dags trigger aurum_backfill_orchestrator \
  -c '{"source_name": "eia", "start_date": "2024-01-01", "end_date": "2024-12-31"}'
```

### 3. SeaTunnel Template Rendering

```bash
# Render SeaTunnel config for backfill
python scripts/seatunnel/render_config.py \
  --template seatunnel/jobs/templates/eia_backfill_to_kafka.conf.tmpl \
  --vars '{"backfill_date": "2024-12-31", "source_name": "eia"}'
```

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                   Airflow DAG                           │
│              Backfill Orchestrator                      │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │
│  │Validation   │ │Slice        │ │Execution    │        │
│  │Task         │ │Creation     │ │Task         │        │
│  │             │ │Task         │ │             │        │
│  └─────────────┘ └─────────────┘ └─────────────┘        │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│              Backfill Driver & Workers                  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │
│  │Job          │ │Concurrency  │ │Progress     │        │
│  │Generation   │ │Control      │ │Tracking     │        │
│  │             │ │             │ │             │        │
│  └─────────────┘ └─────────────┘ └─────────────┘        │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                 SeaTunnel Jobs                          │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │
│  │EIA API      │ │Data         │ │Kafka        │        │
│  │Extraction   │ │Validation   │ │Sink         │        │
│  │             │ │             │ │             │        │
│  └─────────────┘ └─────────────┘ └─────────────┘        │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Orchestration**: Airflow DAG validates and coordinates the backfill
2. **Planning**: Slice creation divides work into manageable chunks
3. **Execution**: Backfill driver manages concurrent job execution
4. **Processing**: SeaTunnel jobs handle data extraction and transformation
5. **Completion**: Results are validated and reported

## Configuration

### Backfill Driver Configuration

```python
from scripts.ops.backfill_driver import BackfillConfig

config = BackfillConfig(
    source='eia',
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    concurrency=3,           # Max concurrent jobs
    batch_size=1000,         # Records per batch
    max_retries=3,           # Retry attempts
    rate_limit=50,           # API calls per second
    dry_run=False,           # Validation only
    priority='normal'        # Job priority
)
```

### SeaTunnel Template Variables

```bash
# Required environment variables
export EIA_API_KEY="your_eia_api_key"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"

# Backfill-specific variables
export BACKFILL_DATE="2024-12-31"
export BACKFILL_BATCH_SIZE="1000"
export BACKFILL_MAX_CONCURRENT="3"
```

### Airflow DAG Parameters

```json
{
  "source_name": "eia",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "operation_type": "backfill",
  "priority": "normal",
  "max_concurrency": 3,
  "batch_size": 1000,
  "dry_run": false
}
```

## Usage Examples

### Example 1: EIA Historical Data Backfill

```bash
# 1. Dry run to validate configuration
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 2 \
  --dry-run

# 2. Execute actual backfill
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 2 \
  --batch-size 500 \
  --max-retries 5 \
  --rate-limit 10
```

### Example 2: NOAA Weather Data Backfill

```bash
# NOAA requires more conservative settings due to API limits
python scripts/ops/backfill_driver.py \
  --source noaa \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --concurrency 1 \
  --batch-size 200 \
  --max-retries 3 \
  --rate-limit 2
```

### Example 3: High-Volume Backfill with Monitoring

```bash
# Monitor resource usage during large backfills
python scripts/ops/backfill_driver.py \
  --source fred \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 5 \
  --batch-size 2000 \
  --max-retries 2 \
  --progress-interval 60 \
  --job-id "FRED_BACKFILL_2024_FULL"
```

## Monitoring & Observability

### Progress Tracking

```bash
# Monitor backfill progress in real-time
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --progress-interval 30 \
  --job-id "MONITORED_BACKFILL"
```

### Metrics Collection

The backfill driver automatically collects:

- **Execution Metrics**: Jobs completed, failed, success rate
- **Performance Metrics**: Processing speed, throughput
- **Resource Metrics**: Memory usage, CPU utilization
- **Error Metrics**: Error types, retry patterns
- **Cost Metrics**: API calls, data transfer volume

### Reporting

```bash
# Generate detailed backfill report
python scripts/ops/generate_backfill_report.py \
  --job-id BACKFILL_20241231_EIA \
  --format json \
  --output backfill_report.json
```

## Error Handling & Recovery

### Retry Logic

- **Exponential Backoff**: 1s, 2s, 4s, 8s, 16s, 32s, 60s max
- **Rate Limit Handling**: Automatic detection and backoff
- **API Error Recovery**: Specific handling for 429, 500, 502 errors
- **Partial Failure Recovery**: Individual job retry without full restart

### Manual Recovery

```bash
# Resume failed backfill from last successful point
python scripts/ops/backfill_driver.py \
  --resume \
  --job-id BACKFILL_20241231_EIA

# Retry only failed jobs
python scripts/ops/backfill_driver.py \
  --retry-failed \
  --job-id BACKFILL_20241231_EIA \
  --max-retries 2
```

### Emergency Stop

```bash
# Emergency stop for critical issues
python scripts/ops/backfill_driver.py \
  --emergency-stop \
  --job-id BACKFILL_20241231_EIA
```

## Integration with Other Systems

### Slice System Integration

```python
from aurum.slices import SliceClient, SliceConfig, SliceManager, SliceType

# Create slices for backfill operation
async with SliceClient(config) as client:
    manager = SliceManager(client)

    slice_ids = await manager.create_slices_from_operation(
        source_name="eia",
        operation_type="backfill",
        operation_data={
            "date_range": "2024-01-01 to 2024-12-31",
            "total_jobs": 365
        },
        slice_configs=slice_configs
    )
```

### Cost Profiler Integration

```python
from aurum.cost_profiler import CostProfiler, ProfilerConfig

# Track backfill costs
profiler = CostProfiler(ProfilerConfig())
session_id = profiler.start_profiling(
    dataset_name="backfill_eia_2024",
    data_source="eia"
)

# Backfill execution
# ... execute backfill ...

profiler.end_profiling(session_id)
```

## Performance Optimization

### Concurrency Tuning

```bash
# Start with low concurrency and increase
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  --concurrency 1 \
  --batch-size 500

# Gradually increase concurrency
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  --concurrency 3 \
  --batch-size 1000
```

### Memory Optimization

```bash
# Use smaller batch sizes for memory-constrained environments
python scripts/ops/backfill_driver.py \
  --source eia \
  --batch-size 200 \
  --concurrency 2 \
  --memory-limit 2g
```

### API Rate Limit Optimization

```bash
# Respect API quotas with conservative settings
python scripts/ops/backfill_driver.py \
  --source eia \
  --rate-limit 8 \
  --concurrency 1 \
  --batch-size 500 \
  --retry-backoff 5
```

## Troubleshooting

### Common Issues

#### Rate Limiting
```bash
# Solution: Reduce concurrency and increase delays
python scripts/ops/backfill_driver.py \
  --source eia \
  --concurrency 1 \
  --rate-limit 5 \
  --retry-backoff 10
```

#### Memory Issues
```bash
# Solution: Reduce batch size and parallelism
python scripts/ops/backfill_driver.py \
  --source eia \
  --batch-size 200 \
  --parallelism 1 \
  --max-memory 1g
```

#### API Failures
```bash
# Solution: Increase retry attempts and add error details
python scripts/ops/backfill_driver.py \
  --source eia \
  --max-retries 5 \
  --debug \
  --error-details
```

### Debug Mode

```bash
# Enable debug logging for troubleshooting
python scripts/ops/backfill_driver.py \
  --debug \
  --log-level DEBUG \
  --progress-interval 10
```

### Health Checks

```bash
# Validate system health before starting large backfills
python scripts/ops/check_backfill_health.py \
  --source eia \
  --date-range 2024-01-01:2024-12-31
```

## Security Considerations

- **API Keys**: Store in environment variables or secure vault
- **Data Access**: Implement proper authentication and authorization
- **Audit Logging**: Log all backfill operations for compliance
- **Data Classification**: Handle sensitive data appropriately
- **Network Security**: Use HTTPS for all API calls

## Best Practices

### Pre-Backfill Checklist

1. **Validate Configuration**: Run dry-run first
2. **Check Resources**: Ensure adequate memory and CPU
3. **API Limits**: Verify quota availability
4. **Backup Strategy**: Plan for data recovery
5. **Monitoring**: Set up alerts and dashboards

### During Backfill

1. **Monitor Progress**: Track completion and errors
2. **Resource Usage**: Watch memory and CPU utilization
3. **Error Rates**: Monitor for unusual failure patterns
4. **Cost Tracking**: Monitor API usage and costs
5. **Performance**: Adjust concurrency as needed

### Post-Backfill

1. **Validate Results**: Check data completeness and quality
2. **Generate Reports**: Create execution summary
3. **Clean Up**: Remove temporary files and logs
4. **Document Lessons**: Record issues and improvements
5. **Plan Next Steps**: Schedule follow-up operations

## API Reference

### BackfillDriver Class

Main orchestration class for backfill operations.

**Methods:**
- `execute()`: Execute the backfill operation
- `_dry_run()`: Perform validation without execution
- `_execute_jobs()`: Execute jobs with concurrency control
- `_execute_single_job()`: Execute individual job
- `pause()`: Pause the backfill operation
- `resume()`: Resume paused operation
- `cancel()`: Cancel the operation

### BackfillConfig Class

Configuration for backfill operations.

**Parameters:**
- `source`: Data source name
- `start_date`: Start date for backfill
- `end_date`: End date for backfill
- `concurrency`: Max concurrent jobs
- `batch_size`: Records per batch
- `max_retries`: Maximum retry attempts
- `dry_run`: Validation only mode

## Support & Maintenance

### Regular Maintenance

- **Database Cleanup**: Remove old job records periodically
- **Log Rotation**: Manage log file sizes
- **Performance Review**: Analyze execution patterns
- **Quota Monitoring**: Track API usage trends
- **Error Analysis**: Review failure patterns

### Emergency Contacts

- **Data Engineering**: data-eng@company.com
- **Infrastructure**: infra@company.com
- **On-call Engineer**: Check PagerDuty rotation

### Additional Resources

- **Runbooks**: See `docs/runbooks/data_operations_runbook.md`
- **Monitoring**: Grafana dashboards for backfill operations
- **API Documentation**: Individual data source API docs
- **Architecture**: System architecture documentation

---

**Last Updated**: 2025-01-21
**Version**: 1.0.0
**Status**: Production Ready
