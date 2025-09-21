# Data Ingestion Cost Profiler

The Cost Profiler system provides comprehensive monitoring, analysis, and cost estimation for data ingestion pipelines. It tracks performance metrics, identifies bottlenecks, and estimates operational costs across different data sources.

## Features

- **Performance Monitoring**: Track throughput, latency, error rates, and resource usage
- **Cost Estimation**: Estimate API, compute, storage, and data transfer costs
- **Performance Analysis**: Identify issues and generate optimization recommendations
- **Multi-format Reporting**: Generate reports in JSON, CSV, and Markdown formats
- **Integration Support**: Easy integration with SeaTunnel jobs and Airflow DAGs

## Components

### Core Components

1. **CostProfiler**: Main profiling engine that manages profiling sessions
2. **MetricsCollector**: Collects performance metrics during job execution
3. **PerformanceAnalyzer**: Analyzes metrics and identifies performance issues
4. **CostEstimator**: Estimates operational costs based on usage patterns
5. **ProfileReporter**: Generates comprehensive reports in multiple formats

### Specialized Collectors

- **SeaTunnelMetricsCollector**: Metrics collection specifically for SeaTunnel jobs
- **Kafka metrics integration**: Track message throughput and data volumes

## Quick Start

### Basic Usage

```python
from aurum.cost_profiler import CostProfiler, ProfilerConfig, PerformanceAnalyzer

# Initialize profiler
config = ProfilerConfig(
    collection_interval_seconds=1.0,
    enable_memory_monitoring=True
)
profiler = CostProfiler(config)

# Start profiling
session_id = profiler.start_profiling("my_dataset", "eia")

# Record metrics (during job execution)
# ... job execution ...

# Analyze performance
analyzer = PerformanceAnalyzer()
analysis = analyzer.analyze_dataset_profile(profile)
```

### SeaTunnel Integration

```python
from aurum.cost_profiler import SeaTunnelMetricsCollector, CollectionConfig

# Initialize SeaTunnel collector
config = CollectionConfig(collection_interval_seconds=2.0)
collector = SeaTunnelMetricsCollector(config, job_config)

# Start collection
collector.start_collection("dataset_name", "data_source")

# During job execution
collector.record_api_call("https://api.example.com", 1.0, True)
collector.record_data_processing(1000, 1024000, 5.0)
collector.record_seatunnel_source_metrics("source_name", 1000, 5.0)

# Get final metrics
final_metrics = collector.stop_collection()
```

### Airflow DAG Integration

```python
from aurum.cost_profiler import CostProfiler

# In your Airflow DAG
profiler = CostProfiler(ProfilerConfig())

def extract_task(**context):
    session_id = profiler.start_profiling("dataset_name", "data_source")

    # Your extraction logic here
    # Record metrics during execution
    # ...

    profiler.end_profiling(session_id)
```

## Configuration

### ProfilerConfig

```python
from aurum.cost_profiler import ProfilerConfig

config = ProfilerConfig(
    collection_interval_seconds=1.0,    # How often to collect metrics
    enable_memory_monitoring=True,      # Track memory usage
    enable_cpu_monitoring=False,        # Track CPU usage (expensive)
    max_metric_history=1000,           # Max metrics to keep in memory
    cost_per_api_call_usd=0.001,       # Cost per API call
    cost_per_gb_processed_usd=0.02,    # Cost per GB processed
    cost_per_hour_compute_usd=0.05,    # Cost per hour of compute
    throughput_warning_threshold=100.0, # Rows per second threshold
    error_rate_warning_threshold=0.05,  # 5% error rate threshold
    report_interval_minutes=15          # Report generation interval
)
```

### CollectionConfig

```python
from aurum.cost_profiler import CollectionConfig

config = CollectionConfig(
    collection_interval_seconds=1.0,    # Collection frequency
    enable_system_metrics=True,         # Collect system metrics
    enable_api_metrics=True,           # Collect API metrics
    buffer_size=1000,                  # Metrics buffer size
    max_collection_time_seconds=300.0  # Max collection time
)
```

## Metrics Collection

### Available Metrics

- **Throughput**: Rows per second, bytes per second
- **Latency**: API response times, processing times
- **Error Rates**: API errors, retry rates
- **Resource Usage**: Memory usage, CPU usage
- **API Metrics**: Call counts, success rates
- **Data Quality**: Record counts, data validation scores

### Recording Metrics

```python
from aurum.cost_profiler import MetricsCollector

collector = MetricsCollector(CollectionConfig())

# API metrics
collector.record_api_call("https://api.example.com", 1.5, True)
collector.record_retry("https://api.example.com", 2)

# Data processing metrics
collector.record_data_processing(1000, 1024000, 10.0)

# Error metrics
collector.record_error("VALIDATION_ERROR", "Invalid data format")

# Kafka metrics
collector.record_kafka_metrics("topic.name", 3, 1000, 1024000)
```

## Performance Analysis

### Issue Detection

The system automatically detects common performance issues:

- **Low Throughput**: Below threshold rows/bytes per second
- **High Error Rates**: Above threshold API error rates
- **High Retry Rates**: Excessive retry attempts
- **Resource Issues**: High memory/CPU usage
- **API Performance**: Slow response times

### Optimization Recommendations

Based on analysis, the system generates recommendations:

- Increase batch sizes for better throughput
- Implement exponential backoff for retries
- Add connection pooling for API calls
- Optimize memory usage with streaming
- Review API rate limits and quotas

## Cost Estimation

### Cost Models

The system includes configurable cost models for:

- **API Costs**: Per-call pricing for different data sources
- **Compute Costs**: Hourly rates for processing
- **Storage Costs**: Monthly rates for data storage
- **Data Transfer Costs**: Network transfer pricing

### Cost Breakdown

```python
from aurum.cost_profiler import CostEstimator

estimator = CostEstimator()
cost_estimate = estimator.estimate_dataset_cost(profile, "monthly")

print(f"API Costs: ${cost_estimate.api_costs_usd".2f"}")
print(f"Compute Costs: ${cost_estimate.compute_costs_usd".2f"}")
print(f"Total Monthly Cost: ${cost_estimate.total_costs_usd".2f"}")
```

## Reporting

### Report Generation

```python
from aurum.cost_profiler import ProfileReporter, ReportConfig

config = ReportConfig(
    include_costs=True,
    include_performance_analysis=True,
    output_format="json",
    cost_time_period="monthly"
)

reporter = ProfileReporter(config)
report = reporter.generate_profile_report(profiles, analysis_results, cost_estimator)

# Save report
reporter.save_report(report, Path("performance_report.json"))
```

### Report Formats

- **JSON**: Comprehensive structured data
- **CSV**: Tabular data for spreadsheet analysis
- **Markdown**: Human-readable documentation

### CLI Usage

```bash
# Analyze profiles from directory
python scripts/cost_profiler/cost_profiler_cli.py analyze /path/to/profiles

# Generate cost report
python scripts/cost_profiler/cost_profiler_cli.py cost /path/to/profiles --period monthly

# Generate comprehensive report
python scripts/cost_profiler/cost_profiler_cli.py report /path/to/profiles --format markdown --output report.md
```

## Integration Examples

### SeaTunnel Job Integration

```python
def run_seatunnel_job_with_profiling(job_config, dataset_name, data_source):
    # Initialize profiler
    profiler = CostProfiler(ProfilerConfig())

    # Start profiling
    session_id = profiler.start_profiling(dataset_name, data_source)

    # Initialize metrics collector
    collector = SeaTunnelMetricsCollector(CollectionConfig(), job_config)
    collector.start_collection(dataset_name, data_source)

    try:
        # Execute SeaTunnel job
        # Record metrics during execution
        # ...

        collector.record_seatunnel_source_metrics("http_source", records, time)
        collector.record_kafka_metrics(topic, partitions, messages, bytes)

    finally:
        # Always stop collection
        final_metrics = collector.stop_collection()
        profiler.record_metrics(session_id, final_metrics)
        profiler.end_profiling(session_id)

    return profiler.get_dataset_profile(dataset_name)
```

### Airflow DAG Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from aurum.cost_profiler import CostProfiler, ProfilerConfig

profiler = CostProfiler(ProfilerConfig())

def profiled_task(dataset_name, data_source, **context):
    session_id = profiler.start_profiling(dataset_name, data_source)

    try:
        # Your task logic here
        # Record metrics as you go
        # ...
    finally:
        profiler.end_profiling(session_id)

dag = DAG('ingestion_dag', ...)

task = PythonOperator(
    task_id='extract_data',
    python_callable=profiled_task,
    op_kwargs={
        'dataset_name': 'my_dataset',
        'data_source': 'eia'
    },
    dag=dag
)
```

## Best Practices

### Profiling Strategy

1. **Start Early**: Begin profiling during development to establish baselines
2. **Monitor Key Metrics**: Focus on throughput, error rates, and resource usage
3. **Set Thresholds**: Configure appropriate warning thresholds for your use case
4. **Regular Analysis**: Run analysis regularly to identify trends
5. **Cost Optimization**: Use cost estimates to guide architectural decisions

### Performance Optimization

1. **Batch Processing**: Use larger batch sizes to improve throughput
2. **Connection Pooling**: Implement connection pooling for API calls
3. **Retry Logic**: Add exponential backoff for failed requests
4. **Resource Monitoring**: Monitor memory and CPU usage
5. **Error Handling**: Implement comprehensive error handling

### Cost Management

1. **API Efficiency**: Minimize API calls through bulk operations
2. **Data Transfer**: Optimize data transfer patterns
3. **Compute Resources**: Right-size compute resources
4. **Storage Costs**: Use appropriate storage tiers
5. **Regular Review**: Review cost reports regularly

## Troubleshooting

### Common Issues

1. **High Memory Usage**: Enable streaming processing, increase memory allocation
2. **Low Throughput**: Increase parallelism, optimize batch sizes
3. **High Error Rates**: Implement better error handling, check API limits
4. **Cost Overruns**: Review API usage, optimize data transfer

### Debugging

- Enable detailed logging: `ProfilerConfig(enable_detailed_logging=True)`
- Check metrics collection: Use `collector.get_metrics_summary()`
- Review thresholds: Adjust `throughput_warning_threshold` as needed
- Monitor system resources: Enable `enable_system_metrics`

## API Reference

See the docstrings in the source code for detailed API documentation:

- `aurum.cost_profiler.profiler`: Core profiler classes
- `aurum.cost_profiler.metrics_collector`: Metrics collection
- `aurum.cost_profiler.performance_analyzer`: Performance analysis
- `aurum.cost_profiler.cost_estimator`: Cost estimation
- `aurum.cost_profiler.reporting`: Report generation

## Contributing

When extending the cost profiler:

1. Add new metric types to `MetricType` enum
2. Implement collection logic in `MetricsCollector`
3. Add analysis rules in `PerformanceAnalyzer`
4. Update cost models in `CostEstimator`
5. Add report formats in `ProfileReporter`

## License

This component is part of the Aurum data ingestion platform.
