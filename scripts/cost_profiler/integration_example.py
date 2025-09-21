#!/usr/bin/env python3
"""Example integration of cost profiler with SeaTunnel jobs and Airflow DAGs."""

import time
import json
from pathlib import Path
from typing import Dict, Any, Optional

from aurum.cost_profiler import (
    CostProfiler,
    ProfilerConfig,
    MetricsCollector,
    CollectionConfig,
    PerformanceMetrics,
    SeaTunnelMetricsCollector
)
from aurum.logging import create_logger, LogLevel


def seatunnel_job_integration_example():
    """Example of integrating cost profiler into SeaTunnel job execution."""

    print("=== SeaTunnel Job Integration Example ===")

    # Initialize cost profiler
    profiler_config = ProfilerConfig(
        collection_interval_seconds=2.0,
        enable_memory_monitoring=True,
        enable_cpu_monitoring=False
    )

    profiler = CostProfiler(profiler_config)

    # Example SeaTunnel job configuration
    seatunnel_config = {
        "env": {
            "job.mode": "BATCH",
            "parallelism": 4
        },
        "source": [
            {
                "plugin_name": "Http",
                "result_table_name": "eia_data",
                "url": "https://api.eia.gov/v2/electricity/rto/region-data/data/",
                "params": {
                    "api_key": "${EIA_API_KEY}",
                    "frequency": "hourly",
                    "start": "${WINDOW_START}",
                    "end": "${WINDOW_END}"
                }
            }
        ],
        "sink": [
            {
                "plugin_name": "Kafka",
                "source_table_name": "eia_data",
                "topic": "aurum.eia.hourly",
                "bootstrap_servers": "localhost:9092"
            }
        ]
    }

    # Start profiling session
    session_id = profiler.start_profiling(
        dataset_name="eia_hourly_electricity",
        data_source="eia"
    )

    print(f"Started profiling session: {session_id}")

    # Initialize SeaTunnel metrics collector
    collection_config = CollectionConfig(
        collection_interval_seconds=1.0,
        enable_system_metrics=True
    )

    collector = SeaTunnelMetricsCollector(collection_config, seatunnel_config)

    # Start metrics collection
    collector.start_collection("eia_hourly_electricity", "eia")

    # Simulate SeaTunnel job execution
    print("Simulating SeaTunnel job execution...")

    # Simulate API calls
    api_endpoints = [
        "https://api.eia.gov/v2/electricity/rto/region-data/data/",
        "https://api.eia.gov/v2/electricity/rto/region-data/data/",
        "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    ]

    for i, endpoint in enumerate(api_endpoints):
        # Simulate API call with some latency
        latency = 0.5 + (i * 0.2)  # Increasing latency
        success = i < 2  # First two succeed, third fails

        time.sleep(latency)

        collector.record_api_call(endpoint, latency, success)

        if not success:
            collector.record_error("API_ERROR", f"Request timeout for {endpoint}")

        # Simulate data processing
        if success:
            records_processed = 1000 + (i * 500)  # Increasing batch sizes
            bytes_processed = records_processed * 1024  # 1KB per record
            processing_time = latency * 0.8

            collector.record_data_processing(records_processed, bytes_processed, processing_time)

            # Record SeaTunnel source metrics
            collector.record_seatunnel_source_metrics(
                source_name="eia_http_source",
                record_count=records_processed,
                processing_time=processing_time
            )

    # Record Kafka metrics
    collector.record_kafka_metrics(
        topic="aurum.eia.hourly",
        partition_count=3,
        message_count=2500,
        byte_count=2560000  # 2.5MB
    )

    # Stop metrics collection
    final_metrics = collector.stop_collection()

    # Record final metrics with profiler
    profiler.record_metrics(session_id, final_metrics)

    # End profiling session
    profile = profiler.end_profiling(session_id)

    print(f"Completed profiling session. Profile: {profile}")

    # Generate performance analysis
    analyzer = PerformanceAnalyzer()
    analysis_result = analyzer.analyze_dataset_profile(profile)

    print(f"Analysis complete. Issues found: {len(analysis_result.performance_issues)}")

    for issue in analysis_result.performance_issues:
        print(f"  - {issue.issue_type.value}: {issue.description}")

    return profile, analysis_result


def airflow_dag_integration_example():
    """Example of integrating cost profiler into Airflow DAG execution."""

    print("\n=== Airflow DAG Integration Example ===")

    # Initialize cost profiler for DAG
    profiler_config = ProfilerConfig(
        collection_interval_seconds=5.0,
        enable_memory_monitoring=True
    )

    profiler = CostProfiler(profiler_config)

    # Simulate Airflow DAG execution
    dag_id = "ingest_eia_data"
    task_id = "extract_eia_hourly"

    # Start profiling for the entire DAG
    dag_session_id = profiler.start_profiling(
        dataset_name="eia_dag_execution",
        data_source="eia",
        session_id=f"{dag_id}_{int(time.time())}"
    )

    print(f"Started DAG profiling session: {dag_session_id}")

    # Simulate multiple task executions within the DAG
    tasks = [
        {"task_id": "extract_eia_hourly", "data_source": "eia", "expected_records": 10000},
        {"task_id": "transform_eia_data", "data_source": "eia", "expected_records": 10000},
        {"task_id": "load_to_kafka", "data_source": "kafka", "expected_records": 10000}
    ]

    task_metrics = []

    for task in tasks:
        print(f"Executing task: {task['task_id']}")

        # Start task-level profiling
        task_session_id = profiler.start_profiling(
            dataset_name=task['task_id'],
            data_source=task['data_source'],
            session_id=f"{task['task_id']}_{int(time.time())}"
        )

        # Initialize metrics collection for task
        collection_config = CollectionConfig(
            collection_interval_seconds=2.0,
            enable_system_metrics=True
        )

        collector = MetricsCollector(collection_config)
        collector.start_collection(task['task_id'], task['data_source'])

        # Simulate task execution
        task_start_time = time.time()

        # Simulate processing with realistic metrics
        if task['task_id'] == "extract_eia_hourly":
            # API extraction task
            for i in range(10):
                api_latency = 0.3 + (i * 0.1)
                success = i < 9  # 90% success rate

                time.sleep(api_latency)

                collector.record_api_call(
                    f"https://api.eia.gov/v2/data/{i}",
                    api_latency,
                    success
                )

                if success:
                    records = 1000
                    collector.record_data_processing(records, records * 512, api_latency)
                else:
                    collector.record_error("API_RATE_LIMIT", "Rate limit exceeded")

        elif task['task_id'] == "transform_eia_data":
            # Transformation task
            records = 9000  # Some data loss from errors
            processing_time = 15.0
            time.sleep(processing_time)

            collector.record_data_processing(records, records * 1024, processing_time)

        elif task['task_id'] == "load_to_kafka":
            # Kafka loading task
            records = 9000
            processing_time = 8.0
            time.sleep(processing_time)

            collector.record_data_processing(records, records * 1024, processing_time)
            collector.record_kafka_metrics(
                topic="aurum.eia.hourly",
                partition_count=3,
                message_count=records,
                byte_count=records * 1024
            )

        # Stop task collection and record metrics
        task_metrics_obj = collector.stop_collection()
        profiler.record_metrics(task_session_id, task_metrics_obj)
        profiler.end_profiling(task_session_id)

        task_metrics.append(task_metrics_obj)

        execution_time = time.time() - task_start_time
        print(f"  Completed {task['task_id']} in {execution_time".2f"}s")

    # End DAG profiling
    dag_profile = profiler.end_profiling(dag_session_id)

    print(f"Completed DAG execution. Total tasks: {len(tasks)}")

    # Analyze overall performance
    analyzer = PerformanceAnalyzer()
    analysis_result = analyzer.analyze_dataset_profile(dag_profile)

    print(f"DAG analysis complete. Issues found: {len(analysis_result.performance_issues)}")

    return dag_profile, analysis_result, task_metrics


def generate_integration_report(profiles, analyses):
    """Generate a comprehensive integration report."""

    print("\n=== Integration Report ===")

    # Cost estimation
    from aurum.cost_profiler import CostEstimator
    cost_estimator = CostEstimator()

    # Generate cost report
    cost_report = cost_estimator.generate_cost_report({}, "monthly")

    print(f"Estimated monthly cost: ${cost_report.get('summary', {}).get('total_costs_usd', 0)".2f"}")

    # Performance summary
    total_throughput = 0
    total_errors = 0
    total_api_calls = 0

    for profile in profiles:
        if profile and profile.metrics:
            avg_metrics = profile.get_average_metrics()
            if avg_metrics:
                throughput = avg_metrics.calculate_throughput()
                total_throughput += throughput.get("rows_per_second", 0)
                total_errors += avg_metrics.error_count
                total_api_calls += avg_metrics.api_call_count

    print(f"Average throughput: {total_throughput".1f"} records/second")
    print(f"Total API calls: {total_api_calls}")
    print(f"Total errors: {total_errors}")
    print(f"Error rate: {(total_errors / max(total_api_calls, 1)) * 100".1f"}%")

    # Issues summary
    total_issues = sum(
        len(analysis.performance_issues)
        for analysis in analyses
        if analysis
    )

    print(f"Total performance issues: {total_issues}")

    # Recommendations
    print("\n=== Recommendations ===")

    if total_errors > 0:
        print("- Implement exponential backoff for API retries")
        print("- Add circuit breaker pattern for failing endpoints")
        print("- Increase batch sizes to reduce API call frequency")

    if total_throughput < 100:  # Low throughput threshold
        print("- Consider parallel processing for data extraction")
        print("- Optimize data transformation pipeline")
        print("- Review API rate limits and quotas")


def main():
    """Main function demonstrating cost profiler integration."""

    print("Data Ingestion Cost Profiler - Integration Examples")
    print("=" * 50)

    # Run SeaTunnel integration example
    seatunnel_profile, seatunnel_analysis = seatunnel_job_integration_example()

    # Run Airflow DAG integration example
    airflow_profile, airflow_analysis, task_metrics = airflow_dag_integration_example()

    # Generate comprehensive report
    generate_integration_report(
        [seatunnel_profile, airflow_profile],
        [seatunnel_analysis, airflow_analysis]
    )

    # Save profiles for later analysis
    save_sample_profiles([seatunnel_profile, airflow_profile])

    print("\nIntegration examples completed successfully!")
    print("Check the 'sample_profiles' directory for saved profile data.")


def save_sample_profiles(profiles):
    """Save sample profiles to files for demonstration."""

    output_dir = Path("sample_profiles")
    output_dir.mkdir(exist_ok=True)

    for i, profile in enumerate(profiles):
        if profile and hasattr(profile, 'get_performance_summary'):
            summary = profile.get_performance_summary()

            output_file = output_dir / f"sample_profile_{i+1}.json"

            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)

            print(f"Saved sample profile to {output_file}")


if __name__ == "__main__":
    main()
