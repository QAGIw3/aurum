#!/usr/bin/env python3
"""Demo script for structured logging system."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Optional

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.logging import (
    StructuredLogger,
    LogLevel,
    create_logger,
    LoggingMetrics
)


def demo_basic_logging() -> None:
    """Demonstrate basic structured logging."""
    print("📝 Demo: Basic Structured Logging")
    print("=" * 50)

    # Create logger
    logger = StructuredLogger("demo_source", dataset="demo_dataset")

    # Log various types of events
    logger.log(LogLevel.INFO, "Starting demo application", "app_start")
    logger.log(LogLevel.DEBUG, "Configuration loaded", "config_load", hostname="demo-host")

    # Simulate some processing
    logger.start_operation("data_processing")
    time.sleep(0.1)  # Simulate work
    logger.end_operation("data_processing_id", success=True, records_processed=1000)

    # Log data quality
    logger.log_data_quality(
        records_processed=1000,
        records_valid=950,
        records_invalid=50,
        metadata={"validation_rules": "standard"}
    )

    # Simulate API call
    logger.log_api_call(
        endpoint="/api/v1/data",
        method="POST",
        status_code=200,
        response_time_ms=245.67
    )

    # Log completion
    logger.log(LogLevel.INFO, "Demo application completed", "app_complete")

    print("✅ Basic logging demo completed\n")


def demo_error_handling() -> None:
    """Demonstrate error handling and logging."""
    print("🚨 Demo: Error Handling and Logging")
    print("=" * 50)

    logger = StructuredLogger("demo_source")

    # Simulate various error scenarios
    errors = [
        (ValueError("Invalid data format"), "data_validation"),
        (ConnectionError("API endpoint unreachable"), "api_connection"),
        (TimeoutError("Request timed out"), "api_timeout")
    ]

    for error, context in errors:
        logger.log_error(error, context=context)

    print("✅ Error handling demo completed\n")


def demo_batch_processing() -> None:
    """Demonstrate batch processing with progress logging."""
    print("📊 Demo: Batch Processing with Progress")
    print("=" * 50)

    logger = StructuredLogger("batch_processor")

    total_batches = 5
    records_per_batch = 200

    for batch_id in range(1, total_batches + 1):
        logger.start_operation(f"batch_{batch_id}")

        # Simulate batch processing
        time.sleep(0.05)

        # Log progress
        logger.log_batch_progress(
            batch_id=batch_id,
            total_batches=total_batches,
            records_in_batch=records_per_batch
        )

        logger.end_operation(f"batch_{batch_id}", success=True)

    print("✅ Batch processing demo completed\n")


def demo_metrics_collection() -> None:
    """Demonstrate metrics collection."""
    print("📈 Demo: Metrics Collection")
    print("=" * 50)

    # Create logger with metrics tracking
    logger = StructuredLogger("metrics_demo")
    metrics = LoggingMetrics()

    # Simulate various activities
    activities = [
        ("INFO", "data_ingestion", "Data ingestion started"),
        ("DEBUG", "data_validation", "Validating records"),
        ("WARN", "data_quality", "Low quality score detected"),
        ("ERROR", "api_call", "API rate limit exceeded"),
        ("INFO", "data_ingestion", "Data ingestion completed")
    ]

    for level, event_type, message in activities:
        start_time = time.time()

        # Simulate activity
        time.sleep(0.01)

        processing_time = (time.time() - start_time) * 1000

        # Log event and record metrics
        logger.log(LogLevel(level), message, event_type, processing_time_ms=processing_time)
        metrics.record_log_event(level, "metrics_demo", event_type, processing_time)

    # Display metrics summary
    summary = metrics.get_summary()
    print("📊 Metrics Summary:")
    print(f"   Total Events: {summary['log_events_total']}")
    print(f"   Events by Level: {summary['log_events_by_level']}")
    print(f"   Events by Type: {summary['log_events_by_type']}")

    if summary['processing_time_percentiles']:
        for key, percentiles in summary['processing_time_percentiles'].items():
            print(f"   Processing Time ({key}): p50={percentiles['p50']".2f"}ms, mean={percentiles['mean']".2f"}ms")

    print("✅ Metrics collection demo completed\n")


def demo_json_output() -> None:
    """Demonstrate JSON output format."""
    print("🔧 Demo: JSON Output Format")
    print("=" * 50)

    from aurum.logging import LogEvent, LogLevel

    events = [
        LogEvent(
            level=LogLevel.INFO,
            message="Starting data ingestion",
            event_type="job_start",
            source_name="json_demo",
            job_id="job_123",
            metadata={"batch_size": 1000}
        ),
        LogEvent(
            level=LogLevel.ERROR,
            message="API rate limit exceeded",
            event_type="api_error",
            source_name="json_demo",
            error_code="RATE_LIMIT_EXCEEDED",
            error_message="Too many requests",
            metadata={"retry_after": 60, "requests_made": 150}
        ),
        LogEvent(
            level=LogLevel.INFO,
            message="Data ingestion completed",
            event_type="job_complete",
            source_name="json_demo",
            duration_ms=1234.56,
            records_processed=50000,
            metadata={"quality_score": 0.98}
        )
    ]

    print("JSON Output Examples:")
    for i, event in enumerate(events, 1):
        print(f"\n--- Event {i} ---")
        print(json.dumps(event.to_dict(), indent=2))

    print("\n✅ JSON output demo completed\n")


def demo_create_logger_function() -> None:
    """Demonstrate the create_logger convenience function."""
    print("🏗️ Demo: create_logger Convenience Function")
    print("=" * 50)

    # Create logger with Kafka and ClickHouse sinks
    logger = create_logger(
        source_name="convenience_demo",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="aurum.ops.logs",
        clickhouse_url="http://localhost:8123",
        clickhouse_table="aurum_logs",
        dataset="demo_dataset"
    )

    logger.log(LogLevel.INFO, "Logger created successfully", "logger_init")
    logger.log_api_call("/demo/endpoint", "GET", 200, 45.23)
    logger.log_data_quality(100, 95, 5)

    print("✅ create_logger demo completed\n")


def demo_performance_comparison() -> None:
    """Demonstrate performance characteristics."""
    print("⚡ Demo: Performance Comparison")
    print("=" * 50)

    import time

    # Test different logging approaches
    logger = StructuredLogger("perf_test")

    # Traditional logging approach (simulated)
    start_time = time.time()
    for i in range(1000):
        message = f"Processing record {i}"
        timestamp = time.time()
        # Traditional logging would involve string formatting, etc.
    traditional_time = time.time() - start_time

    # Structured logging approach
    start_time = time.time()
    for i in range(1000):
        logger.log(
            LogLevel.DEBUG,
            f"Processing record {i}",
            "record_processing",
            records_processed=i,
            metadata={"record_id": i}
        )
    structured_time = time.time() - start_time

    print("Performance Comparison (1000 log events):")
    print(f"   Traditional approach: {traditional_time".4f"}s")
    print(f"   Structured logging: {structured_time".4f"}s")
    print(f"   Overhead: {((structured_time/traditional_time)-1)*100".1f"}%")

    print("\n✅ Performance demo completed\n")


def main() -> int:
    """Main demo function."""
    print("🚀 Aurum Structured Logging System Demo")
    print("=" * 60)
    print()

    # Run all demos
    demo_basic_logging()
    demo_error_handling()
    demo_batch_processing()
    demo_metrics_collection()
    demo_json_output()
    demo_create_logger_function()
    demo_performance_comparison()

    print("🎉 All demos completed successfully!")
    print()
    print("Key Features Demonstrated:")
    print("  ✅ Structured log events with rich metadata")
    print("  ✅ Different log levels (DEBUG, INFO, WARN, ERROR, FATAL)")
    print("  ✅ Event types (job_start, api_call, data_quality, etc.)")
    print("  ✅ Operation tracking with start/end timing")
    print("  ✅ Error handling with context")
    print("  ✅ Batch processing with progress tracking")
    print("  ✅ Metrics collection and analysis")
    print("  ✅ JSON serialization for external systems")
    print("  ✅ Kafka and ClickHouse integration")
    print("  ✅ Performance characteristics")
    print()
    print("Next Steps:")
    print("  1. Configure Kafka and ClickHouse sinks")
    print("  2. Integrate structured logging into SeaTunnel jobs")
    print("  3. Set up monitoring dashboards for log analytics")
    print("  4. Create alerting rules based on error patterns")

    return 0


if __name__ == "__main__":
    sys.exit(main())
