"""Complete pipeline orchestrator for external data ingestion.

This module orchestrates the full pipeline from external data sources through:
1. Kafka ingestion with optimized producers
2. TimescaleDB for time-series storage
3. Iceberg for analytical storage
4. Data validation and quality checks
5. Monitoring and alerting

Supports circuit breaker patterns, quota management, and comprehensive error handling.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Protocol, Type, Union
from contextlib import asynccontextmanager

from ..collect import (
    ExternalCollector,
    CollectorConfig,
    CollectorContext,
    CheckpointStore,
    PostgresCheckpointStore
)
from ..quota_manager import QuotaManager, DatasetQuota, get_quota_manager
from ...kafka.optimized_producer import OptimizedKafkaProducer, ProducerPool
from ...common.circuit_breaker import CircuitBreaker
from ...dq.iso_validator import IsoDataQualityValidator, get_data_quality_validator
from ...observability.metrics import get_metrics_client


@dataclass
class PipelineConfig:
    """Configuration for the complete ingestion pipeline."""

    # Kafka configuration
    kafka_bootstrap_servers: str
    kafka_topic_prefix: str = "aurum.iso"

    # Schema registry
    schema_registry_url: Optional[str] = None

    # Storage configuration
    timescale_connection: str
    iceberg_catalog: str = "nessie"
    iceberg_warehouse: str = "s3://aurum/curated/iceberg"

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 120

    # Rate limiting
    max_concurrent_ingests: int = 10
    requests_per_minute_per_iso: int = 60

    # Data quality settings
    enable_great_expectations: bool = True
    enable_seatunnel_validation: bool = True

    # Monitoring
    enable_prometheus_metrics: bool = True
    alert_on_schema_drift: bool = True
    alert_on_staleness: bool = True


@dataclass
class DatasetQuota:
    """Quota configuration for a dataset."""

    dataset_id: str
    iso_code: str
    max_records_per_hour: int = 1_000_000
    max_bytes_per_hour: int = 1_000_000_000  # 1GB
    max_concurrent_requests: int = 5
    priority: int = 1  # 1=highest, 10=lowest

    _current_records: int = field(default=0, init=False)
    _current_bytes: int = field(default=0, init=False)
    _hour_start: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0), init=False)

    def check_quota(self, record_count: int, byte_count: int) -> bool:
        """Check if quota allows the given record and byte counts."""
        now = datetime.now(timezone.utc)
        if now.hour != self._hour_start.hour:
            # Reset counters for new hour
            self._current_records = 0
            self._current_bytes = 0
            self._hour_start = now.replace(minute=0, second=0, microsecond=0)

        return (
            self._current_records + record_count <= self.max_records_per_hour and
            self._current_bytes + byte_count <= self.max_bytes_per_hour
        )

    def update_usage(self, record_count: int, byte_count: int):
        """Update usage counters."""
        self._current_records += record_count
        self._current_bytes += byte_count


class QuotaManager:
    """Manages quotas across multiple datasets."""

    def __init__(self):
        self.datasets: Dict[str, DatasetQuota] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    def register_dataset(self, quota: DatasetQuota):
        """Register a dataset quota."""
        self.datasets[quota.dataset_id] = quota
        self._semaphores[quota.dataset_id] = asyncio.Semaphore(quota.max_concurrent_requests)

    async def acquire_quota(self, dataset_id: str, record_count: int, byte_count: int) -> bool:
        """Acquire quota for a dataset operation."""
        if dataset_id not in self.datasets:
            return True  # No quota configured, allow

        quota = self.datasets[dataset_id]

        # Check rate limits
        if not quota.check_quota(record_count, byte_count):
            return False

        # Acquire concurrency limit
        try:
            await asyncio.wait_for(
                self._semaphores[dataset_id].acquire(),
                timeout=30.0
            )
            return True
        except asyncio.TimeoutError:
            return False

    def release_quota(self, dataset_id: str):
        """Release quota for a dataset operation."""
        if dataset_id in self._semaphores:
            self._semaphores[dataset_id].release()

    def update_usage(self, dataset_id: str, record_count: int, byte_count: int):
        """Update usage for a dataset."""
        if dataset_id in self.datasets:
            self.datasets[dataset_id].update_usage(record_count, byte_count)


class PipelineMetrics:
    """Metrics collection for the pipeline."""

    def __init__(self):
        self.metrics = get_metrics_client()
        self.counters = {
            'kafka_messages_produced': 0,
            'timescale_rows_inserted': 0,
            'iceberg_files_written': 0,
            'validation_errors': 0,
            'circuit_breaker_trips': 0,
            'quota_violations': 0
        }

    def increment(self, metric: str, value: int = 1):
        """Increment a counter metric."""
        if metric in self.counters:
            self.counters[metric] += value
        self.metrics.increment_counter(f"pipeline.{metric}", value)

    def gauge(self, metric: str, value: Union[int, float]):
        """Set a gauge metric."""
        self.metrics.gauge(f"pipeline.{metric}", value)


class DataSink(Protocol):
    """Protocol for data sinks in the pipeline."""

    async def write_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Write a batch of records to the sink."""
        ...

    async def health_check(self) -> bool:
        """Check if the sink is healthy."""
        ...


class KafkaSink:
    """Kafka sink for streaming data."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.producer_pool = ProducerPool(
            pool_size=3,
            bootstrap_servers=config.kafka_bootstrap_servers,
            schema_registry_url=config.schema_registry_url
        )
        self.metrics = PipelineMetrics()

    async def write_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Write records to Kafka."""
        topic = metadata.get('topic', f"{self.config.kafka_topic_prefix}.{metadata.get('iso_code', 'unknown')}.{metadata.get('data_type', 'unknown')}.v1")

        # Ensure topic exists
        producer = self.producer_pool.get_producer()
        producer.create_topic_if_not_exists(topic)

        # Add metadata to records
        for record in records:
            record['pipeline_metadata'] = metadata
            record['ingest_timestamp'] = datetime.now(timezone.utc).isoformat()

        # Produce to Kafka
        result = await self.producer_pool.produce_to_topic(topic, records)

        self.metrics.increment('kafka_messages_produced', result['total_messages'])
        return result

    async def health_check(self) -> bool:
        """Check Kafka connectivity."""
        try:
            producer = self.producer_pool.get_producer()
            return producer.producer is not None
        except Exception:
            return False


class TimescaleSink:
    """TimescaleDB sink for time-series data."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.metrics = PipelineMetrics()

    async def write_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Write records to TimescaleDB."""
        # This would implement TimescaleDB insertion
        # For now, return mock results
        inserted_count = len(records)
        self.metrics.increment('timescale_rows_inserted', inserted_count)

        return {
            'rows_inserted': inserted_count,
            'table': metadata.get('table', 'iso_data'),
            'status': 'success'
        }

    async def health_check(self) -> bool:
        """Check TimescaleDB connectivity."""
        # Implement actual health check
        return True


class IcebergSink:
    """Iceberg sink for analytical storage."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metrics = PipelineMetrics()

    async def write_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Write records to Iceberg table."""
        # This would implement Iceberg table writing
        # For now, return mock results
        file_count = len(records) // 1000 + 1  # Rough estimate
        self.metrics.increment('iceberg_files_written', file_count)

        return {
            'files_written': file_count,
            'table': metadata.get('table', 'iso_data'),
            'records': len(records),
            'status': 'success'
        }

    async def health_check(self) -> bool:
        """Check Iceberg connectivity."""
        # Implement actual health check
        return True


class DataValidator:
    """Comprehensive data validation for the pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.validator = asyncio.run(get_data_quality_validator())
        self.metrics = PipelineMetrics()

    async def validate_batch(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a batch of records using comprehensive validation."""
        # Run comprehensive validation
        validation_results = await self.validator.validate_batch(records, metadata)

        # Add basic validation metrics
        total_records = len(records)
        summary = self.validator.get_validation_summary(validation_results)

        # Emit validation metrics
        if not summary['overall_passed']:
            self.metrics.increment('validation_errors', summary['total_issues'])

        return {
            'total_records': total_records,
            'validation_results': validation_results,
            'summary': summary,
            'passed': summary['overall_passed'],
            'issues': summary['total_issues']
        }


class PipelineOrchestrator:
    """Main orchestrator for the complete ingestion pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.quota_manager = asyncio.run(get_quota_manager())
        self.metrics = PipelineMetrics()
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_timeout
        ) if config.circuit_breaker_enabled else None

        # Initialize sinks
        self.kafka_sink = KafkaSink(config)
        self.timescale_sink = TimescaleSink(config.timescale_connection)
        self.iceberg_sink = IcebergSink(config)
        self.validator = DataValidator(config)

        # Track active ingests
        self._active_ingests: Dict[str, asyncio.Task] = {}

    async def orchestrate_caiso_ingest(
        self,
        iso_collector: Any,
        start_time: datetime,
        end_time: datetime,
        data_types: List[str] = None
    ) -> Dict[str, Any]:
        """Orchestrate CAISO data ingestion through the complete pipeline."""

        if data_types is None:
            data_types = ['lmp', 'load', 'asm']

        results = {
            'iso_code': 'CAISO',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'data_types': data_types,
            'datasets': {},
            'total_records': 0,
            'pipeline_metrics': {}
        }

        # Check circuit breaker
        if self.circuit_breaker and self.circuit_breaker.is_open():
            raise RuntimeError("Circuit breaker is open for CAISO ingestion")

        try:
            # Create ingestion tasks for each data type
            tasks = []
            for data_type in data_types:
                dataset_id = f"caiso_{data_type}"
                task = asyncio.create_task(
                    self._ingest_dataset(iso_collector, data_type, dataset_id, start_time, end_time)
                )
                self._active_ingests[dataset_id] = task
                tasks.append(task)

            # Wait for all tasks to complete
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for i, result in enumerate(task_results):
                data_type = data_types[i]
                dataset_id = f"caiso_{data_type}"

                if isinstance(result, Exception):
                    results['datasets'][dataset_id] = {
                        'status': 'failed',
                        'error': str(result)
                    }
                    if self.circuit_breaker:
                        self.circuit_breaker.record_failure()
                else:
                    results['datasets'][dataset_id] = result
                    results['total_records'] += result.get('records_processed', 0)
                    if self.circuit_breaker:
                        self.circuit_breaker.record_success()

            # Record success in circuit breaker
            if self.circuit_breaker:
                self.circuit_breaker.record_success()

        except Exception as e:
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()
            raise
        finally:
            # Clean up active ingests
            for task in self._active_ingests.values():
                if not task.done():
                    task.cancel()
            self._active_ingests.clear()

        results['pipeline_metrics'] = self.metrics.counters
        return results

    async def _ingest_dataset(
        self,
        iso_collector: Any,
        data_type: str,
        dataset_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Ingest a single dataset through the pipeline."""

        dataset_result = {
            'dataset_id': dataset_id,
            'data_type': data_type,
            'status': 'success',
            'records_processed': 0,
            'validation_results': {},
            'sink_results': {},
            'errors': []
        }

        try:
            # Check quota
            allowed, reason = await self.quota_manager.check_and_acquire_quota(
                dataset_id,
                1000,  # Estimated record count
                1000000  # Estimated byte count
            )

            if not allowed:
                self.metrics.increment('quota_violations')
                raise RuntimeError(f"Quota check failed for dataset {dataset_id}: {reason}")

            # Collect data from ISO
            if data_type == 'lmp':
                records = await iso_collector.ingest_prc_lmp(
                    start_utc=start_time,
                    end_utc=end_time,
                    market_run_id='RTM'
                )
            elif data_type == 'load':
                records = await iso_collector.ingest_load_and_forecast(
                    start_utc=start_time,
                    end_utc=end_time,
                    market_run_id='RTM'
                )
            elif data_type == 'asm':
                records = await iso_collector.ingest_as_results(
                    start_utc=start_time,
                    end_utc=end_time,
                    market_run_id='RTM'
                )
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

            dataset_result['records_processed'] = len(records)

            # Validate data
            metadata = {
                'iso_code': 'CAISO',
                'data_type': data_type,
                'required_fields': self._get_required_fields(data_type),
                'topic': f"{self.config.kafka_topic_prefix}.caiso.{data_type}.v1",
                'table': f"caiso.{data_type}"
            }

            validation_results = await self.validator.validate_batch(records, metadata)
            dataset_result['validation_results'] = validation_results

            if not validation_results['passed']:
                raise RuntimeError(f"Validation failed: {validation_results['issues']} issues found in {validation_results['total_records']} records")

            # Write to sinks
            sink_results = {}

            # Kafka
            kafka_result = await self.kafka_sink.write_batch(records, metadata)
            sink_results['kafka'] = kafka_result

            # TimescaleDB
            timescale_result = await self.timescale_sink.write_batch(records, metadata)
            sink_results['timescale'] = timescale_result

            # Iceberg
            iceberg_result = await self.iceberg_sink.write_batch(records, metadata)
            sink_results['iceberg'] = iceberg_result

            dataset_result['sink_results'] = sink_results

            # Update quota usage with actual counts
            record_count = len(records)
            byte_count = sum(len(str(r)) for r in records)
            self.quota_manager.release_quota(dataset_id, record_count, byte_count)

        except Exception as e:
            dataset_result['status'] = 'failed'
            dataset_result['errors'].append(str(e))
            # Still release quota even on failure
            self.quota_manager.release_quota(dataset_id, 0, 0)
            raise

        return dataset_result

    def _get_required_fields(self, data_type: str) -> List[str]:
        """Get required fields for a data type."""
        field_maps = {
            'lmp': ['location_id', 'price_total', 'interval_start'],
            'load': ['location_id', 'load_mw', 'interval_start'],
            'asm': ['location_id', 'price_mcp', 'product', 'interval_start']
        }
        return field_maps.get(data_type, [])

    async def health_check(self) -> Dict[str, bool]:
        """Check health of all pipeline components."""
        health_results = {}

        # Check sinks
        health_results['kafka'] = await self.kafka_sink.health_check()
        health_results['timescale'] = await self.timescale_sink.health_check()
        health_results['iceberg'] = await self.iceberg_sink.health_check()

        # Check circuit breaker
        health_results['circuit_breaker'] = not (self.circuit_breaker and self.circuit_breaker.is_open())

        return health_results


# Convenience function to create a pipeline for CAISO
async def create_caiso_pipeline(config: PipelineConfig) -> PipelineOrchestrator:
    """Create and initialize a CAISO pipeline."""
    pipeline = PipelineOrchestrator(config)

    # Register CAISO quotas
    for data_type in ['lmp', 'load', 'asm']:
        quota = DatasetQuota(
            dataset_id=f"caiso_{data_type}",
            iso_code="CAISO",
            max_records_per_hour=500_000,
            max_bytes_per_hour=500_000_000,
            max_concurrent_requests=3,
            priority=1
        )
        pipeline.quota_manager.register_dataset(quota)

    return pipeline
