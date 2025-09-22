"""Comprehensive tests for pipeline orchestrator."""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.external.pipeline.orchestrator import (
    PipelineOrchestrator,
    PipelineConfig,
    DataValidator,
    KafkaSink,
    TimescaleSink,
    IcebergSink
)
from aurum.external.quota_manager import DatasetQuota, QuotaManager
from aurum.test_fixtures import (
    MockHttpResponse,
    MockKafkaProducer,
    MockDatabaseConnection,
    get_test_config,
    get_pipeline_config,
    get_test_quotas,
    create_test_metadata
)
from aurum.test_fixtures.test_data_generators import generate_caiso_lmp_data


class TestPipelineOrchestrator:
    """Test suite for pipeline orchestrator."""

    @pytest.fixture
    def pipeline_config(self):
        """Create pipeline configuration for testing."""
        return PipelineConfig(
            kafka_bootstrap_servers="test:9092",
            timescale_connection="postgresql://test:test@test:5432/test",
            enable_great_expectations=False,
            enable_seatunnel_validation=False,
            circuit_breaker_enabled=False
        )

    @pytest.fixture
    def orchestrator(self, pipeline_config):
        """Create orchestrator instance."""
        return PipelineOrchestrator(pipeline_config)

    @pytest.fixture
    def mock_caiso_collector(self):
        """Create mock CAISO collector."""
        collector = MagicMock()
        collector.ingest_prc_lmp = AsyncMock(return_value=generate_caiso_lmp_data(10))
        collector.ingest_load_and_forecast = AsyncMock(return_value=generate_caiso_lmp_data(5))
        collector.ingest_as_results = AsyncMock(return_value=generate_caiso_lmp_data(3))
        return collector

    @pytest_asyncio.fixture
    async def orchestrator_with_quotas(self, orchestrator):
        """Orchestrator with quota management configured."""
        quotas = get_test_quotas()
        for quota in quotas:
            orchestrator.quota_manager.register_dataset(quota)
        return orchestrator

    def test_orchestrator_initialization(self, orchestrator):
        """Test orchestrator initialization."""
        assert orchestrator.config is not None
        assert orchestrator.quota_manager is not None
        assert orchestrator.metrics is not None
        assert orchestrator.kafka_sink is not None
        assert orchestrator.timescale_sink is not None
        assert orchestrator.iceberg_sink is not None
        assert orchestrator.validator is not None

    @pytest.mark.asyncio
    async def test_orchestrate_caiso_ingest_success(
        self,
        orchestrator_with_quotas,
        mock_caiso_collector
    ):
        """Test successful CAISO ingestion orchestration."""
        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now()

        result = await orchestrator_with_quotas.orchestrate_caiso_ingest(
            mock_caiso_collector,
            start_time,
            end_time,
            ['lmp', 'load', 'asm']
        )

        assert result is not None
        assert result['iso_code'] == 'CAISO'
        assert result['total_records'] > 0
        assert len(result['datasets']) == 3  # lmp, load, asm

        # Check each dataset result
        for dataset_id in ['caiso_lmp', 'caiso_load', 'caiso_asm']:
            assert dataset_id in result['datasets']
            dataset_result = result['datasets'][dataset_id]
            assert dataset_result['status'] == 'success'
            assert dataset_result['records_processed'] > 0
            assert 'validation_results' in dataset_result
            assert 'sink_results' in dataset_result

    @pytest.mark.asyncio
    async def test_quota_management_integration(self, orchestrator_with_quotas):
        """Test quota management during orchestration."""
        # Check that quotas are properly configured
        quota_status = orchestrator_with_quotas.quota_manager.get_quota_status("test_caiso_lmp")
        assert quota_status is not None
        assert quota_status['dataset_id'] == 'test_caiso_lmp'
        assert quota_status['records_limit'] == 10_000

    @pytest.mark.asyncio
    async def test_data_validation_integration(
        self,
        orchestrator_with_quotas,
        mock_caiso_collector
    ):
        """Test data validation during orchestration."""
        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now()

        result = await orchestrator_with_quotas.orchestrate_caiso_ingest(
            mock_caiso_collector,
            start_time,
            end_time,
            ['lmp']
        )

        # Check validation results
        lmp_result = result['datasets']['caiso_lmp']
        validation_results = lmp_result['validation_results']

        assert 'total_records' in validation_results
        assert 'passed' in validation_results
        assert 'summary' in validation_results
        assert validation_results['total_records'] > 0

    @pytest.mark.asyncio
    async def test_error_handling(self, orchestrator_with_quotas):
        """Test error handling in orchestration."""
        # Create a collector that raises an exception
        error_collector = MagicMock()
        error_collector.ingest_prc_lmp = AsyncMock(side_effect=RuntimeError("API Error"))

        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now()

        with pytest.raises(RuntimeError, match="API Error"):
            await orchestrator_with_quotas.orchestrate_caiso_ingest(
                error_collector,
                start_time,
                end_time,
                ['lmp']
            )

    @pytest.mark.asyncio
    async def test_partial_success_handling(self, orchestrator_with_quotas):
        """Test handling of partial success scenarios."""
        # Create a collector where one data type fails
        partial_collector = MagicMock()
        partial_collector.ingest_prc_lmp = AsyncMock(return_value=generate_caiso_lmp_data(10))
        partial_collector.ingest_load_and_forecast = AsyncMock(side_effect=RuntimeError("Load API Error"))
        partial_collector.ingest_as_results = AsyncMock(return_value=generate_caiso_lmp_data(5))

        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now()

        result = await orchestrator_with_quotas.orchestrate_caiso_ingest(
            partial_collector,
            start_time,
            end_time,
            ['lmp', 'load', 'asm']
        )

        # Should have partial success
        assert result['total_records'] > 0  # LMP and ASM succeeded

        # Check individual results
        assert result['datasets']['caiso_lmp']['status'] == 'success'
        assert result['datasets']['caiso_load']['status'] == 'failed'
        assert result['datasets']['caiso_asm']['status'] == 'success'

    def test_get_required_fields(self, orchestrator):
        """Test required fields identification."""
        required_fields = orchestrator._get_required_fields("lmp")
        assert "location_id" in required_fields
        assert "price_total" in required_fields
        assert "interval_start" in required_fields

        required_fields = orchestrator._get_required_fields("load")
        assert "location_id" in required_fields
        assert "load_mw" in required_fields
        assert "interval_start" in required_fields

    @pytest.mark.asyncio
    async def test_health_check(self, orchestrator):
        """Test health check functionality."""
        health = await orchestrator.health_check()

        assert isinstance(health, dict)
        assert 'kafka' in health
        assert 'timescale' in health
        assert 'iceberg' in health
        assert 'circuit_breaker' in health


class TestDataValidator:
    """Test suite for data validator."""

    @pytest.fixture
    def validator(self, pipeline_config):
        """Create data validator instance."""
        return DataValidator(pipeline_config)

    @pytest.fixture
    def sample_lmp_data(self):
        """Generate sample LMP data."""
        return generate_caiso_lmp_data(20)

    @pytest.mark.asyncio
    async def test_validate_batch_success(self, validator, sample_lmp_data):
        """Test successful batch validation."""
        metadata = create_test_metadata("CAISO", "lmp")

        result = await validator.validate_batch(sample_lmp_data, metadata)

        assert result['total_records'] == len(sample_lmp_data)
        assert result['passed'] is True
        assert 'validation_results' in result
        assert 'summary' in result

    @pytest.mark.asyncio
    async def test_validate_batch_with_validation_errors(self, validator):
        """Test batch validation with errors."""
        # Create invalid data
        invalid_data = [
            {"invalid_field": "no_location_id"},
            {"location_id": "test", "price_total": "not_a_number"},
            {}
        ]

        metadata = create_test_metadata("CAISO", "lmp")

        result = await validator.validate_batch(invalid_data, metadata)

        assert result['total_records'] == len(invalid_data)
        assert result['passed'] is False
        assert result['issues'] > 0


class TestKafkaSink:
    """Test suite for Kafka sink."""

    @pytest.fixture
    def kafka_sink(self, pipeline_config):
        """Create Kafka sink instance."""
        return KafkaSink(pipeline_config)

    @pytest.fixture
    def sample_data(self):
        """Generate sample data for testing."""
        return generate_caiso_lmp_data(5)

    @pytest.mark.asyncio
    async def test_write_batch(self, kafka_sink, sample_data):
        """Test writing batch to Kafka."""
        # Mock the producer pool
        with patch.object(kafka_sink, 'producer_pool') as mock_pool:
            mock_pool.produce_to_topic = AsyncMock(return_value={
                "total_messages": len(sample_data),
                "total_batches": 1,
                "max_duration": 0.1,
                "total_throughput": 50.0
            })

            metadata = {
                'iso_code': 'CAISO',
                'data_type': 'lmp',
                'topic': 'test.topic',
                'table': 'test.table'
            }

            result = await kafka_sink.write_batch(sample_data, metadata)

            assert result['total_messages'] == len(sample_data)
            assert result['total_batches'] == 1
            mock_pool.produce_to_topic.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check(self, kafka_sink):
        """Test Kafka sink health check."""
        # Mock the producer pool
        with patch.object(kafka_sink, 'producer_pool') as mock_pool:
            mock_pool.get_producer.return_value.producer = MagicMock()

            health = await kafka_sink.health_check()

            assert health is True
            mock_pool.get_producer.assert_called()


class TestTimescaleSink:
    """Test suite for TimescaleDB sink."""

    @pytest.fixture
    def timescale_sink(self, pipeline_config):
        """Create TimescaleDB sink instance."""
        return TimescaleSink(pipeline_config.timescale_connection)

    @pytest.fixture
    def sample_data(self):
        """Generate sample data for testing."""
        return generate_caiso_lmp_data(5)

    @pytest.mark.asyncio
    async def test_write_batch(self, timescale_sink, sample_data):
        """Test writing batch to TimescaleDB."""
        metadata = {
            'iso_code': 'CAISO',
            'data_type': 'lmp',
            'table': 'test.caiso_lmp'
        }

        result = await timescale_sink.write_batch(sample_data, metadata)

        assert result['rows_inserted'] == len(sample_data)
        assert result['table'] == 'test.caiso_lmp'
        assert result['status'] == 'success'

    @pytest.mark.asyncio
    async def test_health_check(self, timescale_sink):
        """Test TimescaleDB sink health check."""
        health = await timescale_sink.health_check()
        assert health is True


class TestIcebergSink:
    """Test suite for Iceberg sink."""

    @pytest.fixture
    def iceberg_sink(self, pipeline_config):
        """Create Iceberg sink instance."""
        return IcebergSink(pipeline_config)

    @pytest.fixture
    def sample_data(self):
        """Generate sample data for testing."""
        return generate_caiso_lmp_data(5)

    @pytest.mark.asyncio
    async def test_write_batch(self, iceberg_sink, sample_data):
        """Test writing batch to Iceberg."""
        metadata = {
            'iso_code': 'CAISO',
            'data_type': 'lmp',
            'table': 'test.caiso_lmp'
        }

        result = await iceberg_sink.write_batch(sample_data, metadata)

        assert result['files_written'] > 0
        assert result['table'] == 'test.caiso_lmp'
        assert result['records'] == len(sample_data)
        assert result['status'] == 'success'

    @pytest.mark.asyncio
    async def test_health_check(self, iceberg_sink):
        """Test Iceberg sink health check."""
        health = await iceberg_sink.health_check()
        assert health is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
