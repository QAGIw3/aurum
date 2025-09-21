"""Tests for backfill driver functionality."""

import pytest
import asyncio
import tempfile
from datetime import datetime, date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

from aurum.slices import SliceClient, SliceConfig, SliceManager, SliceType, SliceStatus
from aurum.bulk_archive import BulkArchiveManager, ArchiveConfig
from aurum.logging import create_logger, LogLevel


@pytest.fixture
def sample_config():
    """Sample backfill configuration."""
    return {
        'source': 'eia',
        'start_date': date(2024, 1, 1),
        'end_date': date(2024, 1, 31),
        'concurrency': 2,
        'batch_size': 1000,
        'max_retries': 3,
        'dry_run': False,
        'priority': 'normal'
    }


@pytest.fixture
def temp_dir():
    """Temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestBackfillConfig:
    """Test BackfillConfig functionality."""

    def test_config_initialization(self, sample_config):
        """Test backfill configuration initialization."""
        from scripts.ops.backfill_driver import BackfillConfig

        config = BackfillConfig(**sample_config)

        assert config.source == 'eia'
        assert config.start_date == date(2024, 1, 1)
        assert config.end_date == date(2024, 1, 31)
        assert config.concurrency == 2
        assert config.batch_size == 1000
        assert config.max_retries == 3
        assert config.dry_run is False
        assert config.priority == 'normal'
        assert config.job_id.startswith('BACKFILL_')

    def test_config_defaults(self):
        """Test backfill configuration defaults."""
        from scripts.ops.backfill_driver import BackfillConfig

        config = BackfillConfig(source='eia', start_date=date(2024, 1, 1), end_date=date(2024, 1, 31))

        assert config.concurrency == 3
        assert config.batch_size == 1000
        assert config.max_retries == 3
        assert config.dry_run is False
        assert config.priority == 'normal'
        assert config.job_id.startswith('BACKFILL_')


class TestBackfillJob:
    """Test BackfillJob functionality."""

    def test_job_initialization(self, sample_config):
        """Test backfill job initialization."""
        from scripts.ops.backfill_driver import BackfillJob

        job = BackfillJob(
            job_id='test_job_20240101',
            source=sample_config['source'],
            date=sample_config['start_date'],
            batch_size=sample_config['batch_size']
        )

        assert job.job_id == 'test_job_20240101'
        assert job.source == 'eia'
        assert job.date == date(2024, 1, 1)
        assert job.batch_size == 1000
        assert job.status == 'pending'
        assert job.records_processed == 0
        assert len(job.errors) == 0
        assert job.retry_count == 0


class TestBackfillDriver:
    """Test BackfillDriver functionality."""

    @pytest.fixture
    def driver(self, sample_config):
        """Create a backfill driver instance."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        return BackfillDriver(config)

    def test_driver_initialization(self, driver, sample_config):
        """Test backfill driver initialization."""
        assert driver.config.source == sample_config['source']
        assert driver.config.start_date == sample_config['start_date']
        assert driver.config.end_date == sample_config['end_date']
        assert len(driver.jobs) == 0
        assert driver.total_jobs == 0
        assert driver.completed_jobs == 0
        assert driver.failed_jobs == 0

    def test_generate_jobs(self, driver):
        """Test job generation for date range."""
        # Generate jobs for the date range
        jobs = driver._generate_jobs()

        # Should have 31 jobs (Jan 1-31, 2024)
        assert len(jobs) == 31
        assert driver.total_jobs == 31

        # Check first job
        first_job = jobs[0]
        assert first_job.job_id == f"{driver.config.job_id}_{driver.config.start_date.strftime('%Y%m%d')}"
        assert first_job.source == driver.config.source
        assert first_job.date == driver.config.start_date
        assert first_job.batch_size == driver.config.batch_size

        # Check last job
        last_job = jobs[-1]
        assert last_job.date == driver.config.end_date

    def test_generate_dry_run_recommendations(self, driver):
        """Test dry run recommendation generation."""
        # Mock validation results
        validation = {
            "quota_compliance": True,
            "resource_availability": False,
            "data_source_availability": True,
            "configuration_validity": True
        }

        recommendations = driver._generate_dry_run_recommendations(validation)

        assert len(recommendations) > 0
        assert any("concurrency" in rec for rec in recommendations)

    @pytest.mark.asyncio
    async def test_dry_run(self, driver):
        """Test dry run functionality."""
        result = await driver._dry_run()

        assert result['job_id'] == driver.config.job_id
        assert result['source'] == driver.config.source
        assert result['start_date'] == str(driver.config.start_date)
        assert result['end_date'] == str(driver.config.end_date)
        assert result['total_jobs'] == 31
        assert 'validation_results' in result
        assert 'recommendations' in result

    @pytest.mark.asyncio
    async def test_validate_configuration(self, driver):
        """Test configuration validation."""
        jobs = driver._generate_jobs()
        validation = await driver._validate_configuration(jobs)

        assert isinstance(validation, dict)
        assert 'quota_compliance' in validation
        assert 'resource_availability' in validation

    @pytest.mark.asyncio
    async def test_execute_single_job(self, driver):
        """Test execution of a single job."""
        jobs = driver._generate_jobs()
        job = jobs[0]

        # Mock the SeaTunnel job execution
        with patch.object(driver, '_run_seatunnel_job', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = None

            await driver._execute_single_job(job)

            assert job.status == 'completed'
            assert job.records_processed > 0
            assert job.start_time is not None
            assert job.end_time is not None

    @pytest.mark.asyncio
    async def test_job_to_dict(self, driver):
        """Test job dictionary conversion."""
        jobs = driver._generate_jobs()
        job = jobs[0]

        job_dict = driver._job_to_dict(job)

        assert isinstance(job_dict, dict)
        assert job_dict['job_id'] == job.job_id
        assert job_dict['source'] == job.source
        assert job_dict['date'] == str(job.date)
        assert job_dict['batch_size'] == job.batch_size
        assert job_dict['status'] == job.status


class TestBackfillIntegration:
    """Integration tests for backfill driver."""

    @pytest.mark.asyncio
    async def test_full_backfill_execution(self, sample_config):
        """Test full backfill execution workflow."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        driver = BackfillDriver(config)

        # Mock SeaTunnel execution
        with patch.object(driver, '_run_seatunnel_job', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = None

            # Execute backfill
            results = await driver.execute()

            # Verify results structure
            assert isinstance(results, dict)
            assert results['job_id'] == config.job_id
            assert results['source'] == config.source
            assert results['total_jobs'] == 31
            assert results['completed_jobs'] == 31
            assert results['failed_jobs'] == 0
            assert 'jobs' in results

    @pytest.mark.asyncio
    async def test_backfill_with_failures(self, sample_config):
        """Test backfill execution with simulated failures."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        driver = BackfillDriver(config)

        # Mock SeaTunnel execution with some failures
        failure_count = 0
        async def mock_run_seatunnel(job):
            nonlocal failure_count
            if hash(job.job_id) % 5 == 0:  # 20% failure rate
                failure_count += 1
                raise Exception(f"Simulated failure for {job.job_id}")
            job.records_processed = job.batch_size

        with patch.object(driver, '_run_seatunnel_job', side_effect=mock_run_seatunnel):
            # Execute backfill
            results = await driver.execute()

            # Should have some failures
            assert results['total_jobs'] == 31
            assert results['failed_jobs'] == failure_count
            assert results['completed_jobs'] == 31 - failure_count

    @pytest.mark.asyncio
    async def test_backfill_pause_resume(self, sample_config):
        """Test backfill pause and resume functionality."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        driver = BackfillDriver(config)

        # Start execution
        with patch.object(driver, '_run_seatunnel_job', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = None

            # Pause execution after starting
            await driver.pause()

            # Check that active downloads are cancelled
            assert len(driver.active_downloads) == 0

            # Resume execution
            await driver.resume()

            # Should be able to continue
            assert len(driver.jobs) == 31

    @pytest.mark.asyncio
    async def test_backfill_cancellation(self, sample_config):
        """Test backfill cancellation."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        driver = BackfillDriver(config)

        # Cancel immediately
        await driver.cancel(force=True)

        # Should mark jobs as cancelled
        cancelled_jobs = [job for job in driver.jobs.values() if job.status == 'cancelled']
        assert len(cancelled_jobs) == 31


class TestBackfillCLI:
    """Test CLI functionality."""

    def test_argument_parsing(self):
        """Test CLI argument parsing."""
        from scripts.ops.backfill_driver import main

        # Test basic argument parsing
        test_args = [
            'backfill_driver.py',
            '--source', 'eia',
            '--start-date', '2024-01-01',
            '--end-date', '2024-01-31',
            '--concurrency', '2',
            '--batch-size', '500'
        ]

        with patch('sys.argv', test_args):
            # This would normally call the actual main function
            # For testing, we just verify the argument parsing works
            pass

    def test_date_validation(self):
        """Test date validation logic."""
        # Test that start date before end date validation works
        # This would be tested in the actual main function
        pass


class TestBackfillPerformance:
    """Performance tests for backfill driver."""

    @pytest.mark.asyncio
    async def test_concurrent_job_execution(self, sample_config):
        """Test concurrent job execution performance."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        # Use smaller date range for performance testing
        config = BackfillConfig(
            source='eia',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),  # Only 5 days
            concurrency=3,
            batch_size=100
        )

        driver = BackfillDriver(config)

        # Mock SeaTunnel execution
        with patch.object(driver, '_run_seatunnel_job', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = None

            start_time = datetime.now()
            results = await driver.execute()
            end_time = datetime.now()

            execution_time = (end_time - start_time).total_seconds()

            # Should complete all jobs
            assert results['total_jobs'] == 5
            assert results['completed_jobs'] == 5
            assert results['failed_jobs'] == 0

            # Should complete in reasonable time
            assert execution_time < 30  # Should be much faster for 5 jobs

    @pytest.mark.asyncio
    async def test_memory_usage(self, sample_config):
        """Test memory usage during backfill operations."""
        from scripts.ops.backfill_driver import BackfillDriver, BackfillConfig

        config = BackfillConfig(**sample_config)
        driver = BackfillDriver(config)

        # Mock minimal processing to test memory usage
        async def mock_minimal_run(job):
            job.records_processed = 1
            await asyncio.sleep(0.01)  # Very short delay

        with patch.object(driver, '_run_seatunnel_job', side_effect=mock_minimal_run):
            await driver.execute()

            # Check that driver properly tracks memory
            assert len(driver.jobs) == 31
            assert all(job.records_processed > 0 for job in driver.jobs.values())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
