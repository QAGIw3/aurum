"""Unit tests for TrainingJobsService."""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from src.aurum.services.ml.training_jobs import (
    TrainingJobsService,
    TrainingJob,
    TrainingJobStatus,
    ModelConfig,
    TrainingJobRepository
)


class TestTrainingJobsService:
    """Test suite for TrainingJobsService."""
    
    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        repo = Mock(spec=TrainingJobRepository)
        repo.save_job = AsyncMock()
        repo.get_job = AsyncMock()
        repo.list_jobs = AsyncMock()
        repo.update_job_status = AsyncMock()
        return repo
    
    @pytest.fixture
    def service(self, mock_repository):
        """Create service instance with mock repository."""
        return TrainingJobsService(
            repository=mock_repository,
            cache_enabled=False  # Disable cache for unit tests
        )
    
    @pytest.fixture
    def model_config(self):
        """Create a sample model configuration."""
        return ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100, "max_depth": 6},
            feature_selection=["feature1", "feature2", "feature3"],
            target_variable="target"
        )
    
    @pytest.mark.asyncio
    async def test_start_training_job(self, service, mock_repository, model_config):
        """Test starting a new training job."""
        # Setup
        model_name = "test_model"
        created_by = "test_user"
        
        mock_repository.save_job.side_effect = lambda j: j
        
        # Execute
        with patch.object(service, '_simulate_training', new_callable=AsyncMock):
            job_id = await service.start_training_job(
                model_name=model_name,
                config=model_config,
                created_by=created_by,
                metadata={"experiment": "test"}
            )
        
        # Assert
        assert isinstance(job_id, str)
        assert job_id in service._active_jobs
        assert job_id in service._job_tasks
        
        # Verify job was saved
        mock_repository.save_job.assert_called_once()
        saved_job = mock_repository.save_job.call_args[0][0]
        assert isinstance(saved_job, TrainingJob)
        assert saved_job.model_name == model_name
        assert saved_job.config == model_config
        assert saved_job.created_by == created_by
        assert saved_job.status == TrainingJobStatus.PENDING
        assert saved_job.metrics == {"experiment": "test"}
    
    @pytest.mark.asyncio
    async def test_update_training_job_progress(self, service, mock_repository, model_config):
        """Test updating job progress."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.PENDING
        )
        
        mock_repository.get_job.return_value = job
        mock_repository.save_job.side_effect = lambda j: j
        
        # Execute
        result = await service.update_training_job_progress(
            job_id=job.job_id,
            progress=50.0,
            stage="training",
            metrics={"loss": 0.5}
        )
        
        # Assert
        assert result.progress == 50.0
        assert result.current_stage == "training"
        assert result.status == TrainingJobStatus.RUNNING
        assert "training" in result.stages_completed
        assert result.metrics["loss"] == 0.5
        assert result.started_at is not None
        
        mock_repository.save_job.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_progress_invalid_state(self, service, mock_repository, model_config):
        """Test updating progress for job in terminal state."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.COMPLETED
        )
        
        service._active_jobs[job.job_id] = job
        
        # Execute & Assert
        with pytest.raises(ValueError, match="Cannot update progress for job in completed state"):
            await service.update_training_job_progress(
                job_id=job.job_id,
                progress=50.0
            )
    
    @pytest.mark.asyncio
    async def test_update_progress_job_not_found(self, service, mock_repository):
        """Test updating progress for non-existent job."""
        # Setup
        mock_repository.get_job.return_value = None
        
        # Execute & Assert
        with pytest.raises(ValueError, match="Training job test_job not found"):
            await service.update_training_job_progress(
                job_id="test_job",
                progress=50.0
            )
    
    @pytest.mark.asyncio
    async def test_complete_training_job(self, service, mock_repository, model_config):
        """Test completing a training job."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        service._active_jobs[job.job_id] = job
        task = Mock()
        task.done.return_value = False
        service._job_tasks[job.job_id] = task
        
        mock_repository.get_job.return_value = job
        mock_repository.save_job.side_effect = lambda j: j
        
        # Execute
        result = await service.complete_training_job(
            job_id=job.job_id,
            model_version_id="version_123",
            final_metrics={"accuracy": 0.95}
        )
        
        # Assert
        assert result.status == TrainingJobStatus.COMPLETED
        assert result.progress == 100.0
        assert result.completed_at is not None
        assert result.model_version_id == "version_123"
        assert result.metrics["accuracy"] == 0.95
        
        # Verify cleanup
        assert job.job_id not in service._active_jobs
        assert job.job_id not in service._job_tasks
        task.cancel.assert_called_once()
        
        mock_repository.save_job.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_fail_training_job(self, service, mock_repository, model_config):
        """Test failing a training job."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.RUNNING
        )
        
        service._active_jobs[job.job_id] = job
        mock_repository.get_job.return_value = job
        mock_repository.save_job.side_effect = lambda j: j
        
        # Execute
        result = await service.fail_training_job(
            job_id=job.job_id,
            error_message="Out of memory",
            error_details={"memory_used": "16GB"}
        )
        
        # Assert
        assert result.status == TrainingJobStatus.FAILED
        assert result.error_message == "Out of memory"
        assert result.completed_at is not None
        assert result.metrics["error_details"] == {"memory_used": "16GB"}
        
        assert job.job_id not in service._active_jobs
        mock_repository.save_job.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cancel_training_job(self, service, mock_repository, model_config):
        """Test cancelling a training job."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.RUNNING
        )
        
        service._active_jobs[job.job_id] = job
        task = Mock()
        task.done.return_value = False
        service._job_tasks[job.job_id] = task
        
        mock_repository.get_job.return_value = job
        mock_repository.save_job.side_effect = lambda j: j
        
        # Execute
        result = await service.cancel_training_job(
            job_id=job.job_id,
            reason="User requested"
        )
        
        # Assert
        assert result.status == TrainingJobStatus.CANCELLED
        assert result.error_message == "User requested"
        assert result.completed_at is not None
        
        assert job.job_id not in service._active_jobs
        assert job.job_id not in service._job_tasks
        task.cancel.assert_called_once()
        
        mock_repository.save_job.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cancel_completed_job(self, service, mock_repository, model_config):
        """Test cancelling a completed job fails."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.COMPLETED
        )
        
        mock_repository.get_job.return_value = job
        
        # Execute & Assert
        with pytest.raises(ValueError, match="Cannot cancel job in completed state"):
            await service.cancel_training_job(job.job_id)
    
    @pytest.mark.asyncio
    async def test_get_training_job(self, service, mock_repository, model_config):
        """Test getting a training job."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config
        )
        
        mock_repository.get_job.return_value = job
        
        # Execute
        result = await service.get_training_job(job.job_id)
        
        # Assert
        assert result == job
        mock_repository.get_job.assert_called_once_with(job.job_id)
    
    @pytest.mark.asyncio
    async def test_get_training_job_from_active(self, service, model_config):
        """Test getting job from active jobs."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config
        )
        service._active_jobs[job.job_id] = job
        
        # Execute
        result = await service.get_training_job(job.job_id)
        
        # Assert
        assert result == job
        # Repository should not be called
    
    @pytest.mark.asyncio
    async def test_get_training_job_status(self, service, mock_repository, model_config):
        """Test getting job status."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.RUNNING
        )
        
        mock_repository.get_job.return_value = job
        
        # Execute
        result = await service.get_training_job_status(job.job_id)
        
        # Assert
        assert result == TrainingJobStatus.RUNNING
    
    @pytest.mark.asyncio
    async def test_get_training_job_status_not_found(self, service, mock_repository):
        """Test getting status for non-existent job."""
        # Setup
        mock_repository.get_job.return_value = None
        
        # Execute
        result = await service.get_training_job_status("non_existent")
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_list_training_jobs(self, service, mock_repository, model_config):
        """Test listing training jobs."""
        # Setup
        jobs = [
            TrainingJob(
                job_id="job1",
                model_name="model1",
                config=model_config,
                status=TrainingJobStatus.RUNNING
            ),
            TrainingJob(
                job_id="job2",
                model_name="model2",
                config=model_config,
                status=TrainingJobStatus.COMPLETED
            )
        ]
        
        mock_repository.list_jobs.return_value = jobs
        
        # Execute
        result = await service.list_training_jobs(
            status=TrainingJobStatus.RUNNING,
            limit=50
        )
        
        # Assert
        assert result == jobs
        mock_repository.list_jobs.assert_called_once_with(
            model_name=None,
            status=TrainingJobStatus.RUNNING,
            created_by=None,
            limit=50,
            offset=0
        )
    
    @pytest.mark.asyncio
    async def test_get_active_jobs(self, service, mock_repository, model_config):
        """Test getting active jobs."""
        # Setup
        pending_jobs = [
            TrainingJob(
                job_id="job1",
                model_name="model1",
                config=model_config,
                status=TrainingJobStatus.PENDING
            )
        ]
        running_jobs = [
            TrainingJob(
                job_id="job2",
                model_name="model2",
                config=model_config,
                status=TrainingJobStatus.RUNNING
            )
        ]
        
        mock_repository.list_jobs.side_effect = [pending_jobs, running_jobs]
        
        # Execute
        result = await service.get_active_jobs()
        
        # Assert
        assert len(result) == 2
        assert result[0].status == TrainingJobStatus.PENDING
        assert result[1].status == TrainingJobStatus.RUNNING
        
        # Verify both statuses were queried
        assert mock_repository.list_jobs.call_count == 2
    
    @pytest.mark.asyncio
    async def test_simulate_training(self, service, mock_repository, model_config):
        """Test the training simulation."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config,
            status=TrainingJobStatus.PENDING
        )
        
        service._active_jobs[job.job_id] = job
        mock_repository.get_job.return_value = job
        mock_repository.save_job.side_effect = lambda j: j
        
        # Mock the update and complete methods
        with patch.object(service, 'update_training_job_progress', new_callable=AsyncMock) as mock_update:
            with patch.object(service, 'complete_training_job', new_callable=AsyncMock) as mock_complete:
                # Execute simulation with very short sleep
                with patch('asyncio.sleep', new_callable=AsyncMock):
                    await service._simulate_training(job.job_id)
        
        # Assert progress was updated for each stage
        assert mock_update.call_count == 6  # 6 stages
        
        # Assert job was completed
        mock_complete.assert_called_once()
        complete_args = mock_complete.call_args[1]
        assert complete_args['job_id'] == job.job_id
        assert 'model_version_id' in complete_args
        assert 'final_metrics' in complete_args
    
    @pytest.mark.asyncio
    async def test_simulate_training_cancelled(self, service):
        """Test training simulation when job is cancelled."""
        # Setup
        job_id = "test_job_id"
        
        # Remove job from active jobs to simulate cancellation
        service._active_jobs.pop(job_id, None)
        
        # Execute
        await service._simulate_training(job_id)
        
        # Assert - should return early without error
        # No assertions needed - just ensure no exceptions
    
    @pytest.mark.asyncio
    async def test_simulate_training_error(self, service, mock_repository, model_config):
        """Test training simulation with error."""
        # Setup
        job = TrainingJob(
            job_id="test_job_id",
            model_name="test_model",
            config=model_config
        )
        
        service._active_jobs[job.job_id] = job
        
        # Mock update to raise error
        with patch.object(service, 'update_training_job_progress', side_effect=Exception("Training error")):
            with patch.object(service, 'fail_training_job', new_callable=AsyncMock) as mock_fail:
                await service._simulate_training(job.job_id)
        
        # Assert job was failed
        mock_fail.assert_called_once()
        fail_args = mock_fail.call_args[1]
        assert fail_args['job_id'] == job.job_id
        assert fail_args['error_message'] == "Training error"
        assert fail_args['error_details']['exception_type'] == "Exception"
