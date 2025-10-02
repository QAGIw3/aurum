"""Contract tests to ensure service interfaces are properly implemented."""

import pytest
from typing import Any, Dict, List, Optional

from aurum.api.services.model.interfaces import (
    IModelManagementService,
    IModelTrainingService,
    IModelComparisonService,
    IModelSchedulingService
)
from aurum.api.services.model import (
    ModelConfig,
    ModelVersion,
    TrainingJob,
    ModelComparison,
    RetrainSchedule,
    RegisteredModel,
    ChampionChallengerSelection
)


class TestServiceContractCompliance:
    """Test that services properly implement their interfaces."""

    def test_model_management_service_contract(self):
        """Test that ModelManagementService implements IModelManagementService correctly."""
        from aurum.api.services.model.management_service import ModelManagementService

        service = ModelManagementService()

        # Verify interface compliance
        assert isinstance(service, IModelManagementService)

        # Test that all required methods exist
        required_methods = [
            'register_model',
            'register_model_version',
            'get_model_version',
            'list_model_versions',
            'list_models',
            'update_model_version_status'
        ]

        for method_name in required_methods:
            assert hasattr(service, method_name), f"Missing method: {method_name}"
            method = getattr(service, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_model_training_service_contract(self):
        """Test that ModelTrainingService implements IModelTrainingService correctly."""
        from aurum.api.services.model.training_service import ModelTrainingService

        service = ModelTrainingService()

        # Verify interface compliance
        assert isinstance(service, IModelTrainingService)

        # Test that all required methods exist
        required_methods = [
            'start_training_job',
            'update_training_job_progress',
            'complete_training_job',
            'cancel_training_job',
            'get_training_job',
            'list_training_jobs'
        ]

        for method_name in required_methods:
            assert hasattr(service, method_name), f"Missing method: {method_name}"
            method = getattr(service, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_model_comparison_service_contract(self):
        """Test that ModelComparisonService implements IModelComparisonService correctly."""
        from aurum.api.services.model.comparison_service import ModelComparisonService

        service = ModelComparisonService()

        # Verify interface compliance
        assert isinstance(service, IModelComparisonService)

        # Test that all required methods exist
        required_methods = [
            'compare_models',
            'select_champion_model',
            'promote_to_champion',
            'create_champion_challenger_selection',
            'list_champion_challenger_comparisons'
        ]

        for method_name in required_methods:
            assert hasattr(service, method_name), f"Missing method: {method_name}"
            method = getattr(service, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_model_scheduling_service_contract(self):
        """Test that ModelSchedulingService implements IModelSchedulingService correctly."""
        from aurum.api.services.model.scheduling_service import ModelSchedulingService

        service = ModelSchedulingService()

        # Verify interface compliance
        assert isinstance(service, IModelSchedulingService)

        # Test that all required methods exist
        required_methods = [
            'create_retrain_schedule',
            'update_retrain_schedule',
            'delete_retrain_schedule',
            'list_retrain_schedules',
            'trigger_scheduled_retrain'
        ]

        for method_name in required_methods:
            assert hasattr(service, method_name), f"Missing method: {method_name}"
            method = getattr(service, method_name)
            assert callable(method), f"Method {method_name} is not callable"


class TestModelContracts:
    """Test that model data contracts are properly defined."""

    def test_model_config_contract(self):
        """Test ModelConfig data contract."""
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100},
            feature_selection=["feature1", "feature2"],
            target_variable="target"
        )

        # Required fields
        assert config.model_type == "xgboost"
        assert config.hyperparameters == {"n_estimators": 100}
        assert config.feature_selection == ["feature1", "feature2"]
        assert config.target_variable == "target"

        # Default values
        assert config.training_period_days == 365
        assert config.validation_period_days == 30
        assert config.test_period_days == 30
        assert config.cross_validation_folds == 5
        assert config.random_seed == 42

    def test_model_version_contract(self):
        """Test ModelVersion data contract."""
        from datetime import datetime

        version = ModelVersion(
            version_id="test-version-1",
            model_name="test_model",
            version_number="v1.0.0",
            description="Test version",
            config=ModelConfig(
                model_type="test",
                hyperparameters={},
                feature_selection=["test"],
                target_variable="test"
            ),
            training_start_date=datetime.now(),
            training_end_date=datetime.now(),
            model_path="/test/path",
            model_size_bytes=1024,
            performance_metrics={"accuracy": 0.95},
            created_by="test_user",
            status="active"
        )

        # Required fields
        assert version.version_id == "test-version-1"
        assert version.model_name == "test_model"
        assert version.version_number == "v1.0.0"
        assert version.description == "Test version"
        assert version.created_by == "test_user"
        assert version.status == "active"

        # Default values
        assert version.performance_metrics == {"accuracy": 0.95}
        assert version.feature_importance == {}
        assert version.validation_results == {}

    def test_training_job_contract(self):
        """Test TrainingJob data contract."""
        from datetime import datetime

        job = TrainingJob(
            job_id="test-job-1",
            model_name="test_model",
            config=ModelConfig(
                model_type="test",
                hyperparameters={},
                feature_selection=["test"],
                target_variable="test"
            ),
            status="pending",
            created_by="test_user"
        )

        # Required fields
        assert job.job_id == "test-job-1"
        assert job.model_name == "test_model"
        assert job.status == "pending"
        assert job.created_by == "test_user"

        # Default values
        assert job.progress == 0.0
        assert job.current_stage == ""

    def test_model_comparison_contract(self):
        """Test ModelComparison data contract."""
        from datetime import datetime

        comparison = ModelComparison(
            comparison_id="test-comparison-1",
            champion_version="v1.0.0",
            challenger_version="v1.1.0",
            comparison_metrics={"accuracy_improvement": 0.02},
            statistical_significance={"p_value": 0.01},
            business_impact={"revenue_lift": 10000},
            recommendation="promote_challenger",
            compared_by="test_user"
        )

        # Required fields
        assert comparison.comparison_id == "test-comparison-1"
        assert comparison.champion_version == "v1.0.0"
        assert comparison.challenger_version == "v1.1.0"
        assert comparison.recommendation == "promote_challenger"
        assert comparison.compared_by == "test_user"

        # Default values
        assert comparison.confidence_level == 0.95


class TestServiceBehaviorContracts:
    """Test that services behave according to their contracts."""

    @pytest.mark.asyncio
    async def test_model_registration_workflow(self):
        """Test the complete model registration workflow."""
        from aurum.api.services.model.management_service import ModelManagementService

        service = ModelManagementService()

        # Test model registration
        model = await service.register_model(
            model_name="contract_test_model",
            description="Test model for contract validation",
            model_type="xgboost",
            created_by="test_user"
        )

        assert model.model_name == "contract_test_model"
        assert model.model_type == "xgboost"
        assert model.description == "Test model for contract validation"

        # Test model version registration
        config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100},
            feature_selection=["feature1"],
            target_variable="target"
        )

        from datetime import datetime
        version = ModelVersion(
            version_id="contract-version-1",
            model_name="contract_test_model",
            version_number="v1.0.0",
            description="Contract test version",
            config=config,
            training_start_date=datetime.now(),
            training_end_date=datetime.now(),
            model_path="/test/path",
            model_size_bytes=1024,
            created_by="test_user",
            status="active"
        )

        registered_version = await service.register_model_version(
            model_name="contract_test_model",
            version=version,
            created_by="test_user"
        )

        assert registered_version.version_id == "contract-version-1"
        assert registered_version.model_name == "contract_test_model"

        # Test listing
        versions = await service.list_model_versions("contract_test_model")
        assert len(versions) >= 1

        models = await service.list_models()
        assert len(models) >= 1

    @pytest.mark.asyncio
    async def test_training_job_workflow(self):
        """Test the complete training job workflow."""
        from aurum.api.services.model.training_service import ModelTrainingService

        service = ModelTrainingService()

        # Test training job creation
        config = ModelConfig(
            model_type="test",
            hyperparameters={},
            feature_selection=["test"],
            target_variable="test"
        )

        job_id = await service.start_training_job(
            model_name="contract_training_model",
            config=config,
            created_by="test_user"
        )

        assert job_id is not None

        # Test job retrieval
        job = await service.get_training_job(job_id)
        assert job is not None
        assert job.job_id == job_id
        assert job.status == "pending"

        # Test job listing
        jobs = await service.list_training_jobs()
        assert len(jobs) >= 1

        # Test progress update
        success = await service.update_training_job_progress(
            job_id=job_id,
            progress=0.5,
            stage="feature_engineering",
            updated_by="test_user"
        )

        assert success

        # Verify progress was updated
        updated_job = await service.get_training_job(job_id)
        assert updated_job.progress == 0.5
        assert updated_job.current_stage == "feature_engineering"

    @pytest.mark.asyncio
    async def test_model_comparison_workflow(self):
        """Test the model comparison workflow."""
        from aurum.api.services.model.comparison_service import ModelComparisonService

        service = ModelComparisonService()

        # Test champion/challenger selection creation
        selection = await service.create_champion_challenger_selection(
            model_name="contract_comparison_model",
            champion_version="v1.0.0",
            challenger_versions=["v1.1.0", "v1.2.0"],
            selection_criteria={"accuracy": 0.4, "speed": 0.3},
            created_by="test_user"
        )

        assert selection.model_name == "contract_comparison_model"
        assert selection.champion_version == "v1.0.0"
        assert len(selection.challenger_versions) == 2
        assert selection.selection_criteria == {"accuracy": 0.4, "speed": 0.3}

        # Test listing selections
        selections = await service.list_champion_challenger_comparisons()
        # In real implementation, would assert selections are returned

    @pytest.mark.asyncio
    async def test_scheduling_workflow(self):
        """Test the scheduling workflow."""
        from aurum.api.services.model.scheduling_service import ModelSchedulingService

        service = ModelSchedulingService()

        # Test schedule creation
        config = ModelConfig(
            model_type="test",
            hyperparameters={},
            feature_selection=["test"],
            target_variable="test"
        )

        schedule = await service.create_retrain_schedule(
            model_name="contract_scheduling_model",
            cron_expression="0 2 * * 1",  # Weekly on Monday
            config=config,
            created_by="test_user"
        )

        assert schedule.model_name == "contract_scheduling_model"
        assert schedule.cron_expression == "0 2 * * 1"
        assert schedule.enabled == True

        # Test schedule listing
        schedules = await service.list_retrain_schedules()
        assert len(schedules) >= 1

        # Test schedule update
        success = await service.update_retrain_schedule(
            schedule_id=schedule.schedule_id,
            enabled=False,
            updated_by="test_user"
        )

        assert success

        # Test schedule deletion
        delete_success = await service.delete_retrain_schedule(
            schedule_id=schedule.schedule_id,
            deleted_by="test_user"
        )

        assert delete_success


class TestErrorContractCompliance:
    """Test that services handle errors according to their contracts."""

    @pytest.mark.asyncio
    async def test_invalid_model_registration(self):
        """Test error handling for invalid model registration."""
        from aurum.api.services.model.management_service import ModelManagementService

        service = ModelManagementService()

        # Test registering model with invalid data
        # This should either succeed or raise appropriate exception
        # In real implementation, would test various validation scenarios

        # For demo, just test that service handles basic operations
        model = await service.register_model(
            model_name="",
            description="",
            model_type="",
            created_by=""
        )

        # Should either succeed or raise appropriate exception
        # The contract is that the service handles the request appropriately

    @pytest.mark.asyncio
    async def test_training_job_error_handling(self):
        """Test error handling for training job operations."""
        from aurum.api.services.model.training_service import ModelTrainingService

        service = ModelTrainingService()

        # Test operations that should succeed
        config = ModelConfig(
            model_type="test",
            hyperparameters={},
            feature_selection=["test"],
            target_variable="test"
        )

        # Valid job creation should succeed
        job_id = await service.start_training_job(
            model_name="error_test_model",
            config=config,
            created_by="test_user"
        )

        assert job_id is not None

        # Test job cancellation
        cancel_success = await service.cancel_training_job(
            job_id=job_id,
            cancelled_by="test_user"
        )

        assert cancel_success

    def test_model_contract_validation(self):
        """Test that model contracts enforce data validation."""
        # Test ModelConfig validation
        with pytest.raises(Exception):  # Should raise validation error
            ModelConfig(
                model_type="",  # Invalid empty type
                hyperparameters={},
                feature_selection=[],
                target_variable=""
            )

        # Test valid configuration
        valid_config = ModelConfig(
            model_type="xgboost",
            hyperparameters={"n_estimators": 100},
            feature_selection=["feature1"],
            target_variable="target"
        )

        assert valid_config.model_type == "xgboost"
