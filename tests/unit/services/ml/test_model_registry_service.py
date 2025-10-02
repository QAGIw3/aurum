"""Unit tests for ModelRegistryService."""

import pytest
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from src.aurum.services.ml.model_registry import (
    ModelRegistryService,
    RegisteredModel,
    ModelVersion,
    ModelRegistryRepository
)


class TestModelRegistryService:
    """Test suite for ModelRegistryService."""
    
    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        repo = Mock(spec=ModelRegistryRepository)
        repo.save_model = AsyncMock()
        repo.get_model = AsyncMock()
        repo.list_models = AsyncMock()
        repo.save_version = AsyncMock()
        repo.get_version = AsyncMock()
        repo.list_versions = AsyncMock()
        return repo
    
    @pytest.fixture
    def service(self, mock_repository):
        """Create service instance with mock repository."""
        return ModelRegistryService(
            repository=mock_repository,
            cache_enabled=False  # Disable cache for unit tests
        )
    
    @pytest.mark.asyncio
    async def test_register_model_new(self, service, mock_repository):
        """Test registering a new model."""
        # Setup
        model_name = "test_model"
        model_type = "xgboost"
        description = "Test model"
        tags = {"env": "test"}
        owners = ["user1", "user2"]
        
        mock_repository.get_model.return_value = None  # Model doesn't exist
        mock_repository.save_model.side_effect = lambda m: m  # Return same model
        
        # Execute
        result = await service.register_model(
            model_name=model_name,
            model_type=model_type,
            description=description,
            tags=tags,
            owners=owners
        )
        
        # Assert
        assert result.model_name == model_name
        assert result.model_type == model_type
        assert result.description == description
        assert result.tags == tags
        assert result.owners == set(owners)
        assert result.status == "active"
        
        mock_repository.get_model.assert_called_once_with(model_name)
        mock_repository.save_model.assert_called_once()
        
        # Verify the model was saved correctly
        saved_model = mock_repository.save_model.call_args[0][0]
        assert isinstance(saved_model, RegisteredModel)
        assert saved_model.model_name == model_name
    
    @pytest.mark.asyncio
    async def test_register_model_existing(self, service, mock_repository):
        """Test updating an existing model."""
        # Setup
        model_name = "existing_model"
        existing_model = RegisteredModel(
            model_name=model_name,
            model_type="tensorflow",
            description="Old description",
            tags={"env": "prod"},
            owners={"user1"}
        )
        
        mock_repository.get_model.return_value = existing_model
        mock_repository.save_model.side_effect = lambda m: m
        
        # Execute
        result = await service.register_model(
            model_name=model_name,
            model_type="tensorflow",  # Same type
            description="New description",
            tags={"version": "2.0"},
            owners=["user2"]
        )
        
        # Assert
        assert result.description == "New description"
        assert result.tags == {"env": "prod", "version": "2.0"}  # Merged
        assert result.owners == {"user1", "user2"}  # Combined
        
        mock_repository.get_model.assert_called_once_with(model_name)
        mock_repository.save_model.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_register_model_version(self, service, mock_repository):
        """Test registering a model version."""
        # Setup
        model_name = "test_model"
        model = RegisteredModel(
            model_name=model_name,
            model_type="xgboost",
            versions=[]
        )
        
        mock_repository.get_model.return_value = model
        mock_repository.save_model.side_effect = lambda m: m
        mock_repository.save_version.side_effect = lambda v: v
        
        # Execute
        result = await service.register_model_version(
            model_name=model_name,
            version_number="v1.0",
            description="Initial version",
            model_path="s3://models/test_model/v1.0",
            config={"learning_rate": 0.1},
            performance_metrics={"accuracy": 0.95},
            created_by="ml_engineer"
        )
        
        # Assert
        assert result.model_name == model_name
        assert result.version_number == "v1.0"
        assert result.description == "Initial version"
        assert result.model_path == "s3://models/test_model/v1.0"
        assert result.config == {"learning_rate": 0.1}
        assert result.performance_metrics == {"accuracy": 0.95}
        assert result.created_by == "ml_engineer"
        
        mock_repository.save_version.assert_called_once()
        mock_repository.save_model.assert_called_once()
        
        # Verify model was updated with version
        saved_model = mock_repository.save_model.call_args[0][0]
        assert len(saved_model.versions) == 1
        assert saved_model.latest_version == result.version_id
    
    @pytest.mark.asyncio
    async def test_register_model_version_no_model(self, service, mock_repository):
        """Test registering version for non-existent model."""
        # Setup
        mock_repository.get_model.return_value = None
        
        # Execute & Assert
        with pytest.raises(ValueError, match="Model test_model not found"):
            await service.register_model_version(
                model_name="test_model",
                version_number="v1.0"
            )
    
    @pytest.mark.asyncio
    async def test_get_model(self, service, mock_repository):
        """Test getting a model."""
        # Setup
        model_name = "test_model"
        model = RegisteredModel(
            model_name=model_name,
            model_type="xgboost"
        )
        mock_repository.get_model.return_value = model
        
        # Execute
        result = await service.get_model(model_name)
        
        # Assert
        assert result == model
        mock_repository.get_model.assert_called_once_with(model_name)
    
    @pytest.mark.asyncio
    async def test_get_model_from_cache(self, service, mock_repository):
        """Test getting model from in-memory cache."""
        # Setup
        model_name = "cached_model"
        model = RegisteredModel(
            model_name=model_name,
            model_type="tensorflow"
        )
        service._models_by_name[model_name] = model
        
        # Execute
        result = await service.get_model(model_name)
        
        # Assert
        assert result == model
        mock_repository.get_model.assert_not_called()  # Should use cache
    
    @pytest.mark.asyncio
    async def test_get_model_version(self, service, mock_repository):
        """Test getting a model version."""
        # Setup
        version_id = str(uuid4())
        version = ModelVersion(
            version_id=version_id,
            model_name="test_model",
            version_number="v1.0",
            created_by="user"
        )
        mock_repository.get_version.return_value = version
        
        # Execute
        result = await service.get_model_version(version_id)
        
        # Assert
        assert result == version
        mock_repository.get_version.assert_called_once_with(version_id)
    
    @pytest.mark.asyncio
    async def test_get_latest_model_version(self, service, mock_repository):
        """Test getting the latest version of a model."""
        # Setup
        model_name = "test_model"
        version_id = str(uuid4())
        model = RegisteredModel(
            model_name=model_name,
            model_type="xgboost",
            latest_version=version_id
        )
        version = ModelVersion(
            version_id=version_id,
            model_name=model_name,
            version_number="v2.0",
            created_by="user"
        )
        
        mock_repository.get_model.return_value = model
        mock_repository.get_version.return_value = version
        
        # Execute
        result = await service.get_latest_model_version(model_name)
        
        # Assert
        assert result == version
        mock_repository.get_model.assert_called_once_with(model_name)
        mock_repository.get_version.assert_called_once_with(version_id)
    
    @pytest.mark.asyncio
    async def test_get_latest_model_version_no_versions(self, service, mock_repository):
        """Test getting latest version when model has no versions."""
        # Setup
        model = RegisteredModel(
            model_name="test_model",
            model_type="xgboost",
            latest_version=None
        )
        mock_repository.get_model.return_value = model
        
        # Execute
        result = await service.get_latest_model_version("test_model")
        
        # Assert
        assert result is None
        mock_repository.get_version.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_list_models(self, service, mock_repository):
        """Test listing models."""
        # Setup
        models = [
            RegisteredModel(model_name="model1", model_type="xgboost", owners={"user1"}),
            RegisteredModel(model_name="model2", model_type="tensorflow", owners={"user1", "user2"})
        ]
        mock_repository.list_models.return_value = models
        
        # Execute
        result = await service.list_models(limit=10, offset=0)
        
        # Assert
        assert result == models
        mock_repository.list_models.assert_called_once_with(
            status=None,
            tags=None,
            limit=10,
            offset=0
        )
    
    @pytest.mark.asyncio
    async def test_list_models_with_owner_filter(self, service, mock_repository):
        """Test listing models with owner filter."""
        # Setup
        models = [
            RegisteredModel(model_name="model1", model_type="xgboost", owners={"user1"}),
            RegisteredModel(model_name="model2", model_type="tensorflow", owners={"user1", "user2"}),
            RegisteredModel(model_name="model3", model_type="pytorch", owners={"user2"})
        ]
        mock_repository.list_models.return_value = models
        
        # Execute
        result = await service.list_models(owner="user1")
        
        # Assert
        assert len(result) == 2
        assert all("user1" in m.owners for m in result)
    
    @pytest.mark.asyncio
    async def test_list_model_versions(self, service, mock_repository):
        """Test listing model versions."""
        # Setup
        versions = [
            ModelVersion(model_name="test_model", version_number="v1.0", created_by="user"),
            ModelVersion(model_name="test_model", version_number="v2.0", created_by="user")
        ]
        mock_repository.list_versions.return_value = versions
        
        # Execute
        result = await service.list_model_versions("test_model", limit=50)
        
        # Assert
        assert result == versions
        mock_repository.list_versions.assert_called_once_with(
            model_name="test_model",
            status=None,
            limit=50,
            offset=0
        )
    
    @pytest.mark.asyncio
    async def test_update_model_metadata(self, service, mock_repository):
        """Test updating model metadata."""
        # Setup
        model_name = "test_model"
        model = RegisteredModel(
            model_name=model_name,
            model_type="xgboost",
            description="Old description",
            status="active",
            tags={"env": "dev"},
            owners={"user1"}
        )
        
        mock_repository.get_model.return_value = model
        mock_repository.save_model.side_effect = lambda m: m
        
        # Execute
        result = await service.update_model_metadata(
            model_name=model_name,
            description="New description",
            status="deprecated",
            tags={"version": "2.0"},
            owners=["user2"],
            metadata={"notes": "Updated"}
        )
        
        # Assert
        assert result.description == "New description"
        assert result.status == "deprecated"
        assert result.tags == {"env": "dev", "version": "2.0"}
        assert result.owners == {"user1", "user2"}
        assert result.metadata == {"notes": "Updated"}
        
        mock_repository.save_model.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_model_metadata_not_found(self, service, mock_repository):
        """Test updating metadata for non-existent model."""
        # Setup
        mock_repository.get_model.return_value = None
        
        # Execute & Assert
        with pytest.raises(ValueError, match="Model test_model not found"):
            await service.update_model_metadata(
                model_name="test_model",
                description="New description"
            )
    
    @pytest.mark.asyncio
    async def test_retire_model(self, service, mock_repository):
        """Test retiring a model."""
        # Setup
        model_name = "test_model"
        model = RegisteredModel(
            model_name=model_name,
            model_type="xgboost",
            status="active"
        )
        
        mock_repository.get_model.return_value = model
        mock_repository.save_model.side_effect = lambda m: m
        
        # Execute
        result = await service.retire_model(model_name, reason="Obsolete")
        
        # Assert
        assert result.status == "retired"
        assert result.metadata["retirement_reason"] == "Obsolete"
        assert "retired_at" in result.metadata
        
        mock_repository.save_model.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_emit_metric(self, service):
        """Test metric emission (placeholder test)."""
        # This is a placeholder - in real implementation would test telemetry
        await service._emit_metric("test_metric", value=1.0, tags={"test": "true"})
        # No assertion - just ensure no errors
    
    @pytest.mark.asyncio
    async def test_cache_operations(self, service):
        """Test cache operations (placeholders)."""
        # Test cache get
        result = await service._get_from_cache("test_key")
        assert result is None  # Placeholder returns None
        
        # Test cache set
        await service._set_cache("test_key", {"data": "value"}, ttl=300)
        # No assertion - just ensure no errors
