"""Unit tests for FeatureStoreService."""

import pytest
from unittest.mock import AsyncMock
from datetime import datetime

from aurum.services.ml import FeatureStoreService, FeatureConfig, FeatureDefinition
from aurum.services.base import ServiceContext, ValidationError, NotFoundError


@pytest.fixture
def feature_store_service():
    """Create feature store service."""
    return FeatureStoreService()


@pytest.fixture
def service_context():
    """Create test service context."""
    return ServiceContext(tenant_id="test-tenant", user_id="test-user")


@pytest.mark.asyncio
async def test_create_feature_definition_success(feature_store_service, service_context):
    """Test successful feature definition creation."""
    # Act
    result = await feature_store_service.create_feature_definition(
        name="test_feature",
        description="Test feature",
        feature_type="numerical",
        data_type="float64",
        source_tables=["test_table"],
        context=service_context
    )

    # Assert
    assert result.success
    assert result.data.name == "test_feature"
    assert result.data.feature_type == "numerical"
    assert "test_feature" in feature_store_service._feature_definitions


@pytest.mark.asyncio
async def test_create_feature_definition_duplicate_name(feature_store_service):
    """Test feature definition creation with duplicate name."""
    # Arrange - create first feature
    await feature_store_service.create_feature_definition(
        name="duplicate_feature",
        description="First feature",
        feature_type="numerical",
        data_type="float64",
        source_tables=["table1"]
    )

    # Act & Assert
    with pytest.raises(ValidationError) as exc:
        await feature_store_service.create_feature_definition(
            name="duplicate_feature",  # Same name
            description="Second feature",
            feature_type="numerical",
            data_type="float64",
            source_tables=["table2"]
        )

    assert "already exists" in str(exc.value)


@pytest.mark.asyncio
async def test_create_feature_definition_invalid_feature_type(feature_store_service):
    """Test feature definition creation with invalid feature type."""
    with pytest.raises(ValidationError) as exc:
        await feature_store_service.create_feature_definition(
            name="test_feature",
            description="Test feature",
            feature_type="invalid_type",
            data_type="float64",
            source_tables=["test_table"]
        )

    assert "Invalid feature type" in str(exc.value)


@pytest.mark.asyncio
async def test_get_feature_definition_success(feature_store_service):
    """Test successful feature definition retrieval."""
    # Arrange
    await feature_store_service.create_feature_definition(
        name="test_feature",
        description="Test feature",
        feature_type="numerical",
        data_type="float64",
        source_tables=["test_table"]
    )

    # Act
    result = await feature_store_service.get_feature_definition("test_feature")

    # Assert
    assert result.success
    assert result.data.name == "test_feature"
    assert result.metadata["feature_name"] == "test_feature"


@pytest.mark.asyncio
async def test_get_feature_definition_not_found(feature_store_service):
    """Test feature definition retrieval for non-existent feature."""
    with pytest.raises(NotFoundError) as exc:
        await feature_store_service.get_feature_definition("nonexistent")

    assert exc.value.resource == "feature"
    assert exc.value.identifier == "nonexistent"


@pytest.mark.asyncio
async def test_list_feature_definitions(feature_store_service):
    """Test listing feature definitions."""
    # Arrange - create some features
    await feature_store_service.create_feature_definition(
        name="feature1",
        description="Feature 1",
        feature_type="numerical",
        data_type="float64",
        source_tables=["table1"]
    )
    await feature_store_service.create_feature_definition(
        name="feature2",
        description="Feature 2",
        feature_type="categorical",
        data_type="string",
        source_tables=["table2"]
    )

    # Act
    result = await feature_store_service.list_feature_definitions(limit=10)

    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["feature_count"] == 2


@pytest.mark.asyncio
async def test_list_feature_definitions_with_filter(feature_store_service):
    """Test listing feature definitions with type filter."""
    # Arrange
    await feature_store_service.create_feature_definition(
        name="numerical_feature",
        description="Numerical feature",
        feature_type="numerical",
        data_type="float64",
        source_tables=["table1"]
    )
    await feature_store_service.create_feature_definition(
        name="categorical_feature",
        description="Categorical feature",
        feature_type="categorical",
        data_type="string",
        source_tables=["table2"]
    )

    # Act
    result = await feature_store_service.list_feature_definitions(
        feature_type="numerical",
        limit=10
    )

    # Assert
    assert result.success
    assert len(result.data) == 1
    assert result.data[0].feature_type == "numerical"
    assert result.metadata["feature_type"] == "numerical"


@pytest.mark.asyncio
async def test_generate_features_success(feature_store_service):
    """Test successful feature generation."""
    # Act
    result = await feature_store_service.generate_features(
        feature_names=["temperature_avg", "load_peak"],  # Use default features
        entity_ids=["entity1", "entity2"],
        asof_date=datetime.now()
    )

    # Assert
    assert result.success
    assert len(result.data) == 2  # Two features
    assert "temperature_avg" in result.data
    assert "load_peak" in result.data
    assert result.metadata["feature_names"] == ["temperature_avg", "load_peak"]


@pytest.mark.asyncio
async def test_generate_features_empty_names(feature_store_service):
    """Test feature generation with empty feature names."""
    with pytest.raises(ValidationError) as exc:
        await feature_store_service.generate_features(
            feature_names=[],
            entity_ids=["entity1"],
            asof_date=datetime.now()
        )

    assert "cannot be empty" in str(exc.value)


@pytest.mark.asyncio
async def test_generate_features_future_date(feature_store_service):
    """Test feature generation with future date."""
    future_date = datetime.now().replace(year=2030)

    with pytest.raises(ValidationError) as exc:
        await feature_store_service.generate_features(
            feature_names=["temperature_avg"],
            entity_ids=["entity1"],
            asof_date=future_date
        )

    assert "cannot be in the future" in str(exc.value)


@pytest.mark.asyncio
async def test_get_feature_lineage_success(feature_store_service):
    """Test successful feature lineage retrieval."""
    # Arrange
    await feature_store_service.create_feature_definition(
        name="dependent_feature",
        description="Feature that depends on others",
        feature_type="derived",
        data_type="float64",
        source_tables=["derived_table"],
        dependencies=["temperature_avg", "load_peak"]
    )

    # Act
    result = await feature_store_service.get_feature_lineage("dependent_feature")

    # Assert
    assert result.success
    assert result.data["feature"] == "dependent_feature"
    assert "temperature_avg" in result.data["dependencies"]
    assert "load_peak" in result.data["dependencies"]


@pytest.mark.asyncio
async def test_get_feature_lineage_not_found(feature_store_service):
    """Test feature lineage for non-existent feature."""
    with pytest.raises(NotFoundError) as exc:
        await feature_store_service.get_feature_lineage("nonexistent")

    assert exc.value.resource == "feature"


@pytest.mark.asyncio
async def test_feature_store_initialization():
    """Test feature store service initialization."""
    service = FeatureStoreService()

    # Should have default feature definitions
    assert len(service._feature_definitions) > 0
    assert "temperature_avg" in service._feature_definitions
    assert "load_peak" in service._feature_definitions
    assert "price_volatility" in service._feature_definitions

