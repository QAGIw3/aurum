"""Tests for maintenance operations."""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timezone

from aurum.maintenance.interfaces import (
    MaintenanceConfig,
    MaintenanceStatus,
    OperationType,
)
from aurum.maintenance.operations import (
    CompactionOperation,
    RetentionOperation,
)


@pytest.mark.asyncio
class TestCompactionOperation:
    """Test compaction operation."""
    
    async def test_operation_properties(self):
        """Test basic operation properties."""
        operation = CompactionOperation()
        
        assert operation.operation_type == OperationType.COMPACTION
        assert operation.supports_dry_run is True
    
    async def test_config_validation_success(self):
        """Test successful configuration validation."""
        operation = CompactionOperation()
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="test_table",
            parameters={"target_file_size_mb": 128}
        )
        
        errors = await operation.validate_config(config)
        assert len(errors) == 0
    
    async def test_config_validation_invalid_target_size(self):
        """Test validation with invalid target file size."""
        operation = CompactionOperation()
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="test_table",
            parameters={"target_file_size_mb": -10}
        )
        
        errors = await operation.validate_config(config)
        assert len(errors) > 0
        assert any("target_file_size_mb must be a positive number" in error for error in errors)
    
    async def test_config_validation_missing_table(self):
        """Test validation with missing target table."""
        operation = CompactionOperation()
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="",  # Empty table name
        )
        
        errors = await operation.validate_config(config)
        assert len(errors) > 0
        assert any("target_table is required" in error for error in errors)
    
    async def test_execute_success(self):
        """Test successful operation execution."""
        operation = CompactionOperation()
        
        # Mock backend
        mock_backend = AsyncMock()
        mock_backend.backend_type = "iceberg"
        mock_backend.health_check = AsyncMock(return_value=True)
        mock_backend.execute_iceberg_operation = AsyncMock(return_value={
            "rewritten_files": 5,
            "added_files": 3,
            "deleted_files": 5,
            "rewritten_bytes": 1024000,
        })
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="test_table",
            dry_run=False,
            parameters={"target_file_size_mb": 128}
        )
        
        result = await operation.execute(mock_backend, config)
        
        # Verify result
        assert result.status == MaintenanceStatus.COMPLETED
        assert result.config == config
        assert "files_rewritten" in result.metrics
        assert result.metrics["files_rewritten"] == 5
        assert result.error_message is None
    
    async def test_execute_backend_unhealthy(self):
        """Test execution with unhealthy backend."""
        operation = CompactionOperation()
        
        # Mock unhealthy backend
        mock_backend = AsyncMock()
        mock_backend.backend_type = "iceberg"
        mock_backend.health_check = AsyncMock(return_value=False)
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="test_table"
        )
        
        result = await operation.execute(mock_backend, config)
        
        # Verify failure
        assert result.status == MaintenanceStatus.FAILED
        assert "Backend iceberg is not healthy" in result.error_message


@pytest.mark.asyncio
class TestRetentionOperation:
    """Test retention operation."""
    
    async def test_operation_properties(self):
        """Test basic operation properties."""
        operation = RetentionOperation()
        
        assert operation.operation_type == OperationType.RETENTION
        assert operation.supports_dry_run is True
    
    async def test_execute_placeholder(self):
        """Test placeholder execution."""
        operation = RetentionOperation()
        
        # Mock backend
        mock_backend = AsyncMock()
        mock_backend.backend_type = "test"
        mock_backend.health_check = AsyncMock(return_value=True)
        
        config = MaintenanceConfig(
            operation_type=OperationType.RETENTION,
            target_table="test_table",
            dry_run=True
        )
        
        result = await operation.execute(mock_backend, config)
        
        # Verify placeholder result
        assert result.status == MaintenanceStatus.COMPLETED
        assert result.metrics["operation"] == "retention"
        assert result.metrics["table"] == "test_table"
        assert result.metrics["dry_run"] is True