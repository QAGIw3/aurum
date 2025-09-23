"""Tests for the refactored maintenance system."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from aurum.maintenance import (
    MaintenanceService,
    MaintenanceFactory,
    MaintenanceExecutorImpl,
    OperationType,
    MaintenanceStatus,
    MaintenanceConfig,
)


class TestMaintenanceFactory:
    """Test the maintenance factory."""
    
    def test_create_backend_iceberg(self):
        """Test creating an Iceberg backend."""
        backend = MaintenanceFactory.create_backend(
            "iceberg", 
            {"host": "localhost", "port": 8080}
        )
        assert backend.backend_type == "iceberg"
        assert OperationType.COMPACTION in backend.supported_operations
    
    def test_create_operation_compaction(self):
        """Test creating a compaction operation."""
        operation = MaintenanceFactory.create_operation(OperationType.COMPACTION)
        assert operation.operation_type == OperationType.COMPACTION
        assert operation.supports_dry_run is True
    
    def test_create_unknown_backend_raises(self):
        """Test that unknown backend types raise errors."""
        with pytest.raises(ValueError, match="Unknown backend type"):
            MaintenanceFactory.create_backend("unknown", {})
    
    def test_create_unknown_operation_raises(self):
        """Test that unknown operation types raise errors.""" 
        with pytest.raises(ValueError, match="Unknown operation type"):
            # This will fail since we need a valid OperationType
            MaintenanceFactory.create_operation("unknown")
    
    def test_get_supported_backends(self):
        """Test getting supported backend types."""
        backends = MaintenanceFactory.get_supported_backends()
        assert "iceberg" in backends
        assert "timescale" in backends
        assert "clickhouse" in backends
    
    def test_get_supported_operations(self):
        """Test getting supported operation types."""
        operations = MaintenanceFactory.get_supported_operations()
        assert OperationType.COMPACTION in operations
        assert OperationType.RETENTION in operations


@pytest.mark.asyncio
class TestMaintenanceExecutor:
    """Test the maintenance executor."""
    
    async def test_execute_operation_success(self):
        """Test successful operation execution."""
        executor = MaintenanceExecutorImpl(max_workers=1)
        
        # Mock operation and backend
        operation = Mock()
        operation.execute = AsyncMock(return_value=Mock(
            operation_id="test-123",
            status=MaintenanceStatus.COMPLETED,
            metrics={"files_processed": 10}
        ))
        
        backend = AsyncMock()
        backend.connect = AsyncMock()
        
        config = MaintenanceConfig(
            operation_type=OperationType.COMPACTION,
            target_table="test_table"
        )
        
        result = await executor.execute_operation(operation, backend, config)
        
        # Verify execution
        backend.connect.assert_called_once()
        operation.execute.assert_called_once_with(backend, config)
        assert result.status == MaintenanceStatus.COMPLETED
    
    async def test_execute_batch_operations(self):
        """Test batch operation execution."""
        executor = MaintenanceExecutorImpl(max_workers=2)
        
        # Create multiple mock operations
        operations = []
        for i in range(3):
            operation = Mock()
            operation.execute = AsyncMock(return_value=Mock(
                operation_id=f"test-{i}",
                status=MaintenanceStatus.COMPLETED
            ))
            
            backend = AsyncMock()
            backend.connect = AsyncMock()
            
            config = MaintenanceConfig(
                operation_type=OperationType.COMPACTION,
                target_table=f"test_table_{i}"
            )
            
            operations.append((operation, backend, config))
        
        results = await executor.execute_batch(operations)
        
        assert len(results) == 3
        for result in results:
            assert result.status == MaintenanceStatus.COMPLETED


@pytest.mark.asyncio  
class TestMaintenanceService:
    """Test the high-level maintenance service."""
    
    @patch('aurum.maintenance.service.AurumSettings')
    async def test_execute_maintenance_operation(self, mock_settings):
        """Test executing a single maintenance operation."""
        # Mock settings
        mock_settings.from_env.return_value = Mock()
        mock_settings.from_env.return_value.data_backend = Mock(
            trino_host="localhost",
            trino_port=8080,
            trino_user="test",
            iceberg_catalog="iceberg"
        )
        
        service = MaintenanceService()
        
        # Mock the backend creation and execution
        with patch.object(service._factory, 'create_backend') as mock_create_backend, \
             patch.object(service._factory, 'create_operation') as mock_create_operation, \
             patch.object(service._executor, 'execute_operation') as mock_execute:
            
            # Setup mocks
            mock_backend = AsyncMock()
            mock_backend.connect = AsyncMock()
            mock_create_backend.return_value = mock_backend
            
            mock_operation = Mock()
            mock_create_operation.return_value = mock_operation
            
            mock_result = Mock(
                operation_id="test-123",
                status=MaintenanceStatus.COMPLETED
            )
            mock_execute.return_value = mock_result
            
            # Execute operation
            result = await service.execute_maintenance(
                backend_type="iceberg",
                operation_type=OperationType.COMPACTION,
                target_table="test_table",
                dry_run=True,
                parameters={"target_file_size_mb": 128}
            )
            
            # Verify calls
            mock_create_backend.assert_called_once()
            mock_create_operation.assert_called_once_with(OperationType.COMPACTION)
            mock_execute.assert_called_once()
            
            assert result.status == MaintenanceStatus.COMPLETED
    
    @patch('aurum.maintenance.service.AurumSettings')
    async def test_execute_batch_maintenance(self, mock_settings):
        """Test executing batch maintenance operations."""
        # Mock settings
        mock_settings.from_env.return_value = Mock()
        mock_settings.from_env.return_value.data_backend = Mock(
            trino_host="localhost",
            trino_port=8080,
            trino_user="test",
            iceberg_catalog="iceberg"
        )
        
        service = MaintenanceService()
        
        # Mock the factory and executor
        with patch.object(service._factory, 'create_backend') as mock_create_backend, \
             patch.object(service._factory, 'create_operation') as mock_create_operation, \
             patch.object(service._executor, 'execute_batch') as mock_execute_batch:
            
            # Setup mocks
            mock_backend = AsyncMock()
            mock_backend.connect = AsyncMock()
            mock_create_backend.return_value = mock_backend
            
            mock_operation = Mock()
            mock_create_operation.return_value = mock_operation
            
            mock_results = [
                Mock(operation_id="test-1", status=MaintenanceStatus.COMPLETED),
                Mock(operation_id="test-2", status=MaintenanceStatus.COMPLETED),
            ]
            mock_execute_batch.return_value = mock_results
            
            # Execute batch
            operations = [
                {
                    "backend_type": "iceberg",
                    "operation_type": "compaction",
                    "target_table": "table1",
                    "dry_run": True
                },
                {
                    "backend_type": "iceberg", 
                    "operation_type": "retention",
                    "target_table": "table2",
                    "parameters": {"retention_days": 30}
                }
            ]
            
            results = await service.execute_batch_maintenance(operations)
            
            # Verify results
            assert len(results) == 2
            for result in results:
                assert result.status == MaintenanceStatus.COMPLETED
    
    async def test_cleanup_resources(self):
        """Test resource cleanup."""
        service = MaintenanceService()
        
        # Mock a backend
        mock_backend = AsyncMock()
        service._backends["iceberg"] = mock_backend
        
        # Mock executor cleanup
        service._executor.cleanup = Mock()
        
        await service.cleanup()
        
        # Verify cleanup calls
        mock_backend.disconnect.assert_called_once()
        service._executor.cleanup.assert_called_once()