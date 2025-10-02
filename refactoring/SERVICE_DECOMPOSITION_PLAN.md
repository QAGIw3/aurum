# Service Layer Decomposition Plan

**Phase:** 2.1 - Service Layer Modernization  
**Status:** Active Implementation  
**Priority:** CRITICAL

## Executive Summary

This plan details the decomposition of monolithic service files into focused, single-responsibility services following SOLID principles. The primary targets are:

1. **model_registry_service.py** (2,392 lines)
2. **developer_workspace_service.py** (2,125 lines)
3. **dbt_management_service.py** (1,761 lines)

## Decomposition Strategy

### Target Architecture
```
src/aurum/services/
├── ml/
│   ├── model_registry.py        # Core model registration
│   ├── training_jobs.py         # Training job management
│   ├── model_comparison.py      # Champion/challenger logic
│   ├── model_selection.py       # Selection algorithms
│   ├── retrain_scheduler.py     # Scheduled retraining
│   └── mlflow_integration.py    # MLflow adapter
├── platform/
│   ├── audit_trail.py           # Audit logging service
│   ├── background_tasks.py      # Task management
│   └── workspace_manager.py     # Developer workspaces
└── data/
    ├── dbt_models.py            # DBT model management
    ├── data_marts.py            # Data mart operations
    └── test_fixtures.py         # Test data management
```

## Detailed Decomposition Plans

### 1. Model Registry Service Decomposition

**Current:** `model_registry_service.py` (2,392 lines)  
**Target:** 6 focused services

#### 1.1 Core Model Registry Service
```python
# src/aurum/services/ml/model_registry.py
class ModelRegistryService(BaseService):
    """Core model registration and versioning."""
    
    Responsibilities:
    - Register models
    - Version management
    - Model metadata updates
    - Model retrieval and listing
    
    Methods:
    - register_model()
    - register_model_version()
    - get_model()
    - list_models()
    - update_model_metadata()
```

#### 1.2 Training Jobs Service
```python
# src/aurum/services/ml/training_jobs.py
class TrainingJobsService(BaseService):
    """Training job lifecycle management."""
    
    Responsibilities:
    - Start training jobs
    - Track progress
    - Complete/cancel jobs
    - Job status monitoring
    
    Methods:
    - start_training_job()
    - update_training_job_progress()
    - complete_training_job()
    - cancel_training_job()
    - get_training_job_status()
```

#### 1.3 Model Comparison Service
```python
# src/aurum/services/ml/model_comparison.py
class ModelComparisonService(BaseService):
    """Champion/challenger comparison logic."""
    
    Responsibilities:
    - Compare model performance
    - Statistical significance testing
    - Business impact assessment
    - Generate recommendations
    
    Methods:
    - compare_models()
    - calculate_significance()
    - assess_business_impact()
    - generate_recommendation()
```

#### 1.4 Model Selection Service
```python
# src/aurum/services/ml/model_selection.py
class ModelSelectionService(BaseService):
    """Champion model selection algorithms."""
    
    Responsibilities:
    - Select champion models
    - Apply selection criteria
    - Promote models
    - Track champion history
    
    Methods:
    - select_champion_model()
    - promote_to_champion()
    - get_current_champion()
    - list_champion_history()
```

#### 1.5 Retrain Scheduler Service
```python
# src/aurum/services/ml/retrain_scheduler.py
class RetrainSchedulerService(BaseService):
    """Scheduled model retraining."""
    
    Responsibilities:
    - Schedule retrain jobs
    - Execute scheduled runs
    - Monitor schedules
    - Handle retrain triggers
    
    Methods:
    - create_retrain_schedule()
    - update_schedule()
    - trigger_retrain()
    - list_schedules()
```

#### 1.6 MLflow Integration Service
```python
# src/aurum/services/ml/mlflow_integration.py
class MLflowIntegrationService(BaseService):
    """MLflow experiment tracking integration."""
    
    Responsibilities:
    - MLflow client management
    - Experiment tracking
    - Model artifact storage
    - Metrics logging
    
    Methods:
    - create_experiment()
    - log_metrics()
    - log_model()
    - get_experiment_runs()
```

### 2. Developer Workspace Service Decomposition

**Current:** `developer_workspace_service.py` (2,125 lines)  
**Target:** 4 focused services

#### 2.1 Workspace Manager Service
```python
# src/aurum/services/platform/workspace_manager.py
class WorkspaceManagerService(BaseService):
    """Developer workspace lifecycle management."""
    
    Responsibilities:
    - Create/delete workspaces
    - Workspace configuration
    - Resource allocation
    - Access control
```

#### 2.2 Dataset Manager Service
```python
# src/aurum/services/platform/dataset_manager.py
class DatasetManagerService(BaseService):
    """Workspace dataset management."""
    
    Responsibilities:
    - Upload/download datasets
    - Dataset versioning
    - Schema management
    - Data validation
```

#### 2.3 Query Execution Service
```python
# src/aurum/services/platform/query_execution.py
class QueryExecutionService(BaseService):
    """SQL query execution in workspaces."""
    
    Responsibilities:
    - Execute queries
    - Result caching
    - Query optimization
    - Resource limits
```

#### 2.4 Notebook Integration Service
```python
# src/aurum/services/platform/notebook_integration.py
class NotebookIntegrationService(BaseService):
    """Jupyter notebook integration."""
    
    Responsibilities:
    - Notebook server management
    - Kernel lifecycle
    - Code execution
    - Output management
```

### 3. DBT Management Service Decomposition

**Current:** `dbt_management_service.py` (1,761 lines)  
**Target:** 3 focused services

#### 3.1 DBT Models Service
```python
# src/aurum/services/data/dbt_models.py
class DBTModelsService(BaseService):
    """DBT model management."""
    
    Responsibilities:
    - Model discovery
    - Model execution
    - Dependency management
    - Model documentation
```

#### 3.2 Data Marts Service
```python
# src/aurum/services/data/data_marts.py
class DataMartsService(BaseService):
    """Data mart operations."""
    
    Responsibilities:
    - Create/update marts
    - Refresh schedules
    - Access control
    - Performance optimization
```

#### 3.3 Test Fixtures Service
```python
# src/aurum/services/data/test_fixtures.py
class TestFixturesService(BaseService):
    """Test data management."""
    
    Responsibilities:
    - Generate fixtures
    - Validate test data
    - Fixture versioning
    - Test execution
```

## Implementation Steps

### Phase 1: Extract Core Services (Week 1)
1. Create new service structure directories
2. Extract ModelRegistryService core functionality
3. Extract TrainingJobsService
4. Update imports and dependencies
5. Add comprehensive tests

### Phase 2: Extract Supporting Services (Week 2)
1. Extract ModelComparisonService
2. Extract ModelSelectionService
3. Extract RetrainSchedulerService
4. Extract MLflowIntegrationService
5. Update integration points

### Phase 3: Extract Platform Services (Week 3)
1. Extract WorkspaceManagerService
2. Extract DatasetManagerService
3. Extract QueryExecutionService
4. Extract NotebookIntegrationService
5. Migrate existing functionality

### Phase 4: Extract Data Services (Week 4)
1. Extract DBTModelsService
2. Extract DataMartsService
3. Extract TestFixturesService
4. Complete migration

## Migration Guidelines

### Service Structure Template
```python
# src/aurum/services/{domain}/{service}.py

from typing import Optional, List, Dict, Any
from src.aurum.services.base import BaseService
from src.aurum.data.repositories.{domain} import {Domain}Repository

class {Service}Service(BaseService):
    """
    {Service description}
    
    This service handles {responsibility}.
    """
    
    def __init__(
        self,
        repository: Optional[{Domain}Repository] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 300
    ):
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_repository()
    
    def _get_repository(self) -> {Domain}Repository:
        # Get from DI container
        pass
    
    async def {method}(self, **kwargs) -> Any:
        """Method documentation."""
        # Implementation
        pass
```

### Testing Template
```python
# tests/unit/services/test_{service}_service.py

import pytest
from unittest.mock import Mock, AsyncMock
from src.aurum.services.{domain}.{service} import {Service}Service

class Test{Service}Service:
    
    @pytest.fixture
    def mock_repository(self):
        return Mock()
    
    @pytest.fixture
    def service(self, mock_repository):
        return {Service}Service(repository=mock_repository)
    
    async def test_{method}(self, service, mock_repository):
        # Test implementation
        pass
```

## Success Criteria

### Code Quality Metrics
- [ ] All services < 500 lines
- [ ] Single responsibility per service
- [ ] 100% type hints
- [ ] >85% test coverage
- [ ] Zero circular dependencies

### Architecture Goals
- [ ] Clear service boundaries
- [ ] Dependency injection throughout
- [ ] Async-first implementation
- [ ] Consistent error handling
- [ ] Comprehensive logging

### Performance Targets
- [ ] Service initialization < 100ms
- [ ] Method response time < 50ms p95
- [ ] Memory footprint reduced by 30%
- [ ] Connection pooling implemented

## Risk Mitigation

### Breaking Changes
- Use adapter pattern for backward compatibility
- Implement feature flags for gradual rollout
- Maintain legacy imports with deprecation warnings

### Testing Strategy
- Write tests before extraction
- Maintain integration test suite
- Performance benchmarks before/after
- End-to-end validation

### Rollback Plan
- Keep original files until migration complete
- Version control for each extraction
- Feature flags for quick disable
- Monitoring for regression detection

---

**Next Steps:**
1. Review and approve plan
2. Create service directories
3. Begin Phase 1 extraction
4. Update progress tracking

**Owner:** Refactoring Team  
**Review:** Weekly progress updates
