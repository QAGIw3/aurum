"""Scenario service for modeling and what-if analysis.

Implements business logic for scenario creation, execution, and results retrieval.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
from datetime import datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import ScenarioRepository

logger = logging.getLogger(__name__)


class ScenarioService(BaseService):
    """Service for scenario operations.
    
    Scenarios represent what-if analyses and modeling runs.
    Metadata stored in Postgres, outputs in Iceberg.
    
    This service:
    - Validates scenario configurations
    - Manages scenario lifecycle
    - Orchestrates scenario execution
    - Retrieves and aggregates results
    - Enforces access control
    """
    
    def __init__(self, scenario_repository: ScenarioRepository):
        """Initialize service with dependencies.
        
        Args:
            scenario_repository: Repository for scenario data access
        """
        super().__init__()
        self.scenario_repo = scenario_repository
    
    async def create_scenario(
        self,
        name: str,
        description: Optional[str] = None,
        assumptions: Optional[Dict[str, Any]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a new scenario.
        
        Business logic:
        - Validates scenario name
        - Validates assumptions schema
        - Sets default values
        - Associates with tenant
        - Initializes status
        
        Args:
            name: Scenario name (required, unique per tenant)
            description: Scenario description
            assumptions: Scenario assumptions/parameters
            context: Service context with tenant info
            
        Returns:
            ServiceResult with created scenario
            
        Raises:
            ValidationError: If validation fails
            ServiceError: If creation fails
        """
        self._log_operation(
            "create_scenario",
            context=context,
            name=name
        )
        
        try:
            # Validate inputs
            self._validate_scenario_name(name)
            if assumptions:
                self._validate_assumptions(assumptions)
            
            # Extract tenant from context
            tenant_id = context.tenant_id if context else None
            
            # Create scenario
            scenario = await self.scenario_repo.create_scenario(
                name=name,
                description=description,
                assumptions=assumptions or {},
                tenant_id=tenant_id
            )
            
            self.logger.info(
                f"Created scenario: {scenario.get('id')}",
                extra={
                    "scenario_id": scenario.get("id"),
                    "name": name,
                    "tenant_id": tenant_id
                }
            )
            
            return ServiceResult.ok(
                data=scenario,
                metadata={
                    "scenario_id": scenario.get("id"),
                    "created": True
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "create_scenario", context)
    
    async def get_scenario(
        self,
        scenario_id: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get scenario by ID.
        
        Business logic:
        - Validates UUID format
        - Checks tenant access
        - Returns scenario with metadata
        
        Args:
            scenario_id: Scenario UUID
            context: Service context
            
        Returns:
            ServiceResult with scenario
            
        Raises:
            ValidationError: If UUID invalid
            NotFoundError: If scenario not found
            ServiceError: If retrieval fails
        """
        self._log_operation("get_scenario", context=context, scenario_id=scenario_id)
        
        try:
            # Validate UUID
            try:
                uuid_obj = UUID(scenario_id)
            except ValueError:
                raise ValidationError(
                    "Invalid scenario ID format",
                    field="scenario_id"
                )
            
            # Get scenario
            scenario = await self.scenario_repo.find_by_id(uuid_obj)
            
            if not scenario:
                raise NotFoundError("scenario", scenario_id)
            
            # Check tenant access
            if context and context.tenant_id:
                if scenario.get("tenant_id") != context.tenant_id:
                    raise NotFoundError("scenario", scenario_id)  # Don't leak existence
            
            return ServiceResult.ok(
                data=scenario,
                metadata={"scenario_id": scenario_id}
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_scenario", context)
    
    async def list_scenarios(
        self,
        limit: int = 100,
        offset: int = 0,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """List scenarios with pagination.
        
        Business logic:
        - Filters by tenant
        - Applies pagination
        - Orders by creation date
        
        Args:
            limit: Maximum results (max 1000)
            offset: Pagination offset
            context: Service context
            
        Returns:
            ServiceResult with list of scenarios
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If listing fails
        """
        self._log_operation("list_scenarios", context=context, limit=limit, offset=offset)
        
        try:
            # Validate parameters
            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )
            
            if offset < 0:
                raise ValidationError(
                    "Offset cannot be negative",
                    field="offset"
                )
            
            # Get tenant ID from context
            tenant_id = context.tenant_id if context else None
            
            # List scenarios
            scenarios = await self.scenario_repo.list_scenarios(
                tenant_id=tenant_id,
                limit=limit,
                offset=offset
            )
            
            return ServiceResult.ok(
                data=scenarios,
                metadata={
                    "count": len(scenarios),
                    "limit": limit,
                    "offset": offset,
                    "has_more": len(scenarios) == limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "list_scenarios", context)
    
    async def get_scenario_outputs(
        self,
        scenario_id: str,
        limit: int = 1000,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get scenario outputs/results.
        
        Business logic:
        - Validates scenario exists
        - Checks tenant access
        - Retrieves outputs from Iceberg
        - Applies result limits
        
        Args:
            scenario_id: Scenario UUID
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with scenario outputs
            
        Raises:
            ValidationError: If UUID invalid
            NotFoundError: If scenario not found
            ServiceError: If retrieval fails
        """
        self._log_operation(
            "get_scenario_outputs",
            context=context,
            scenario_id=scenario_id
        )
        
        try:
            # Validate and get scenario (checks access)
            scenario_result = await self.get_scenario(scenario_id, context)
            if not scenario_result.success:
                raise ServiceError("Failed to get scenario")
            
            # Validate UUID
            try:
                uuid_obj = UUID(scenario_id)
            except ValueError:
                raise ValidationError(
                    "Invalid scenario ID format",
                    field="scenario_id"
                )
            
            # Get outputs
            outputs = await self.scenario_repo.get_scenario_outputs(
                scenario_id=uuid_obj,
                limit=min(limit, 10000)  # Hard cap
            )
            
            return ServiceResult.ok(
                data=outputs,
                metadata={
                    "scenario_id": scenario_id,
                    "output_count": len(outputs),
                    "has_more": len(outputs) == limit
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_scenario_outputs", context)
    
    # Private helper methods
    
    def _validate_scenario_name(self, name: str) -> None:
        """Validate scenario name."""
        if not name or not name.strip():
            raise ValidationError("Scenario name is required", field="name")
        
        if len(name) > 255:
            raise ValidationError(
                "Scenario name must be 255 characters or less",
                field="name"
            )
        
        # Check for invalid characters
        invalid_chars = ["<", ">", "&", "\"", "'"]
        if any(char in name for char in invalid_chars):
            raise ValidationError(
                "Scenario name contains invalid characters",
                field="name"
            )
    
    def _validate_assumptions(self, assumptions: Dict[str, Any]) -> None:
        """Validate scenario assumptions schema."""
        if not isinstance(assumptions, dict):
            raise ValidationError(
                "Assumptions must be a dictionary",
                field="assumptions"
            )
        
        # Add schema validation here as needed
        # For example, check required fields, data types, ranges, etc.
        pass

