"""Scenario service for modeling and what-if analysis with caching.

Implements business logic for scenario creation, execution, and results retrieval.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from uuid import UUID
from datetime import datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import ScenarioRepository

logger = logging.getLogger(__name__)


class CacheProtocol(Protocol):
    """Protocol for cache implementations."""
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache with TTL."""
        ...
    
    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        ...


class ScenarioService(BaseService):
    """Service for scenario operations with caching support.
    
    Scenarios represent what-if analyses and modeling runs.
    Metadata stored in Postgres, outputs in Iceberg.
    
    This service:
    - Validates scenario configurations
    - Manages scenario lifecycle
    - Orchestrates scenario execution
    - Retrieves and aggregates results
    - Enforces access control
    - Caches scenario metadata for performance
    """
    
    def __init__(
        self,
        scenario_repository: ScenarioRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 300  # 5 minutes for scenarios
    ):
        """Initialize service with dependencies.
        
        Args:
            scenario_repository: Repository for scenario data access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__()
        self.scenario_repo = scenario_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "scenarios:v1"
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters."""
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:16]
        return f"{self._cache_namespace}:{operation}:{param_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get value from cache if available."""
        if not self.cache:
            return None
        
        try:
            cached = await self.cache.get(cache_key)
            if cached:
                self.logger.debug(f"Cache hit: {cache_key}")
                return cached
            return None
        except Exception as e:
            self.logger.warning(f"Cache get error: {e}")
            return None
    
    async def _set_in_cache(self, cache_key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        if not self.cache:
            return
        
        try:
            ttl = ttl or self.cache_ttl
            await self.cache.set(cache_key, value, ttl)
            self.logger.debug(f"Cache set: {cache_key}")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
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
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get scenario by ID with optional caching.
        
        Business logic:
        - Validates UUID format
        - Checks tenant access
        - Returns scenario with metadata
        - Caches results for performance
        
        Args:
            scenario_id: Scenario UUID
            use_cache: Whether to use caching
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
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("scenario", scenario_id=scenario_id)
                cached_scenario = await self._get_from_cache(cache_key)
                if cached_scenario is not None:
                    # Still need to check tenant access
                    if context and context.tenant_id:
                        if cached_scenario.get("tenant_id") != context.tenant_id:
                            raise NotFoundError("scenario", scenario_id)
                    return ServiceResult.ok(
                        data=cached_scenario,
                        metadata={"scenario_id": scenario_id, "source": "cache"}
                    )
            
            # Get scenario
            scenario = await self.scenario_repo.find_by_id(uuid_obj)
            
            if not scenario:
                raise NotFoundError("scenario", scenario_id)
            
            # Check tenant access
            if context and context.tenant_id:
                if scenario.get("tenant_id") != context.tenant_id:
                    raise NotFoundError("scenario", scenario_id)  # Don't leak existence
            
            # Cache result
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, scenario)
            
            return ServiceResult.ok(
                data=scenario,
                metadata={"scenario_id": scenario_id, "source": "database"}
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

