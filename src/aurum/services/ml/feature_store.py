"""Feature store service for ML feature engineering and management with caching.

Implements business logic for feature store operations including feature
creation, versioning, and serving for ML models.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import datetime, timedelta
from dataclasses import dataclass

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError

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


@dataclass
class FeatureConfig:
    """Configuration for feature generation."""
    lookback_days: int = 365
    temporal_resolution: str = "hourly"
    feature_version: str = "v1"
    enable_caching: bool = True
    cache_ttl_minutes: int = 60
    geography: str = "US"


@dataclass
class FeatureDefinition:
    """Definition of a feature in the feature store."""
    name: str
    description: str
    feature_type: str  # 'numerical', 'categorical', 'temporal', 'derived'
    data_type: str  # 'float64', 'int64', 'string', 'datetime64'
    source_tables: List[str]
    transformation: Optional[str] = None
    dependencies: List[str] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


class FeatureStoreService(BaseService):
    """Service for ML feature store operations with caching support.

    Feature store provides:
    - Cross-asset feature engineering (weather, load, price data)
    - Time-window aggregations and lag features
    - Feature versioning and lineage tracking
    - Training/serving parity for ML models
    - Integration with scenario modeling and forecasting

    This service:
    - Manages feature definitions and versions
    - Generates features from raw data
    - Provides feature serving for ML models
    - Handles feature lineage and dependencies
    - Implements caching for performance
    """

    def __init__(self, cache: Optional[CacheProtocol] = None, cache_ttl: int = 3600):
        """Initialize service with optional cache.
        
        Args:
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds (default 1 hour)
        """
        super().__init__()
        self._feature_definitions: Dict[str, FeatureDefinition] = {}
        self._config = FeatureConfig()
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "features:v1"
        self._initialize_feature_definitions()
    
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

    async def create_feature_definition(
        self,
        name: str,
        description: str,
        feature_type: str,
        data_type: str,
        source_tables: List[str],
        transformation: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[FeatureDefinition]:
        """Create a new feature definition.

        Args:
            name: Feature name (unique identifier)
            description: Human-readable description
            feature_type: Type of feature (numerical, categorical, etc.)
            data_type: Data type of the feature
            source_tables: Source tables for feature computation
            transformation: Optional transformation logic
            dependencies: Feature dependencies
            context: Service context

        Returns:
            ServiceResult with created feature definition

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If creation fails
        """
        self._log_operation(
            "create_feature_definition",
            context=context,
            feature_name=name
        )

        try:
            # Validate inputs
            self._validate_feature_name(name)
            self._validate_feature_type(feature_type)
            self._validate_data_type(data_type)
            self._validate_source_tables(source_tables)

            # Check if feature already exists
            if name in self._feature_definitions:
                raise ValidationError(
                    f"Feature '{name}' already exists",
                    field="name"
                )

            # Create feature definition
            feature_def = FeatureDefinition(
                name=name,
                description=description,
                feature_type=feature_type,
                data_type=data_type,
                source_tables=source_tables,
                transformation=transformation,
                dependencies=dependencies or []
            )

            # Store feature definition
            self._feature_definitions[name] = feature_def

            return ServiceResult.ok(
                data=feature_def,
                metadata={
                    "feature_name": name,
                    "created": True
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "create_feature_definition", context)

    async def get_feature_definition(
        self,
        name: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[FeatureDefinition]:
        """Get a feature definition by name.

        Args:
            name: Feature name
            context: Service context

        Returns:
            ServiceResult with feature definition

        Raises:
            ValidationError: If name invalid
            NotFoundError: If feature not found
            ServiceError: If retrieval fails
        """
        self._log_operation("get_feature_definition", context=context, feature_name=name)

        try:
            self._validate_feature_name(name)

            if name not in self._feature_definitions:
                raise NotFoundError("feature", name)

            feature_def = self._feature_definitions[name]

            return ServiceResult.ok(
                data=feature_def,
                metadata={"feature_name": name}
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_feature_definition", context)

    async def list_feature_definitions(
        self,
        feature_type: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[FeatureDefinition]]:
        """List feature definitions with optional filtering.

        Args:
            feature_type: Filter by feature type
            limit: Maximum results
            context: Service context

        Returns:
            ServiceResult with list of feature definitions
        """
        self._log_operation(
            "list_feature_definitions",
            context=context,
            feature_type=feature_type
        )

        try:
            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )

            # Filter features
            features = list(self._feature_definitions.values())

            if feature_type:
                self._validate_feature_type(feature_type)
                features = [f for f in features if f.feature_type == feature_type]

            # Apply limit
            features = features[:limit]

            return ServiceResult.ok(
                data=features,
                metadata={
                    "feature_count": len(features),
                    "limit": limit,
                    "feature_type": feature_type
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "list_feature_definitions", context)

    async def generate_features(
        self,
        feature_names: List[str],
        entity_ids: List[str],
        asof_date: datetime,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Generate features for entities at a specific point in time.

        Args:
            feature_names: List of feature names to generate
            entity_ids: List of entity identifiers
            asof_date: Point in time for feature calculation
            context: Service context

        Returns:
            ServiceResult with generated features

        Raises:
            ValidationError: If parameters invalid
            ServiceError: If generation fails
        """
        self._log_operation(
            "generate_features",
            context=context,
            feature_count=len(feature_names),
            entity_count=len(entity_ids)
        )

        try:
            # Validate inputs
            self._validate_feature_names(feature_names)
            self._validate_entity_ids(entity_ids)
            self._validate_asof_date(asof_date)

            # Generate features (simplified implementation)
            features = await self._compute_features(feature_names, entity_ids, asof_date)

            return ServiceResult.ok(
                data=features,
                metadata={
                    "feature_names": feature_names,
                    "entity_count": len(entity_ids),
                    "asof_date": asof_date.isoformat(),
                    "generated_features": len(features)
                }
            )

        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "generate_features", context)

    async def get_feature_lineage(
        self,
        feature_name: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get feature lineage and dependencies.

        Args:
            feature_name: Feature name to analyze
            context: Service context

        Returns:
            ServiceResult with feature lineage

        Raises:
            ValidationError: If name invalid
            NotFoundError: If feature not found
            ServiceError: If analysis fails
        """
        self._log_operation("get_feature_lineage", context=context, feature_name=feature_name)

        try:
            self._validate_feature_name(feature_name)

            if feature_name not in self._feature_definitions:
                raise NotFoundError("feature", feature_name)

            feature_def = self._feature_definitions[feature_name]
            lineage = self._build_lineage_graph(feature_def)

            return ServiceResult.ok(
                data=lineage,
                metadata={
                    "feature_name": feature_name,
                    "dependency_count": len(feature_def.dependencies)
                }
            )

        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_feature_lineage", context)

    # Private helper methods

    def _validate_feature_name(self, name: str) -> None:
        """Validate feature name."""
        if not name or not name.strip():
            raise ValidationError("Feature name is required", field="name")

        if len(name) > 100:
            raise ValidationError("Feature name too long", field="name")

        # Check for invalid characters
        invalid_chars = ["<", ">", "&", "\"", "'", ";"]
        if any(char in name for char in invalid_chars):
            raise ValidationError("Feature name contains invalid characters", field="name")

    def _validate_feature_names(self, names: List[str]) -> None:
        """Validate list of feature names."""
        if not names:
            raise ValidationError("Feature names list cannot be empty", field="feature_names")

        if len(names) > 100:
            raise ValidationError("Too many features (max 100)", field="feature_names")

        for name in names:
            self._validate_feature_name(name)

    def _validate_feature_type(self, feature_type: str) -> None:
        """Validate feature type."""
        valid_types = ["numerical", "categorical", "temporal", "derived"]
        if feature_type not in valid_types:
            raise ValidationError(
                f"Invalid feature type. Must be one of: {', '.join(valid_types)}",
                field="feature_type"
            )

    def _validate_data_type(self, data_type: str) -> None:
        """Validate data type."""
        valid_types = ["float64", "int64", "string", "datetime64"]
        if data_type not in valid_types:
            raise ValidationError(
                f"Invalid data type. Must be one of: {', '.join(valid_types)}",
                field="data_type"
            )

    def _validate_source_tables(self, tables: List[str]) -> None:
        """Validate source table names."""
        if not tables:
            raise ValidationError("Source tables list cannot be empty", field="source_tables")

        for table in tables:
            if not table or not table.strip():
                raise ValidationError("Source table name cannot be empty", field="source_tables")

    def _validate_entity_ids(self, entity_ids: List[str]) -> None:
        """Validate entity identifiers."""
        if not entity_ids:
            raise ValidationError("Entity IDs list cannot be empty", field="entity_ids")

        if len(entity_ids) > 1000:
            raise ValidationError("Too many entities (max 1000)", field="entity_ids")

        for entity_id in entity_ids:
            if not entity_id or not entity_id.strip():
                raise ValidationError("Entity ID cannot be empty", field="entity_ids")

    def _validate_asof_date(self, asof_date: datetime) -> None:
        """Validate as-of date."""
        now = datetime.now()
        if asof_date > now:
            raise ValidationError("As-of date cannot be in the future", field="asof_date")

        # Check for reasonable date range
        min_date = now - timedelta(days=365 * 10)  # 10 years ago
        if asof_date < min_date:
            raise ValidationError("As-of date too far in the past", field="asof_date")

    def _initialize_feature_definitions(self) -> None:
        """Initialize default feature definitions."""
        # Add common feature definitions
        self._feature_definitions.update({
            "temperature_avg": FeatureDefinition(
                name="temperature_avg",
                description="Average temperature",
                feature_type="numerical",
                data_type="float64",
                source_tables=["weather_data"],
                transformation="AVG(temperature)"
            ),
            "load_peak": FeatureDefinition(
                name="load_peak",
                description="Peak load",
                feature_type="numerical",
                data_type="float64",
                source_tables=["load_data"],
                transformation="MAX(load)"
            ),
            "price_volatility": FeatureDefinition(
                name="price_volatility",
                description="Price volatility metric",
                feature_type="derived",
                data_type="float64",
                source_tables=["price_data"],
                transformation="STDDEV(price) / AVG(price)",
                dependencies=["price_data"]
            )
        })

    async def _compute_features(
        self,
        feature_names: List[str],
        entity_ids: List[str],
        asof_date: datetime
    ) -> Dict[str, Any]:
        """Compute features for entities at point in time."""
        # Simplified implementation
        # In production, would query actual data sources
        features = {}

        for feature_name in feature_names:
            if feature_name in self._feature_definitions:
                # Generate mock feature values
                import random
                features[feature_name] = {
                    "feature_name": feature_name,
                    "entity_values": {
                        entity_id: random.uniform(0, 100) for entity_id in entity_ids
                    },
                    "computed_at": asof_date.isoformat(),
                    "feature_version": self._config.feature_version
                }

        return features

    def _build_lineage_graph(self, feature_def: FeatureDefinition) -> Dict[str, Any]:
        """Build lineage graph for a feature."""
        lineage = {
            "feature": feature_def.name,
            "description": feature_def.description,
            "source_tables": feature_def.source_tables,
            "transformation": feature_def.transformation,
            "dependencies": feature_def.dependencies,
            "dependents": []  # Would be populated by analyzing all features
        }

        return lineage

