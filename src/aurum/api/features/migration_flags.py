"""Feature flags for gradual migration of legacy code to new architecture.

This module defines feature flags that control the rollout of new features
and the deprecation of legacy code during the platform refactoring.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional

from .models import FeatureFlag, FeatureFlagRule, RolloutStrategy, UserSegment
from .manager import get_feature_manager

logger = logging.getLogger(__name__)


# Migration Feature Flags
MIGRATION_FLAGS = {
    # Database Connection Management
    "unified_db_connections": FeatureFlag(
        key="unified_db_connections",
        name="Unified Database Connection Management",
        description="Use the new unified connection pool management system",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "db_health_monitoring": FeatureFlag(
        key="db_health_monitoring",
        name="Database Health Monitoring",
        description="Enable production database health monitoring with alerting",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    # External Data Collection
    "unified_external_collectors": FeatureFlag(
        key="unified_external_collectors",
        name="Unified External Data Collectors",
        description="Use the new unified collector framework for external data providers",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "fred_unified_collector": FeatureFlag(
        key="fred_unified_collector",
        name="FRED Unified Collector",
        description="Use the new unified collector for FRED data collection",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "noaa_unified_collector": FeatureFlag(
        key="noaa_unified_collector",
        name="NOAA Unified Collector",
        description="Use the new unified collector for NOAA data collection",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "worldbank_unified_collector": FeatureFlag(
        key="worldbank_unified_collector",
        name="WorldBank Unified Collector",
        description="Use the new unified collector for WorldBank data collection",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    # Service Layer Migration
    "new_ppa_service": FeatureFlag(
        key="new_ppa_service",
        name="New PPA Service Architecture",
        description="Use the new PPA service from services/core/ instead of legacy service",
        enabled=False,  # Start disabled for gradual rollout
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 50},  # Roll out to 50% of users initially
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "decomposed_model_registry": FeatureFlag(
        key="decomposed_model_registry",
        name="Decomposed Model Registry Service",
        description="Use the new decomposed model registry services",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "enhanced_metrics_system": FeatureFlag(
        key="enhanced_metrics_system",
        name="Enhanced Metrics System",
        description="Use the new decomposed metrics system with better observability",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    # Legacy Code Deprecation
    "legacy_dao_deprecation": FeatureFlag(
        key="legacy_dao_deprecation",
        name="Legacy DAO Deprecation Warnings",
        description="Emit deprecation warnings for legacy DAO usage",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "legacy_service_deprecation": FeatureFlag(
        key="legacy_service_deprecation",
        name="Legacy Service Deprecation Warnings",
        description="Emit deprecation warnings for legacy service usage",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    # Performance Optimizations
    "connection_pool_optimization": FeatureFlag(
        key="connection_pool_optimization",
        name="Connection Pool Optimization",
        description="Enable optimized connection pool settings for better performance",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),

    "cache_optimization": FeatureFlag(
        key="cache_optimization",
        name="Cache Optimization",
        description="Enable optimized caching strategies",
        enabled=True,
        rules=[
            FeatureFlagRule(
                conditions={"percentage": 100},
                rollout_strategy=RolloutStrategy.PERCENTAGE,
                enabled=True,
            )
        ],
    ),
}


async def is_migration_feature_enabled(flag_key: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if a migration feature flag is enabled."""
    manager = get_feature_manager()
    return await manager.is_enabled(flag_key, context or {})


async def get_migration_feature_variant(flag_key: str, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Get the variant for a migration feature flag (for A/B testing)."""
    manager = get_feature_manager()
    return await manager.get_variant(flag_key, context or {})


async def initialize_migration_flags() -> None:
    """Initialize migration feature flags in the feature flag store."""
    manager = get_feature_manager()

    for flag_key, flag in MIGRATION_FLAGS.items():
        try:
            # Register or update the feature flag
            await manager.register_flag(flag)
            logger.info(f"Registered migration feature flag: {flag_key}")
        except Exception as e:
            logger.error(f"Failed to register migration flag {flag_key}: {e}")


# Environment variable shortcuts for common migration flags
def should_use_unified_db_connections() -> bool:
    """Check if unified database connections should be used."""
    import os
    return os.getenv("AURUM_UNIFIED_DB_CONNECTIONS", "true").lower() == "true"


def should_enable_db_health_monitoring() -> bool:
    """Check if database health monitoring should be enabled."""
    import os
    return os.getenv("AURUM_DB_HEALTH_MONITORING", "true").lower() == "true"


def should_use_unified_collectors() -> bool:
    """Check if unified external collectors should be used."""
    import os
    return os.getenv("AURUM_UNIFIED_COLLECTORS", "true").lower() == "true"


def should_use_new_ppa_service() -> bool:
    """Check if new PPA service should be used."""
    import os
    return os.getenv("AURUM_NEW_PPA_SERVICE", "false").lower() == "true"


def should_emit_deprecation_warnings() -> bool:
    """Check if deprecation warnings should be emitted."""
    import os
    return os.getenv("AURUM_EMIT_DEPRECATION_WARNINGS", "true").lower() == "true"


# Utility functions for migration control
async def can_migrate_database_connections(context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if database connection migration is enabled."""
    return await is_migration_feature_enabled("unified_db_connections", context)


async def can_use_production_monitoring(context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if production database monitoring is enabled."""
    return await is_migration_feature_enabled("db_health_monitoring", context)


async def can_use_unified_collectors(context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if unified external collectors are enabled."""
    return await is_migration_feature_enabled("unified_external_collectors", context)


async def should_warn_about_legacy_dao(context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if deprecation warnings for legacy DAOs should be emitted."""
    return await is_migration_feature_enabled("legacy_dao_deprecation", context)


async def should_warn_about_legacy_service(context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if deprecation warnings for legacy services should be emitted."""
    return await is_migration_feature_enabled("legacy_service_deprecation", context)
