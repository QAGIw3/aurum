"""V1 Split Flag integration with the advanced feature flag system."""

from __future__ import annotations

import os
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime
from dataclasses import dataclass

from .feature_flags import (
    FeatureFlag,
    FeatureFlagManager,
    FeatureFlagStatus,
    FeatureFlagStore,
    get_feature_manager
)

logger = logging.getLogger(__name__)


@dataclass
class V1SplitFlagConfig:
    """Configuration for a V1 split flag."""
    env_var: str
    module_path: str
    description: str
    default_enabled: bool = False


class V1SplitFlagManager:
    """Manager for V1 split flags with runtime toggle capabilities."""
    
    # Registry of all V1 split flags
    V1_SPLIT_FLAGS = {
        "eia": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_EIA",
            module_path="aurum.api.v1.eia",
            description="EIA datasets, dataset detail, series JSON/CSV, dimensions API router"
        ),
        "iso": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_ISO", 
            module_path="aurum.api.v1.iso",
            description="ISO LMP last-24h/hourly/daily/negative, JSON/CSV with ETag 304 API router"
        ),
        "drought": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_DROUGHT",
            module_path="aurum.api.v1.drought", 
            description="Drought tiles/info; dimensions/indices/usdm/layers API router"
        ),
        "admin": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_ADMIN",
            module_path="aurum.api.v1.admin",
            description="Cache invalidation endpoints and admin utilities"
        ),
        # Note: PPA and CURVES are noted as deprecated/always-on in docs but kept for compatibility
        "ppa": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_PPA",
            module_path="aurum.api.v1.ppa",
            description="PPA router (always enabled by default, flag retained for compatibility)",
            default_enabled=True
        ),
        "curves": V1SplitFlagConfig(
            env_var="AURUM_API_V1_SPLIT_CURVES",
            module_path="aurum.api.v1.curves", 
            description="Curves router (always enabled by default, flag retained for compatibility)",
            default_enabled=True
        ),
    }
    
    def __init__(self, feature_manager: Optional[FeatureFlagManager] = None):
        self.feature_manager = feature_manager or get_feature_manager()
        
    async def initialize_v1_flags(self) -> None:
        """Initialize V1 split flags in the feature flag system."""
        for flag_key, config in self.V1_SPLIT_FLAGS.items():
            feature_key = f"v1_split_{flag_key}"
            
            # Check if flag already exists
            existing_flag = await self.feature_manager.get_flag(feature_key)
            if existing_flag:
                continue
                
            # Get current environment value
            env_value = os.getenv(config.env_var, "0")
            current_enabled = env_value == "1" or config.default_enabled
            
            # Create feature flag
            flag = FeatureFlag(
                name=f"V1 Split - {flag_key.upper()}",
                key=feature_key,
                description=config.description,
                status=FeatureFlagStatus.ENABLED if current_enabled else FeatureFlagStatus.DISABLED,
                default_value=config.default_enabled,
                rules=[],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                created_by="system",
                tags=["v1-split", "router", flag_key]
            )
            
            await self.feature_manager.set_flag(flag)
            logger.info(f"Initialized V1 split flag: {feature_key} (enabled: {current_enabled})")
    
    async def is_v1_split_enabled(self, flag_name: str) -> bool:
        """Check if a V1 split flag is enabled."""
        if flag_name not in self.V1_SPLIT_FLAGS:
            logger.warning(f"Unknown V1 split flag: {flag_name}")
            return False
            
        feature_key = f"v1_split_{flag_name}"
        
        # First check feature flag system
        try:
            context = {"source": "v1_split_check"}
            flag = await self.feature_manager.get_flag(feature_key)
            if flag:
                is_enabled = flag.status == FeatureFlagStatus.ENABLED
            else:
                # Flag not found, use environment fallback
                config = self.V1_SPLIT_FLAGS[flag_name]
                env_value = os.getenv(config.env_var, "0")
                is_enabled = env_value == "1" or config.default_enabled
            
            # Also sync environment variable for backward compatibility
            config = self.V1_SPLIT_FLAGS[flag_name]
            expected_env_value = "1" if is_enabled else "0"
            current_env_value = os.getenv(config.env_var, "0")
            
            if current_env_value != expected_env_value:
                os.environ[config.env_var] = expected_env_value
                logger.debug(f"Synced environment variable {config.env_var} = {expected_env_value}")
            
            return is_enabled
            
        except Exception as e:
            logger.error(f"Error checking V1 split flag {flag_name}: {e}")
            # Fallback to environment variable
            config = self.V1_SPLIT_FLAGS[flag_name]
            env_value = os.getenv(config.env_var, "0")
            return env_value == "1" or config.default_enabled
    
    async def set_v1_split_enabled(self, flag_name: str, enabled: bool, user_id: str = "system") -> bool:
        """Enable or disable a V1 split flag at runtime."""
        if flag_name not in self.V1_SPLIT_FLAGS:
            logger.warning(f"Unknown V1 split flag: {flag_name}")
            return False
            
        feature_key = f"v1_split_{flag_name}"
        config = self.V1_SPLIT_FLAGS[flag_name]
        
        try:
            # Update feature flag
            status = FeatureFlagStatus.ENABLED if enabled else FeatureFlagStatus.DISABLED
            success = await self.feature_manager.update_flag_status(feature_key, status)
            
            if success:
                # Sync environment variable
                os.environ[config.env_var] = "1" if enabled else "0"
                logger.info(f"V1 split flag {flag_name} {'enabled' if enabled else 'disabled'} by {user_id}")
                return True
            else:
                logger.error(f"Failed to update feature flag {feature_key}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting V1 split flag {flag_name}: {e}")
            return False
    
    async def get_v1_split_status(self) -> Dict[str, Any]:
        """Get status of all V1 split flags."""
        status = {}
        
        for flag_name, config in self.V1_SPLIT_FLAGS.items():
            feature_key = f"v1_split_{flag_name}"
            
            try:
                flag = await self.feature_manager.get_flag(feature_key)
                if flag:
                    status[flag_name] = {
                        "enabled": flag.status == FeatureFlagStatus.ENABLED,
                        "env_var": config.env_var,
                        "module_path": config.module_path,
                        "description": config.description,
                        "default_enabled": config.default_enabled,
                        "last_updated": flag.updated_at.isoformat(),
                        "feature_key": feature_key
                    }
                else:
                    # Flag not in system yet, check environment
                    env_enabled = os.getenv(config.env_var, "0") == "1"
                    status[flag_name] = {
                        "enabled": env_enabled or config.default_enabled,
                        "env_var": config.env_var,
                        "module_path": config.module_path,
                        "description": config.description,
                        "default_enabled": config.default_enabled,
                        "last_updated": None,
                        "feature_key": feature_key,
                        "status": "not_in_feature_system"
                    }
            except Exception as e:
                logger.error(f"Error getting status for V1 split flag {flag_name}: {e}")
                status[flag_name] = {
                    "enabled": False,
                    "error": str(e),
                    "feature_key": feature_key
                }
        
        return status
    
    def get_env_var_for_flag(self, flag_name: str) -> Optional[str]:
        """Get the environment variable name for a V1 split flag."""
        config = self.V1_SPLIT_FLAGS.get(flag_name)
        return config.env_var if config else None
        
    def get_module_path_for_flag(self, flag_name: str) -> Optional[str]:
        """Get the module path for a V1 split flag."""
        config = self.V1_SPLIT_FLAGS.get(flag_name)
        return config.module_path if config else None


# Global instance
_v1_split_manager: Optional[V1SplitFlagManager] = None


def get_v1_split_manager() -> V1SplitFlagManager:
    """Get the global V1 split flag manager."""
    global _v1_split_manager
    if _v1_split_manager is None:
        _v1_split_manager = V1SplitFlagManager()
    return _v1_split_manager


async def initialize_v1_split_flags() -> None:
    """Initialize V1 split flags integration."""
    manager = get_v1_split_manager()
    await manager.initialize_v1_flags()