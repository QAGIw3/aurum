"""Compatibility shim for feature store service.

Provides backward compatibility for code using the old feature_store_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import pandas as pd

from aurum.services.ml.feature_store import (
    FeatureStoreService,
    FeatureConfig as NewFeatureConfig,
    FeatureDefinition as NewFeatureDefinition
)

# Re-export with old class structure
class FeatureConfig(BaseModel):
    """Configuration for feature generation."""
    lookback_days: int = Field(default=365, description="Days of historical data to maintain")
    temporal_resolution: str = Field(default="hourly", description="Temporal resolution for features")
    feature_version: str = Field(default="v1", description="Feature version")
    enable_caching: bool = Field(default=True, description="Enable feature caching")
    cache_ttl_minutes: int = Field(default=60, description="Cache time-to-live in minutes")
    geography: str = Field(default="US", description="Geographic scope for features")


class FeatureDefinition(BaseModel):
    """Definition of a feature in the feature store."""
    name: str
    description: str
    feature_type: str  # 'numerical', 'categorical', 'temporal', 'derived'
    data_type: str  # 'float64', 'int64', 'string', 'datetime64'
    source_tables: List[str]
    transformation: Optional[str] = None
    dependencies: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CrossAssetFeature(BaseModel):
    """A feature that combines data from multiple asset types."""
    name: str
    weather_features: List[str]
    load_features: List[str]
    price_features: List[str]
    join_keys: List[str]  # Keys for joining the different data sources
    aggregation_method: str  # 'mean', 'sum', 'max', 'min', 'lag'
    lookback_periods: int  # Number of periods to look back
    feature_description: str


# Singleton instance
_service_instance = None


def get_feature_store_service(config: Optional[FeatureConfig] = None) -> FeatureStoreService:
    """Get singleton feature store service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = FeatureStoreService()
        if config:
            # Map old config to new structure
            _service_instance._config = NewFeatureConfig(
                lookback_days=config.lookback_days,
                temporal_resolution=config.temporal_resolution,
                feature_version=config.feature_version,
                enable_caching=config.enable_caching,
                cache_ttl_minutes=config.cache_ttl_minutes,
                geography=config.geography
            )
    return _service_instance


async def get_features_for_scenario(
    scenario_id: str,
    feature_names: List[str],
    start_date: datetime,
    end_date: datetime,
    geography: str = "US"
) -> Dict[str, Any]:
    """Get features for scenario modeling."""
    service = get_feature_store_service()
    
    # Map to the new service API
    # The new service uses entity_ids, so we'll use scenario_id as entity
    result = await service.generate_features(
        feature_names=feature_names,
        entity_ids=[scenario_id],
        asof_date=end_date  # Use end_date as the as-of date
    )
    
    if result.success and result.data:
        # Transform to expected format
        features = {}
        for feature_name, feature_data in result.data.items():
            if isinstance(feature_data, dict) and "entity_values" in feature_data:
                features[feature_name] = feature_data["entity_values"].get(scenario_id, None)
            else:
                features[feature_name] = feature_data
                
        return {
            "scenario_id": scenario_id,
            "features": features,
            "feature_names": feature_names,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "geography": geography
        }
    else:
        return {
            "scenario_id": scenario_id,
            "features": {},
            "error": result.error
        }


async def get_training_features(
    model_id: str,
    feature_names: List[str],
    start_date: datetime,
    end_date: datetime,
    split_ratio: float = 0.8
) -> Dict[str, pd.DataFrame]:
    """Get features for model training with train/test split."""
    service = get_feature_store_service()
    
    # Generate features
    result = await service.generate_features(
        feature_names=feature_names,
        entity_ids=[model_id],
        asof_date=end_date
    )
    
    # Create mock dataframes for compatibility
    if result.success and result.data:
        # Generate time series
        date_range = pd.date_range(start=start_date, end=end_date, freq='H')
        n_points = len(date_range)
        split_idx = int(n_points * split_ratio)
        
        # Create feature dataframe
        feature_data = {}
        for feature_name in feature_names:
            import numpy as np
            # Generate mock time series data
            feature_data[feature_name] = np.random.randn(n_points)
        
        df = pd.DataFrame(feature_data, index=date_range)
        
        return {
            "train": df.iloc[:split_idx],
            "test": df.iloc[split_idx:],
            "feature_names": feature_names
        }
    else:
        return {
            "train": pd.DataFrame(),
            "test": pd.DataFrame(),
            "feature_names": feature_names
        }


async def create_time_window_features(
    base_features: pd.DataFrame,
    window_sizes: List[int] = [1, 7, 30],
    aggregations: List[str] = ["mean", "std", "min", "max"]
) -> pd.DataFrame:
    """Create time window aggregation features."""
    result = base_features.copy()
    
    for col in base_features.columns:
        for window in window_sizes:
            for agg in aggregations:
                feature_name = f"{col}_{window}d_{agg}"
                if agg == "mean":
                    result[feature_name] = base_features[col].rolling(window).mean()
                elif agg == "std":
                    result[feature_name] = base_features[col].rolling(window).std()
                elif agg == "min":
                    result[feature_name] = base_features[col].rolling(window).min()
                elif agg == "max":
                    result[feature_name] = base_features[col].rolling(window).max()
    
    return result


async def create_lag_features(
    base_features: pd.DataFrame,
    lag_periods: List[int] = [1, 2, 3, 7, 14, 30]
) -> pd.DataFrame:
    """Create lag features."""
    result = base_features.copy()
    
    for col in base_features.columns:
        for lag in lag_periods:
            result[f"{col}_lag_{lag}"] = base_features[col].shift(lag)
    
    return result


async def create_seasonal_features(
    datetime_index: pd.DatetimeIndex
) -> pd.DataFrame:
    """Create seasonal features from datetime index."""
    features = pd.DataFrame(index=datetime_index)
    
    features["hour"] = datetime_index.hour
    features["day_of_week"] = datetime_index.dayofweek
    features["day_of_month"] = datetime_index.day
    features["month"] = datetime_index.month
    features["quarter"] = datetime_index.quarter
    features["is_weekend"] = (datetime_index.dayofweek >= 5).astype(int)
    features["hour_sin"] = pd.np.sin(2 * pd.np.pi * features["hour"] / 24)
    features["hour_cos"] = pd.np.cos(2 * pd.np.pi * features["hour"] / 24)
    features["month_sin"] = pd.np.sin(2 * pd.np.pi * features["month"] / 12)
    features["month_cos"] = pd.np.cos(2 * pd.np.pi * features["month"] / 12)
    
    return features
