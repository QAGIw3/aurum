"""Feature store for cross-asset joins: weather→load→price features in Iceberg.

This module provides:
- Iceberg feature store tables for cross-asset data
- Automated feature engineering for weather, load, and price data
- Cross-asset joins with proper temporal alignment
- Feature versioning and lineage tracking
- Integration with scenario modeling and forecasting
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured


class FeatureStoreConfig(BaseModel):
    """Configuration for the feature store."""

    iceberg_catalog: str = Field(default="iceberg_market", description="Iceberg catalog name")
    feature_table_prefix: str = Field(default="features_", description="Prefix for feature table names")
    temporal_resolution: str = Field(default="hourly", description="Temporal resolution for features")
    lookback_days: int = Field(default=365, description="Days of historical data to maintain")
    feature_version: str = Field(default="v1", description="Feature version")
    enable_caching: bool = Field(default=True, description="Enable feature caching")
    cache_ttl_minutes: int = Field(default=60, description="Cache time-to-live in minutes")


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
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.metadata is None:
            self.metadata = {}


@dataclass
class CrossAssetFeature:
    """A feature that combines data from multiple asset types."""

    name: str
    weather_features: List[str]
    load_features: List[str]
    price_features: List[str]
    join_keys: List[str]  # Keys for joining the different data sources
    aggregation_method: str  # 'mean', 'sum', 'max', 'min', 'lag'
    lookback_periods: int  # Number of periods to look back
    feature_description: str


class IcebergFeatureStore:
    """Feature store implementation using Iceberg for cross-asset features."""

    def __init__(self, config: FeatureStoreConfig):
        self.config = config
        self.feature_definitions: Dict[str, FeatureDefinition] = {}
        self._cache: Dict[str, Any] = {}
        self._initialize_feature_definitions()

    def _initialize_feature_definitions(self) -> None:
        """Initialize standard feature definitions for cross-asset analysis."""

        # Weather features
        self.feature_definitions["temperature"] = FeatureDefinition(
            name="temperature",
            description="Temperature in Celsius",
            feature_type="numerical",
            data_type="float64",
            source_tables=["noaa_weather"],
            metadata={"units": "Celsius", "source": "NOAA"}
        )

        self.feature_definitions["humidity"] = FeatureDefinition(
            name="humidity",
            description="Relative humidity percentage",
            feature_type="numerical",
            data_type="float64",
            source_tables=["noaa_weather"],
            metadata={"units": "percent", "source": "NOAA"}
        )

        self.feature_definitions["wind_speed"] = FeatureDefinition(
            name="wind_speed",
            description="Wind speed in m/s",
            feature_type="numerical",
            data_type="float64",
            source_tables=["noaa_weather"],
            metadata={"units": "m/s", "source": "NOAA"}
        )

        # Load features
        self.feature_definitions["load_mw"] = FeatureDefinition(
            name="load_mw",
            description="Electricity load in MW",
            feature_type="numerical",
            data_type="float64",
            source_tables=["iso_load"],
            metadata={"units": "MW", "source": "ISO"}
        )

        self.feature_definitions["load_forecast_error"] = FeatureDefinition(
            name="load_forecast_error",
            description="Difference between actual and forecasted load",
            feature_type="derived",
            data_type="float64",
            source_tables=["iso_load"],
            transformation="actual - forecast",
            metadata={"units": "MW", "source": "ISO"}
        )

        # Price features
        self.feature_definitions["lmp_price"] = FeatureDefinition(
            name="lmp_price",
            description="Locational marginal price",
            feature_type="numerical",
            data_type="float64",
            source_tables=["iso_lmp"],
            metadata={"units": "$/MWh", "source": "ISO"}
        )

        self.feature_definitions["price_volatility"] = FeatureDefinition(
            name="price_volatility",
            description="Price volatility over rolling window",
            feature_type="derived",
            data_type="float64",
            source_tables=["iso_lmp"],
            transformation="rolling_std",
            metadata={"units": "$/MWh", "source": "ISO"}
        )

        # Cross-asset features
        self.feature_definitions["weather_load_correlation"] = FeatureDefinition(
            name="weather_load_correlation",
            description="Correlation between weather and load",
            feature_type="derived",
            data_type="float64",
            source_tables=["noaa_weather", "iso_load"],
            transformation="correlation",
            dependencies=["temperature", "load_mw"],
            metadata={"source": "cross-asset"}
        )

        self.feature_definitions["load_price_sensitivity"] = FeatureDefinition(
            name="load_price_sensitivity",
            description="Sensitivity of price to load changes",
            feature_type="derived",
            data_type="float64",
            source_tables=["iso_load", "iso_lmp"],
            transformation="price_elasticity",
            dependencies=["load_mw", "lmp_price"],
            metadata={"source": "cross-asset"}
        )

    async def create_cross_asset_features(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str = "US",
        feature_types: List[str] = None,
    ) -> pd.DataFrame:
        """Create cross-asset features by joining weather, load, and price data."""

        if feature_types is None:
            feature_types = ["weather", "load", "price", "cross_asset"]

        log_structured(
            "info",
            "creating_cross_asset_features",
            start_date=start_date,
            end_date=end_date,
            geography=geography,
            feature_types=feature_types,
        )

        # Get base data from each asset type
        weather_data = await self._get_weather_data(start_date, end_date, geography)
        load_data = await self._get_load_data(start_date, end_date, geography)
        price_data = await self._get_price_data(start_date, end_date, geography)

        # Join the datasets
        joined_data = await self._join_asset_data(weather_data, load_data, price_data)

        # Create features
        features_data = await self._create_features_from_joined_data(joined_data, feature_types)

        # Cache the results
        cache_key = f"{geography}_{start_date}_{end_date}"
        self._cache[cache_key] = features_data

        return features_data

    async def _get_weather_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> pd.DataFrame:
        """Get weather data for the specified time period and geography."""
        # This would query the actual NOAA weather data in Iceberg
        # For now, return synthetic data

        date_range = pd.date_range(start=start_date, end=end_date, freq='H')

        # Generate synthetic weather data
        np.random.seed(42)  # For reproducible results

        weather_data = pd.DataFrame({
            'timestamp': date_range,
            'geography': geography,
            'temperature': 15 + 10 * np.sin(2 * np.pi * date_range.hour / 24) + np.random.normal(0, 2, len(date_range)),
            'humidity': 50 + 30 * np.sin(2 * np.pi * date_range.hour / 24) + np.random.normal(0, 10, len(date_range)),
            'wind_speed': 5 + np.random.exponential(2, len(date_range)),
            'solar_irradiance': np.maximum(0, 800 * np.sin(np.pi * date_range.hour / 12) + np.random.normal(0, 100, len(date_range))),
        })

        return weather_data

    async def _get_load_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> pd.DataFrame:
        """Get load data for the specified time period and geography."""
        # This would query the actual ISO load data in Iceberg
        # For now, return synthetic data

        date_range = pd.date_range(start=start_date, end=end_date, freq='H')

        # Generate synthetic load data with daily and weekly patterns
        base_load = 1000  # Base load in MW

        load_data = pd.DataFrame({
            'timestamp': date_range,
            'geography': geography,
            'load_mw': base_load * (
                0.7 + 0.6 * np.sin(2 * np.pi * date_range.hour / 24) +  # Daily pattern
                0.2 * (1 if date_range.weekday < 5 else 0.5) +  # Weekly pattern (lower on weekends)
                np.random.normal(0, 0.05, len(date_range))  # Random variation
            ),
        })

        return load_data

    async def _get_price_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> pd.DataFrame:
        """Get price data for the specified time period and geography."""
        # This would query the actual ISO LMP data in Iceberg
        # For now, return synthetic data

        date_range = pd.date_range(start=start_date, end=end_date, freq='H')

        # Generate synthetic price data
        base_price = 50  # Base price in $/MWh

        price_data = pd.DataFrame({
            'timestamp': date_range,
            'geography': geography,
            'lmp_price': base_price * (
                0.8 + 0.4 * np.sin(2 * np.pi * date_range.hour / 24) +  # Daily pattern
                0.3 * (1 if date_range.weekday < 5 else 0.7) +  # Weekly pattern
                np.random.normal(0, 0.1, len(date_range))  # Random variation
            ),
        })

        return price_data

    async def _join_asset_data(
        self,
        weather_data: pd.DataFrame,
        load_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Join weather, load, and price data on timestamp and geography."""

        # Merge dataframes on timestamp and geography
        joined = weather_data.merge(
            load_data,
            on=['timestamp', 'geography'],
            how='inner'
        ).merge(
            price_data,
            on=['timestamp', 'geography'],
            how='inner'
        )

        # Sort by timestamp
        joined = joined.sort_values('timestamp').reset_index(drop=True)

        return joined

    async def _create_features_from_joined_data(
        self,
        joined_data: pd.DataFrame,
        feature_types: List[str]
    ) -> pd.DataFrame:
        """Create features from joined cross-asset data."""

        features = joined_data.copy()

        if "weather" in feature_types:
            features = await self._add_weather_features(features)

        if "load" in feature_types:
            features = await self._add_load_features(features)

        if "price" in feature_types:
            features = await self._add_price_features(features)

        if "cross_asset" in feature_types:
            features = await self._add_cross_asset_features(features)

        return features

    async def _add_weather_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add derived weather features."""

        # Temperature-related features
        data['temp_squared'] = data['temperature'] ** 2
        data['temp_cubed'] = data['temperature'] ** 3

        # Weather indicators
        data['is_hot'] = (data['temperature'] > 25).astype(int)
        data['is_cold'] = (data['temperature'] < 5).astype(int)
        data['high_humidity'] = (data['humidity'] > 70).astype(int)

        # Weather changes
        data['temp_change_1h'] = data['temperature'].diff()
        data['temp_change_3h'] = data['temperature'].diff(3)
        data['humidity_change_1h'] = data['humidity'].diff()

        # Wind power potential (simplified)
        data['wind_power_potential'] = 0.5 * data['wind_speed'] ** 3 * 1.225  # kg/m³ air density

        return data

    async def _add_load_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add derived load features."""

        # Load changes
        data['load_change_1h'] = data['load_mw'].diff()
        data['load_change_3h'] = data['load_mw'].diff(3)
        data['load_change_24h'] = data['load_mw'].diff(24)

        # Rolling statistics
        for window in [3, 6, 24, 168]:  # 3h, 6h, 1d, 1w
            if len(data) > window:
                data[f'load_mean_{window}h'] = data['load_mw'].rolling(window).mean()
                data[f'load_std_{window}h'] = data['load_mw'].rolling(window).std()
                data[f'load_max_{window}h'] = data['load_mw'].rolling(window).max()

        # Load ratios
        data['load_ratio_1h'] = data['load_mw'] / data['load_mean_3h'].shift(1)
        data['load_ratio_24h'] = data['load_mw'] / data['load_mean_24h'].shift(24)

        # Peak indicators
        data['is_peak_hour'] = ((data['timestamp'].dt.hour >= 17) & (data['timestamp'].dt.hour <= 20)).astype(int)
        data['is_weekend'] = (data['timestamp'].dt.weekday >= 5).astype(int)

        return data

    async def _add_price_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add derived price features."""

        # Price changes
        data['price_change_1h'] = data['lmp_price'].diff()
        data['price_change_3h'] = data['lmp_price'].diff(3)
        data['price_change_24h'] = data['lmp_price'].diff(24)

        # Rolling statistics
        for window in [3, 6, 24, 168]:
            if len(data) > window:
                data[f'price_mean_{window}h'] = data['lmp_price'].rolling(window).mean()
                data[f'price_std_{window}h'] = data['lmp_price'].rolling(window).std()
                data[f'price_max_{window}h'] = data['lmp_price'].rolling(window).max()

        # Price ratios
        data['price_ratio_1h'] = data['lmp_price'] / data['price_mean_3h'].shift(1)
        data['price_ratio_24h'] = data['lmp_price'] / data['price_mean_24h'].shift(24)

        # Price volatility (coefficient of variation)
        data['price_volatility_24h'] = data['price_std_24h'] / data['price_mean_24h']

        # Price spikes
        data['price_spike'] = ((data['lmp_price'] - data['price_mean_24h']) > 2 * data['price_std_24h']).astype(int)

        return data

    async def _add_cross_asset_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add cross-asset derived features."""

        # Weather-load relationships
        data['temp_load_correlation_24h'] = self._rolling_correlation(
            data['temperature'], data['load_mw'], 24
        )
        data['humidity_load_correlation_24h'] = self._rolling_correlation(
            data['humidity'], data['load_mw'], 24
        )

        # Load-price relationships
        data['load_price_correlation_24h'] = self._rolling_correlation(
            data['load_mw'], data['lmp_price'], 24
        )

        # Weather-price relationships
        data['temp_price_correlation_24h'] = self._rolling_correlation(
            data['temperature'], data['lmp_price'], 24
        )

        # Composite indicators
        data['weather_sensitivity_index'] = (
            data['temp_load_correlation_24h'] +
            data['humidity_load_correlation_24h']
        ) / 2

        data['price_elasticity_index'] = data['load_price_correlation_24h']

        # Load forecast error (simplified)
        if 'load_forecast_24h' in data.columns:
            data['load_forecast_error'] = data['load_mw'] - data['load_forecast_24h']
        else:
            # Use lagged load as simple forecast
            data['load_forecast_error'] = data['load_mw'] - data['load_mw'].shift(24)

        return data

    def _rolling_correlation(self, series1: pd.Series, series2: pd.Series, window: int) -> pd.Series:
        """Calculate rolling correlation between two series."""
        if len(series1) < window or len(series2) < window:
            return pd.Series([np.nan] * len(series1), index=series1.index)

        return series1.rolling(window).corr(series2)

    async def get_features_for_modeling(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str = "US",
        target_variable: str = "lmp_price",
        feature_list: List[str] = None,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """Get features and target for machine learning modeling."""

        # Get cross-asset features
        features = await self.create_cross_asset_features(start_date, end_date, geography)

        # Select features
        if feature_list is None:
            # Default feature selection
            feature_columns = [
                'temperature', 'humidity', 'wind_speed',
                'load_mw', 'load_change_1h', 'load_change_24h',
                'price_change_1h', 'price_volatility_24h',
                'temp_load_correlation_24h', 'load_price_correlation_24h',
                'is_peak_hour', 'is_weekend'
            ]
        else:
            feature_columns = feature_list

        # Prepare X and y
        available_features = [col for col in feature_columns if col in features.columns]
        X = features[available_features].copy()

        # Handle missing values
        X = X.fillna(method='ffill').fillna(method='bfill')

        if target_variable not in features.columns:
            raise ValueError(f"Target variable {target_variable} not found in features")

        y = features[target_variable].copy()
        y = y.fillna(method='ffill').fillna(method='bfill')

        # Remove rows with NaN in target
        valid_idx = y.notna()
        X = X[valid_idx]
        y = y[valid_idx]

        return X, y

    async def save_features_to_iceberg(
        self,
        features: pd.DataFrame,
        table_name: str,
        partition_by: List[str] = None
    ) -> None:
        """Save features to Iceberg table."""

        # This would use PyIceberg or similar to write to Iceberg
        # For now, just log the operation
        log_structured(
            "info",
            "saving_features_to_iceberg",
            table_name=table_name,
            num_rows=len(features),
            columns=list(features.columns),
        )

    def get_feature_definition(self, feature_name: str) -> Optional[FeatureDefinition]:
        """Get definition of a feature."""
        return self.feature_definitions.get(feature_name)

    def list_feature_definitions(self) -> Dict[str, FeatureDefinition]:
        """List all feature definitions."""
        return self.feature_definitions.copy()


class CrossAssetFeatureStore:
    """Main feature store for cross-asset analysis."""

    def __init__(self):
        self.iceberg_store = IcebergFeatureStore(FeatureStoreConfig())
        self._cache: Dict[str, Any] = {}

    async def get_features(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str = "US",
        feature_types: List[str] = None,
    ) -> pd.DataFrame:
        """Get cross-asset features for analysis."""

        cache_key = f"{geography}_{start_date}_{end_date}_{feature_types}"

        if self._cache.get(cache_key) and self.iceberg_store.config.enable_caching:
            return self._cache[cache_key]

        features = await self.iceberg_store.create_cross_asset_features(
            start_date, end_date, geography, feature_types
        )

        if self.iceberg_store.config.enable_caching:
            self._cache[cache_key] = features

        return features

    async def get_features_for_scenario(
        self,
        scenario_id: str,
        curve_families: List[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Get features specifically for scenario modeling."""

        # Determine geography and feature types based on scenario
        geography = "US"  # Default, could be determined from scenario metadata

        feature_types = []
        if "weather" in curve_families or "renewable" in curve_families:
            feature_types.append("weather")
        if "load" in curve_families or "demand" in curve_families:
            feature_types.append("load")
        if "price" in curve_families:
            feature_types.append("price")

        if not feature_types:
            feature_types = ["weather", "load", "price", "cross_asset"]

        features = await self.get_features(start_date, end_date, geography, feature_types)

        # Add scenario-specific features
        features['scenario_id'] = scenario_id
        features['curve_families'] = str(curve_families)

        return features

    async def create_feature_view(
        self,
        view_name: str,
        feature_list: List[str],
        filters: Dict[str, Any] = None,
    ) -> None:
        """Create a feature view for repeated use."""

        # This would create a view in Iceberg or save feature configuration
        log_structured(
            "info",
            "creating_feature_view",
            view_name=view_name,
            feature_list=feature_list,
            filters=filters,
        )

    def get_feature_info(self, feature_name: str) -> Dict[str, Any]:
        """Get information about a feature."""
        definition = self.iceberg_store.get_feature_definition(feature_name)
        if definition:
            return {
                "name": definition.name,
                "description": definition.description,
                "feature_type": definition.feature_type,
                "data_type": definition.data_type,
                "source_tables": definition.source_tables,
                "metadata": definition.metadata,
            }
        return {}

    def list_available_features(self) -> List[str]:
        """List all available features."""
        return list(self.iceberg_store.feature_definitions.keys())


# Global feature store instance
_feature_store: Optional[CrossAssetFeatureStore] = None


def get_feature_store() -> CrossAssetFeatureStore:
    """Get the global feature store instance."""
    global _feature_store
    if _feature_store is None:
        _feature_store = CrossAssetFeatureStore()
    return _feature_store
