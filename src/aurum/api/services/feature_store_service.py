"""ML Feature Store Service for forecasting and model training.

This service provides a unified interface for:
- Cross-asset feature engineering (weather, load, price data)
- Time-window aggregations and lag features
- Feature versioning and lineage tracking
- Training/serving parity for ML models
- Integration with scenario modeling and forecasting
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, log_structured
from ..daos.base_dao import TrinoDAO
from ...telemetry.context import get_tenant_id


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


class FeatureStoreDAO(TrinoDAO):
    """DAO for feature store operations using Trino."""

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize feature store DAO."""
        super().__init__(trino_config)
        self.table_name = "market_data.features"

    async def _connect(self) -> None:
        """Connect to Trino for feature store."""
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        pass

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute Trino query for features."""
        # Mock implementation
        return []

    async def save_features(
        self,
        features: Dict[str, Any],
        feature_set_id: str,
        geography: str,
        timestamp: datetime
    ) -> bool:
        """Save computed features to feature store.

        Args:
            features: Dictionary of feature name -> value
            feature_set_id: Unique identifier for this feature set
            geography: Geographic scope
            timestamp: Timestamp of the features

        Returns:
            True if saved successfully
        """
        # Implementation would save to Iceberg/Parquet
        log_structured(
            "info",
            "saving_features_to_store",
            feature_set_id=feature_set_id,
            feature_count=len(features),
            geography=geography,
            timestamp=timestamp.isoformat()
        )
        return True

    async def load_features(
        self,
        feature_set_id: str,
        geography: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Load features from feature store.

        Args:
            feature_set_id: Feature set identifier
            geography: Geographic scope
            start_date: Start date for features
            end_date: End date for features

        Returns:
            Features dictionary or None if not found
        """
        # Implementation would query Iceberg/Parquet
        log_structured(
            "info",
            "loading_features_from_store",
            feature_set_id=feature_set_id,
            geography=geography,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None
        )
        return None


class FeatureStoreService:
    """ML Feature Store Service for forecasting and model training."""

    def __init__(
        self,
        config: FeatureConfig,
        dao: Optional[FeatureStoreDAO] = None
    ):
        """Initialize feature store service.

        Args:
            config: Feature store configuration
            dao: Optional DAO for persistence (creates default if None)
        """
        self.config = config
        self.dao = dao or FeatureStoreDAO()
        self.feature_definitions: Dict[str, FeatureDefinition] = {}
        self._cache: Dict[str, Any] = {}
        self._initialize_feature_definitions()

        self.logger = logging.getLogger(__name__)
        self.logger.info("Feature store service initialized",
                        geography=config.geography,
                        feature_version=config.feature_version)

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
            dependencies=["load_mw"],
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
            dependencies=["lmp_price"],
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
        feature_types: Optional[List[str]] = None,
        scenario_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create cross-asset features by joining weather, load, and price data.

        Args:
            start_date: Start date for feature generation
            end_date: End date for feature generation
            geography: Geographic scope
            feature_types: Types of features to generate
            scenario_id: Optional scenario ID for tracking

        Returns:
            Dictionary of computed features
        """
        request_id = get_request_id() or str(uuid4())
        tenant_id = get_tenant_id()

        log_structured(
            "info",
            "creating_cross_asset_features",
            request_id=request_id,
            tenant_id=tenant_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            geography=geography,
            feature_types=feature_types,
            scenario_id=scenario_id
        )

        if feature_types is None:
            feature_types = ["weather", "load", "price", "cross_asset"]

        try:
            # Check cache first
            cache_key = f"{geography}_{start_date}_{end_date}_{feature_types}_{scenario_id}"
            if self.config.enable_caching and cache_key in self._cache:
                log_structured("info", "returning_cached_features", cache_key=cache_key)
                return self._cache[cache_key]

            # Get base data from each asset type
            weather_data = await self._get_weather_data(start_date, end_date, geography)
            load_data = await self._get_load_data(start_date, end_date, geography)
            price_data = await self._get_price_data(start_date, end_date, geography)

            # Join the datasets
            joined_data = await self._join_asset_data(weather_data, load_data, price_data)

            # Create features
            features_data = await self._create_features_from_joined_data(joined_data, feature_types)

            # Add metadata
            features_data["metadata"] = {
                "feature_version": self.config.feature_version,
                "geography": geography,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "feature_types": feature_types,
                "scenario_id": scenario_id,
                "generated_at": datetime.utcnow().isoformat(),
                "generated_by": "feature_store_service"
            }

            # Cache the results
            if self.config.enable_caching:
                self._cache[cache_key] = features_data

            # Save to feature store if scenario_id provided
            if scenario_id:
                feature_set_id = f"{scenario_id}_{geography}_{start_date.date().isoformat()}"
                await self.dao.save_features(features_data, feature_set_id, geography, start_date)

            log_structured(
                "info",
                "cross_asset_features_generated",
                request_id=request_id,
                feature_count=len(features_data),
                geography=geography,
                scenario_id=scenario_id
            )

            return features_data

        except Exception as e:
            log_structured(
                "error",
                "feature_generation_failed",
                request_id=request_id,
                error=str(e),
                geography=geography,
                scenario_id=scenario_id
            )
            raise

    async def _get_weather_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> Dict[str, Any]:
        """Get weather data for the specified time period and geography."""
        # In real implementation, would query actual NOAA weather data
        # For now, return synthetic data structure
        date_range = []
        current = start_date
        while current <= end_date:
            date_range.append(current)
            current += timedelta(hours=1)

        # Generate synthetic weather data
        import numpy as np
        np.random.seed(42)  # For reproducible results

        weather_data = {
            "timestamps": [dt.isoformat() for dt in date_range],
            "geography": geography,
            "temperature": 15 + 10 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) + np.random.normal(0, 2, len(date_range)),
            "humidity": 50 + 30 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) + np.random.normal(0, 10, len(date_range)),
            "wind_speed": 5 + np.random.exponential(2, len(date_range)),
            "solar_irradiance": np.maximum(0, 800 * np.sin(np.pi * np.arange(len(date_range)) / 12) + np.random.normal(0, 100, len(date_range))),
        }

        return weather_data

    async def _get_load_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> Dict[str, Any]:
        """Get load data for the specified time period and geography."""
        # In real implementation, would query actual ISO load data
        date_range = []
        current = start_date
        while current <= end_date:
            date_range.append(current)
            current += timedelta(hours=1)

        # Generate synthetic load data with daily and weekly patterns
        import numpy as np
        np.random.seed(43)

        base_load = 1000  # Base load in MW

        load_data = {
            "timestamps": [dt.isoformat() for dt in date_range],
            "geography": geography,
            "load_mw": base_load * (
                0.7 + 0.6 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) +  # Daily pattern
                0.2 * (1 if np.array([dt.weekday for dt in date_range]) < 5 else 0.5) +  # Weekly pattern
                np.random.normal(0, 0.05, len(date_range))  # Random variation
            ),
        }

        return load_data

    async def _get_price_data(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str
    ) -> Dict[str, Any]:
        """Get price data for the specified time period and geography."""
        # In real implementation, would query actual ISO LMP data
        date_range = []
        current = start_date
        while current <= end_date:
            date_range.append(current)
            current += timedelta(hours=1)

        # Generate synthetic price data
        import numpy as np
        np.random.seed(44)

        base_price = 50  # Base price in $/MWh

        price_data = {
            "timestamps": [dt.isoformat() for dt in date_range],
            "geography": geography,
            "lmp_price": base_price * (
                0.8 + 0.4 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) +  # Daily pattern
                0.3 * (1 if np.array([dt.weekday for dt in date_range]) < 5 else 0.7) +  # Weekly pattern
                np.random.normal(0, 0.1, len(date_range))  # Random variation
            ),
        }

        return price_data

    async def _join_asset_data(
        self,
        weather_data: Dict[str, Any],
        load_data: Dict[str, Any],
        price_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Join weather, load, and price data on timestamp and geography."""
        # In real implementation, would perform proper temporal joins
        # For now, create a combined structure

        # Ensure all datasets have the same timestamps
        timestamps = weather_data["timestamps"]
        if load_data["timestamps"] != timestamps or price_data["timestamps"] != timestamps:
            # Would need to align timestamps in real implementation
            pass

        joined_data = {
            "timestamps": timestamps,
            "geography": weather_data["geography"],
            "weather": {
                "temperature": weather_data["temperature"],
                "humidity": weather_data["humidity"],
                "wind_speed": weather_data["wind_speed"],
                "solar_irradiance": weather_data["solar_irradiance"]
            },
            "load": {
                "load_mw": load_data["load_mw"]
            },
            "price": {
                "lmp_price": price_data["lmp_price"]
            }
        }

        return joined_data

    async def _create_features_from_joined_data(
        self,
        joined_data: Dict[str, Any],
        feature_types: List[str]
    ) -> Dict[str, Any]:
        """Create features from joined cross-asset data."""
        features = {}

        if "weather" in feature_types:
            features.update(await self._add_weather_features(joined_data))

        if "load" in feature_types:
            features.update(await self._add_load_features(joined_data))

        if "price" in feature_types:
            features.update(await self._add_price_features(joined_data))

        if "cross_asset" in feature_types:
            features.update(await self._add_cross_asset_features(joined_data))

        return features

    async def _add_weather_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add derived weather features."""
        weather = data["weather"]
        features = {}

        # Temperature-related features
        features["temp_squared"] = [t**2 for t in weather["temperature"]]
        features["temp_cubed"] = [t**3 for t in weather["temperature"]]

        # Weather indicators
        features["is_hot"] = [1 if t > 25 else 0 for t in weather["temperature"]]
        features["is_cold"] = [1 if t < 5 else 0 for t in weather["temperature"]]
        features["high_humidity"] = [1 if h > 70 else 0 for h in weather["humidity"]]

        # Weather changes (simplified)
        temp_values = weather["temperature"]
        features["temp_change_1h"] = [0] + [temp_values[i] - temp_values[i-1] for i in range(1, len(temp_values))]

        # Wind power potential
        features["wind_power_potential"] = [
            0.5 * ws**3 * 1.225 for ws in weather["wind_speed"]  # Air density 1.225 kg/m³
        ]

        return features

    async def _add_load_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add derived load features."""
        load = data["load"]
        features = {}

        load_values = load["load_mw"]

        # Load changes
        features["load_change_1h"] = [0] + [load_values[i] - load_values[i-1] for i in range(1, len(load_values))]
        features["load_change_3h"] = [0, 0] + [load_values[i] - load_values[i-3] for i in range(3, len(load_values))]

        # Rolling statistics (simplified)
        window_sizes = [3, 6, 24, 168]  # 3h, 6h, 1d, 1w
        for window in window_sizes:
            if len(load_values) > window:
                # Simplified rolling mean (in real implementation, would use pandas)
                rolling_means = []
                for i in range(len(load_values)):
                    if i < window:
                        rolling_means.append(sum(load_values[:i+1]) / (i+1))
                    else:
                        rolling_means.append(sum(load_values[i-window+1:i+1]) / window)
                features[f"load_mean_{window}h"] = rolling_means

        # Peak indicators
        features["is_peak_hour"] = [
            1 if 17 <= datetime.fromisoformat(ts).hour <= 20 else 0
            for ts in data["timestamps"]
        ]
        features["is_weekend"] = [
            1 if datetime.fromisoformat(ts).weekday() >= 5 else 0
            for ts in data["timestamps"]
        ]

        return features

    async def _add_price_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add derived price features."""
        price = data["price"]
        features = {}

        price_values = price["lmp_price"]

        # Price changes
        features["price_change_1h"] = [0] + [price_values[i] - price_values[i-1] for i in range(1, len(price_values))]

        # Price volatility (simplified)
        window = 24
        if len(price_values) > window:
            rolling_stds = []
            for i in range(len(price_values)):
                if i < window:
                    # Use available data
                    window_data = price_values[:i+1]
                    mean_val = sum(window_data) / len(window_data)
                    variance = sum((x - mean_val)**2 for x in window_data) / len(window_data)
                    rolling_stds.append(variance**0.5)
                else:
                    window_data = price_values[i-window+1:i+1]
                    mean_val = sum(window_data) / window
                    variance = sum((x - mean_val)**2 for x in window_data) / window
                    rolling_stds.append(variance**0.5)
            features["price_volatility_24h"] = rolling_stds

        # Price spikes (simplified)
        if "price_volatility_24h" in features and "load_mean_24h" in features:
            # This would be more sophisticated in real implementation
            features["price_spike"] = [
                1 if abs(price_values[i]) > 2 * features["price_volatility_24h"][i] else 0
                for i in range(len(price_values))
            ]

        return features

    async def _add_cross_asset_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add cross-asset derived features."""
        features = {}

        weather = data["weather"]
        load = data["load"]
        price = data["price"]

        # Weather-load relationships (simplified correlations)
        temp_values = weather["temperature"]
        load_values = load["load_mw"]

        # Simple correlation approximation
        if len(temp_values) > 1:
            temp_mean = sum(temp_values) / len(temp_values)
            load_mean = sum(load_values) / len(load_values)

            temp_var = sum((t - temp_mean)**2 for t in temp_values) / len(temp_values)
            load_var = sum((l - load_mean)**2 for l in load_values) / len(load_values)

            if temp_var > 0 and load_var > 0:
                covariance = sum((temp_values[i] - temp_mean) * (load_values[i] - load_mean) for i in range(len(temp_values))) / len(temp_values)
                correlation = covariance / (temp_var**0.5 * load_var**0.5)
                features["temp_load_correlation_24h"] = [correlation] * len(temp_values)

        # Load-price relationships
        if len(load_values) > 1 and len(price["lmp_price"]) > 1:
            load_mean = sum(load_values) / len(load_values)
            price_mean = sum(price["lmp_price"]) / len(price["lmp_price"])

            load_var = sum((l - load_mean)**2 for l in load_values) / len(load_values)
            price_var = sum((p - price_mean)**2 for p in price["lmp_price"]) / len(price["lmp_price"])

            if load_var > 0 and price_var > 0:
                covariance = sum((load_values[i] - load_mean) * (price["lmp_price"][i] - price_mean) for i in range(len(load_values))) / len(load_values)
                correlation = covariance / (load_var**0.5 * price_var**0.5)
                features["load_price_correlation_24h"] = [correlation] * len(load_values)

        # Composite indicators
        if "temp_load_correlation_24h" in features and "load_price_correlation_24h" in features:
            features["weather_sensitivity_index"] = [
                (features["temp_load_correlation_24h"][i] + features["load_price_correlation_24h"][i]) / 2
                for i in range(len(temp_values))
            ]

            features["price_elasticity_index"] = features["load_price_correlation_24h"]

        return features

    async def get_features_for_modeling(
        self,
        start_date: datetime,
        end_date: datetime,
        geography: str = "US",
        target_variable: str = "lmp_price",
        feature_list: Optional[List[str]] = None,
        scenario_id: Optional[str] = None
    ) -> Tuple[Dict[str, Any], List[float]]:
        """Get features and target for machine learning modeling.

        Args:
            start_date: Start date for modeling data
            end_date: End date for modeling data
            geography: Geographic scope
            target_variable: Variable to predict
            feature_list: Specific features to include
            scenario_id: Optional scenario ID

        Returns:
            Tuple of (features_dict, target_values)
        """
        # Get cross-asset features
        features = await self.create_cross_asset_features(
            start_date, end_date, geography, scenario_id=scenario_id
        )

        # Select features
        if feature_list is None:
            # Default feature selection
            feature_list = [
                "temperature", "humidity", "wind_speed",
                "load_mw", "load_change_1h", "load_change_24h",
                "price_change_1h", "price_volatility_24h",
                "temp_load_correlation_24h", "load_price_correlation_24h",
                "is_peak_hour", "is_weekend"
            ]

        # Prepare X and y
        available_features = [col for col in feature_list if col in features]
        X = {feature: features[feature] for feature in available_features}

        # Get target variable
        if target_variable not in features:
            raise ValueError(f"Target variable {target_variable} not found in features")

        y = features[target_variable]

        return X, y

    async def save_feature_set(
        self,
        feature_set_id: str,
        features: Dict[str, Any],
        geography: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Save a feature set for reuse.

        Args:
            feature_set_id: Unique identifier for the feature set
            features: Feature data to save
            geography: Geographic scope
            metadata: Optional metadata about the feature set

        Returns:
            True if saved successfully
        """
        # Add metadata
        features["feature_set_metadata"] = {
            "feature_set_id": feature_set_id,
            "geography": geography,
            "created_at": datetime.utcnow().isoformat(),
            "created_by": "feature_store_service",
            **(metadata or {})
        }

        # Save to DAO
        return await self.dao.save_features(
            features,
            feature_set_id,
            geography,
            datetime.utcnow()
        )

    async def load_feature_set(
        self,
        feature_set_id: str,
        geography: str = "US"
    ) -> Optional[Dict[str, Any]]:
        """Load a previously saved feature set.

        Args:
            feature_set_id: Feature set identifier
            geography: Geographic scope

        Returns:
            Feature set data or None if not found
        """
        return await self.dao.load_features(feature_set_id, geography)

    def get_feature_definition(self, feature_name: str) -> Optional[FeatureDefinition]:
        """Get definition of a feature."""
        return self.feature_definitions.get(feature_name)

    def list_feature_definitions(self) -> Dict[str, FeatureDefinition]:
        """List all feature definitions."""
        return self.feature_definitions.copy()

    def list_available_features(self) -> List[str]:
        """List all available features."""
        return list(self.feature_definitions.keys())


# Global feature store service instance
_feature_store_service: Optional[FeatureStoreService] = None


def get_feature_store_service(config: Optional[FeatureConfig] = None) -> FeatureStoreService:
    """Get the global feature store service instance.

    Args:
        config: Optional feature store configuration

    Returns:
        Feature store service instance
    """
    global _feature_store_service

    if _feature_store_service is None:
        if config is None:
            config = FeatureConfig()

        _feature_store_service = FeatureStoreService(config)

    return _feature_store_service


# Convenience functions for common operations
async def get_features_for_scenario(
    scenario_id: str,
    curve_families: List[str],
    start_date: datetime,
    end_date: datetime,
    geography: str = "US"
) -> Dict[str, Any]:
    """Get features specifically for scenario modeling.

    Args:
        scenario_id: Scenario identifier
        curve_families: List of curve families involved
        start_date: Start date for features
        end_date: End date for features
        geography: Geographic scope

    Returns:
        Features dictionary
    """
    service = get_feature_store_service()

    # Determine feature types based on scenario
    feature_types = []
    if "weather" in curve_families or "renewable" in curve_families:
        feature_types.append("weather")
    if "load" in curve_families or "demand" in curve_families:
        feature_types.append("load")
    if "price" in curve_families:
        feature_types.append("price")

    if not feature_types:
        feature_types = ["weather", "load", "price", "cross_asset"]

    features = await service.create_cross_asset_features(
        start_date, end_date, geography, feature_types, scenario_id
    )

    # Add scenario-specific features
    features["scenario_metadata"] = {
        "scenario_id": scenario_id,
        "curve_families": curve_families,
        "geography": geography
    }

    return features


async def get_training_features(
    target_variable: str = "lmp_price",
    feature_list: Optional[List[str]] = None,
    start_date: datetime = None,
    end_date: datetime = None,
    geography: str = "US"
) -> Tuple[Dict[str, Any], List[float]]:
    """Get features and target for model training.

    Args:
        target_variable: Variable to predict
        feature_list: Features to include
        start_date: Start date (defaults to 1 year ago)
        end_date: End date (defaults to now)
        geography: Geographic scope

    Returns:
        Tuple of (features, target)
    """
    service = get_feature_store_service()

    if start_date is None:
        start_date = datetime.utcnow() - timedelta(days=365)
    if end_date is None:
        end_date = datetime.utcnow()

    return await service.get_features_for_modeling(
        start_date, end_date, geography, target_variable, feature_list
    )
