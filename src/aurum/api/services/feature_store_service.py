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
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from uuid import uuid4

import pandas as pd
from pandas import DataFrame
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

    async def create(self, entity: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        raise NotImplementedError("FeatureStoreDAO does not support create operations")

    async def get_by_id(self, id: str) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        raise NotImplementedError("FeatureStoreDAO does not support get_by_id operations")

    async def update(self, id: str, entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        raise NotImplementedError("FeatureStoreDAO does not support update operations")

    async def delete(self, id: str) -> bool:  # type: ignore[override]
        raise NotImplementedError("FeatureStoreDAO does not support delete operations")

    async def list(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> List[Dict[str, Any]]:  # type: ignore[override]
        raise NotImplementedError("FeatureStoreDAO does not support list operations")

    async def _connect(self) -> None:
        """Connect to Trino for feature store."""
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        await super()._disconnect()

    async def get_feature_history(
        self,
        feature_name: str,
        start_date: datetime,
        end_date: datetime,
        geography: str = "US"
    ) -> List[Dict[str, Any]]:
        """Get historical feature data."""
        query = f"""
        SELECT timestamp, value
        FROM {self.table_name}
        WHERE feature_name = ?
          AND geography = ?
          AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp
        """

        parameters = {
            "feature_name": feature_name,
            "geography": geography,
            "start_date": start_date,
            "end_date": end_date
        }

        return await self._execute_trino_query(query, parameters)

    async def save_feature_set(
        self,
        feature_set_id: str,
        features: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> bool:
        """Save a complete feature set with metadata."""
        try:
            # Flatten features for storage
            feature_records = []
            for feature_name, feature_values in features.items():
                if isinstance(feature_values, (list, pd.Series)):
                    if isinstance(feature_values, list):
                        values = feature_values
                    else:
                        values = feature_values.tolist()

                    for i, value in enumerate(values):
                        if i < len(metadata.get("timestamps", [])):
                            timestamp = metadata["timestamps"][i]
                            feature_records.append({
                                "feature_set_id": feature_set_id,
                                "feature_name": feature_name,
                                "timestamp": timestamp,
                                "value": value,
                                "geography": metadata.get("geography", "US")
                            })

            # Insert feature records (simplified implementation)
            for record in feature_records:
                query = """
                INSERT INTO market_data.features
                (feature_set_id, feature_name, timestamp, value, geography)
                VALUES (?, ?, ?, ?, ?)
                """
                await self._execute_trino_query(query, record)

            return True
        except Exception as e:
            log_structured("error", "feature_set_save_failed",
                         feature_set_id=feature_set_id, error=str(e))
            return False

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute Trino query for features."""
        try:
            return await super()._execute_trino_query(query, parameters)
        except Exception as e:
            log_structured("error", "feature_store_query_failed",
                         query=query[:100], error=str(e))
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
        log_structured(
            "info",
            "feature_store_service_initialized",
            geography=config.geography,
            feature_version=config.feature_version
        )

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

            engineered = await self._build_engineered_feature_sets(
                features_data,
                time_window_columns=[col for col in ("load_mw", "lmp_price", "temperature") if col in features_data],
                lag_columns=[
                    col for col in ("load_mw", "lmp_price", "temperature", "humidity", "wind_speed")
                    if col in features_data
                ],
                time_window_sizes=[3, 24, 168],
                lag_periods=[1, 24, 168],
                aggregation_methods=("mean", "std"),
                include_trends=True
            )

            seasonal_features = engineered.get("seasonal", {}).copy()
            seasonal_features.pop("is_weekend", None)

            features_data.update(engineered.get("time_windows", {}))
            features_data.update(engineered.get("lags", {}))
            features_data.update({key: value for key, value in seasonal_features.items() if key not in features_data})
            features_data["engineered_features"] = engineered

            # Add metadata
            features_data["metadata"] = {
                "feature_version": self.config.feature_version,
                "geography": geography,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "feature_types": feature_types,
                "scenario_id": scenario_id,
                "generated_at": datetime.utcnow().isoformat(),
                "generated_by": "feature_store_service",
                "timestamps": [
                    ts.isoformat() if isinstance(ts, datetime) else str(ts)
                    for ts in features_data.get("timestamp", [])
                ]
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

        weekdays = np.array([dt.weekday() for dt in date_range])
        weekday_factor = np.where(weekdays < 5, 1.0, 0.5)

        load_data = {
            "timestamps": [dt.isoformat() for dt in date_range],
            "geography": geography,
            "load_mw": base_load * (
                0.7 + 0.6 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) +  # Daily pattern
                0.2 * weekday_factor +  # Weekly pattern
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

        weekdays = np.array([dt.weekday() for dt in date_range])
        weekday_price_factor = np.where(weekdays < 5, 1.0, 0.7)

        price_data = {
            "timestamps": [dt.isoformat() for dt in date_range],
            "geography": geography,
            "lmp_price": base_price * (
                0.8 + 0.4 * np.sin(2 * np.pi * np.arange(len(date_range)) / 24) +  # Daily pattern
                0.3 * weekday_price_factor +  # Weekly pattern
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
        frame = self._build_base_dataframe(joined_data)

        timestamps = frame["timestamp"].tolist()

        features: Dict[str, Any] = {
            "timestamp": timestamps,
            "timestamps": [
                ts.isoformat() if isinstance(ts, datetime) else str(ts)
                for ts in timestamps
            ],
        }

        for column in frame.columns:
            if column == "timestamp":
                continue
            features[column] = frame[column].tolist()

        if "weather" in feature_types:
            features.update(await self._add_weather_features(frame))

        if "load" in feature_types:
            features.update(await self._add_load_features(frame))

        if "price" in feature_types:
            features.update(await self._add_price_features(frame))

        if "cross_asset" in feature_types:
            features.update(await self._add_cross_asset_features(frame))

        return features

    def _build_base_dataframe(self, joined_data: Dict[str, Any]) -> DataFrame:
        """Create a normalized dataframe from joined asset data."""
        timestamps = pd.to_datetime(joined_data["timestamps"])  # type: ignore[arg-type]

        frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "temperature": joined_data["weather"]["temperature"],
                "humidity": joined_data["weather"]["humidity"],
                "wind_speed": joined_data["weather"]["wind_speed"],
                "solar_irradiance": joined_data["weather"]["solar_irradiance"],
                "load_mw": joined_data["load"]["load_mw"],
                "lmp_price": joined_data["price"]["lmp_price"],
            }
        )

        frame = frame.sort_values("timestamp").reset_index(drop=True)
        return frame

    async def _add_weather_features(self, frame: DataFrame) -> Dict[str, Any]:
        """Add derived weather features."""
        features: Dict[str, Any] = {}

        temperature = frame["temperature"].tolist()
        humidity = frame["humidity"].tolist()
        wind_speed = frame["wind_speed"].tolist()

        features["temp_squared"] = [t ** 2 for t in temperature]
        features["temp_cubed"] = [t ** 3 for t in temperature]
        features["is_hot"] = [1 if t > 25 else 0 for t in temperature]
        features["is_cold"] = [1 if t < 5 else 0 for t in temperature]
        features["high_humidity"] = [1 if h > 70 else 0 for h in humidity]
        features["temp_change_1h"] = [0] + [temperature[i] - temperature[i - 1] for i in range(1, len(temperature))]
        features["wind_power_potential"] = [0.5 * ws ** 3 * 1.225 for ws in wind_speed]

        return features

    async def _add_load_features(self, frame: DataFrame) -> Dict[str, Any]:
        """Add derived load features."""
        features: Dict[str, Any] = {}

        load_values = frame["load_mw"].tolist()
        timestamps = frame["timestamp"].tolist()

        features["load_change_1h"] = [0] + [load_values[i] - load_values[i - 1] for i in range(1, len(load_values))]
        features["load_change_3h"] = [0] * min(3, len(load_values)) + [
            load_values[i] - load_values[i - 3]
            for i in range(3, len(load_values))
        ]
        features["load_change_24h"] = [0] * min(24, len(load_values)) + [
            load_values[i] - load_values[i - 24]
            for i in range(24, len(load_values))
        ]

        window_sizes = [3, 6, 24, 168]
        for window in window_sizes:
            if len(load_values) > window:
                rolling_means = []
                for i in range(len(load_values)):
                    if i < window:
                        rolling_means.append(sum(load_values[: i + 1]) / (i + 1))
                    else:
                        rolling_means.append(sum(load_values[i - window + 1 : i + 1]) / window)
                features[f"load_mean_{window}h"] = rolling_means

        features["is_peak_hour"] = [1 if ts.hour in (17, 18, 19, 20) else 0 for ts in timestamps]
        features["is_weekend"] = [1 if ts.weekday() >= 5 else 0 for ts in timestamps]

        return features

    async def _add_price_features(self, frame: DataFrame) -> Dict[str, Any]:
        """Add derived price features."""
        features: Dict[str, Any] = {}

        price_values = frame["lmp_price"].tolist()

        features["price_change_1h"] = [0] + [price_values[i] - price_values[i - 1] for i in range(1, len(price_values))]

        window = 24
        if len(price_values) > window:
            rolling_stds = []
            for i in range(len(price_values)):
                if i < window:
                    window_data = price_values[: i + 1]
                    mean_val = sum(window_data) / len(window_data)
                    variance = sum((x - mean_val) ** 2 for x in window_data) / len(window_data)
                    rolling_stds.append(variance ** 0.5)
                else:
                    window_data = price_values[i - window + 1 : i + 1]
                    mean_val = sum(window_data) / window
                    variance = sum((x - mean_val) ** 2 for x in window_data) / window
                    rolling_stds.append(variance ** 0.5)
            features["price_volatility_24h"] = rolling_stds

        if "price_volatility_24h" in features and "load_mean_24h" in features:
            features["price_spike"] = [
                1 if abs(price_values[i]) > 2 * features["price_volatility_24h"][i] else 0
                for i in range(len(price_values))
            ]

        return features

    async def _add_cross_asset_features(self, frame: DataFrame) -> Dict[str, Any]:
        """Add cross-asset derived features."""
        features: Dict[str, Any] = {}

        temp_values = frame["temperature"].tolist()
        load_values = frame["load_mw"].tolist()
        price_values = frame["lmp_price"].tolist()

        if len(temp_values) > 1:
            temp_series = pd.Series(temp_values)
            load_series = pd.Series(load_values)
            correlation = temp_series.corr(load_series)
            if pd.notna(correlation):
                features["temp_load_correlation_24h"] = [float(correlation)] * len(temp_series)

        if len(load_values) > 1 and len(price_values) > 1:
            load_series = pd.Series(load_values)
            price_series = pd.Series(price_values)
            correlation = load_series.corr(price_series)
            if pd.notna(correlation):
                features["load_price_correlation_24h"] = [float(correlation)] * len(load_series)

        if "temp_load_correlation_24h" in features and "load_price_correlation_24h" in features:
            combined = [
                (features["temp_load_correlation_24h"][i] + features["load_price_correlation_24h"][i]) / 2
                for i in range(len(features["temp_load_correlation_24h"]))
            ]
            features["weather_sensitivity_index"] = combined
            features["price_elasticity_index"] = features["load_price_correlation_24h"]

        return features

    async def create_time_window_features(
        self,
        base_features: Union[DataFrame, Dict[str, Any]],
        window_sizes: Sequence[int],
        aggregation_methods: Sequence[str] = ("mean", "std", "min", "max"),
        columns: Optional[Sequence[str]] = None,
        min_periods: int = 1
    ) -> Dict[str, Any]:
        """Create time-window aggregation features."""
        df = self._ensure_dataframe(base_features)

        if columns is None:
            numeric_columns = [
                col for col in df.columns
                if pd.api.types.is_numeric_dtype(df[col])
            ]
        else:
            numeric_columns = [col for col in columns if col in df.columns]

        time_window_features: Dict[str, List[Optional[float]]] = {}

        for feature_name in numeric_columns:
            rolling_source = df[feature_name]
            for window in window_sizes:
                rolling_window = rolling_source.rolling(window=window, min_periods=min_periods)
                for method in aggregation_methods:
                    feature_key = f"{feature_name}_rolling_{window}_{method}"
                    if method == "mean":
                        series = rolling_window.mean()
                    elif method == "std":
                        series = rolling_window.std()
                    elif method == "min":
                        series = rolling_window.min()
                    elif method == "max":
                        series = rolling_window.max()
                    elif method == "sum":
                        series = rolling_window.sum()
                    else:
                        continue

                    time_window_features[feature_key] = series.tolist()

        return time_window_features

    async def create_lag_features(
        self,
        base_features: Union[DataFrame, Dict[str, Any]],
        lag_periods: Sequence[int],
        fill_method: str = "forward",
        columns: Optional[Sequence[str]] = None
    ) -> Dict[str, Any]:
        """Create lag features for time series data."""
        df = self._ensure_dataframe(base_features)

        if columns is None:
            candidate_columns = [
                col for col in df.columns
                if pd.api.types.is_numeric_dtype(df[col])
            ]
        else:
            candidate_columns = [col for col in columns if col in df.columns]

        lag_features: Dict[str, List[Optional[float]]] = {}

        for feature_name in candidate_columns:
            series = df[feature_name]
            for lag in lag_periods:
                lag_series = series.shift(lag)

                if fill_method == "forward":
                    lag_series = lag_series.ffill()
                elif fill_method == "backward":
                    lag_series = lag_series.bfill()
                elif fill_method == "interpolate":
                    lag_series = lag_series.interpolate()

                lag_key = f"{feature_name}_lag_{lag}"
                lag_features[lag_key] = lag_series.tolist()

        return lag_features

    def _ensure_dataframe(self, base_features: Union[DataFrame, Dict[str, Any]]) -> DataFrame:
        """Convert feature collection into a timestamp-indexed dataframe."""
        if isinstance(base_features, pd.DataFrame):
            df = base_features.copy()
        elif isinstance(base_features, dict):
            df = pd.DataFrame(base_features)
        else:
            raise TypeError("base_features must be a pandas DataFrame or dict")

        if "timestamp" in df.columns:
            timestamp_column = "timestamp"
        elif "timestamps" in df.columns:
            timestamp_column = "timestamps"
        else:
            raise ValueError("base_features must include a 'timestamp' column")

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df[timestamp_column])
        if "timestamps" in df.columns and timestamp_column != "timestamps":
            df = df.drop(columns=["timestamps"])

        df = df.sort_values("timestamp").reset_index(drop=True)
        df = df.set_index("timestamp")

        return df

    async def _build_engineered_feature_sets(
        self,
        feature_map: Dict[str, Any],
        time_window_columns: Sequence[str],
        lag_columns: Sequence[str],
        time_window_sizes: Sequence[int],
        lag_periods: Sequence[int],
        aggregation_methods: Sequence[str] = ("mean", "std"),
        include_trends: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Generate standard engineered feature groups for downstream consumers."""
        sanitized = {
            key: value
            for key, value in feature_map.items()
            if key not in {"metadata", "feature_set_metadata", "engineered_features"}
        }

        try:
            time_windows = await self.create_time_window_features(
                sanitized,
                window_sizes=time_window_sizes,
                aggregation_methods=aggregation_methods,
                columns=time_window_columns,
                min_periods=1
            )
        except ValueError:
            time_windows = {}

        try:
            lag_features = await self.create_lag_features(
                sanitized,
                lag_periods=lag_periods,
                columns=lag_columns,
                fill_method="forward"
            )
        except ValueError:
            lag_features = {}

        seasonal_features = await self.create_seasonal_features(
            [pd.to_datetime(ts) for ts in sanitized.get("timestamp", [])],
            include_trends=include_trends
        )

        return {
            "time_windows": time_windows,
            "lags": lag_features,
            "seasonal": seasonal_features,
        }

    async def create_seasonal_features(
        self,
        timestamps: List[datetime],
        include_trends: bool = True,
        include_seasonal_decomposition: bool = False
    ) -> Dict[str, Any]:
        """Create seasonal and trend features from timestamps.

        Args:
            timestamps: List of datetime objects
            include_trends: Whether to include trend features
            include_seasonal_decomposition: Whether to include seasonal decomposition

        Returns:
            Dictionary with seasonal features
        """
        seasonal_features = {}

        if not timestamps:
            return seasonal_features

        ts_series = pd.to_datetime(pd.Series(timestamps))

        seasonal_features['hour_of_day'] = ts_series.dt.hour.tolist()
        seasonal_features['day_of_week'] = ts_series.dt.dayofweek.tolist()
        seasonal_features['month_of_year'] = ts_series.dt.month.tolist()
        seasonal_features['quarter'] = ts_series.dt.quarter.tolist()
        seasonal_features['is_weekend'] = (ts_series.dt.dayofweek >= 5).astype(int).tolist()
        seasonal_features['is_holiday'] = [0] * len(ts_series)  # Placeholder for holiday detection

        if include_trends:
            numeric_ts = ts_series.view('int64') // 10**9
            seasonal_features['trend'] = numeric_ts.tolist()

        if include_seasonal_decomposition:
            try:
                # Simple seasonal decomposition (requires statsmodels)
                from statsmodels.tsa.seasonal import seasonal_decompose

                # Create dummy series for decomposition
                dummy_values = pd.Series([1.0] * len(ts_series), index=ts_series)
                decomposition = seasonal_decompose(dummy_values, period=24)  # Assuming hourly data

                seasonal_features['seasonal_component'] = decomposition.seasonal.tolist()
                seasonal_features['trend_component'] = decomposition.trend.tolist()
                seasonal_features['residual'] = decomposition.resid.tolist()

            except ImportError:
                log_structured("warning", "statsmodels_not_available",
                             message="Seasonal decomposition requires statsmodels")

        return seasonal_features

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
        features = await self.create_cross_asset_features(
            start_date, end_date, geography, scenario_id=scenario_id
        )

        engineered_groups = features.get("engineered_features")
        if not engineered_groups:
            engineered_groups = await self._build_engineered_feature_sets(
                features,
                time_window_columns=["load_mw", "lmp_price", "temperature"],
                lag_columns=["load_mw", "lmp_price", "temperature", "humidity", "wind_speed"],
                time_window_sizes=[3, 24, 168],
                lag_periods=[1, 24, 168],
                aggregation_methods=("mean", "std"),
                include_trends=True
            )
            features.update(engineered_groups.get("time_windows", {}))
            features.update(engineered_groups.get("lags", {}))
            seasonal_features = engineered_groups.get("seasonal", {}).copy()
            seasonal_features.pop("is_weekend", None)
            features.update({key: value for key, value in seasonal_features.items() if key not in features})
            features["engineered_features"] = engineered_groups

        seasonal_features = engineered_groups.get("seasonal", {})
        seasonal_features.pop("is_weekend", None)

        # Select features
        if feature_list is None:
            # Default feature selection
            feature_list = [
                "temperature", "humidity", "wind_speed",
                "load_mw", "load_change_1h", "load_change_24h",
                "price_change_1h", "price_volatility_24h",
                "temp_load_correlation_24h", "load_price_correlation_24h",
                "is_peak_hour", "is_weekend",
                "load_mw_rolling_24_mean", "lmp_price_rolling_24_mean",
                "temperature_lag_24", "load_mw_lag_24", "lmp_price_lag_1",
                "hour_of_day", "day_of_week"
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

    engineered_groups = features.get("engineered_features")
    if not engineered_groups:
        engineered_groups = await service._build_engineered_feature_sets(
            features,
            time_window_columns=["load_mw", "lmp_price", "temperature"],
            lag_columns=["load_mw", "lmp_price", "temperature", "humidity", "wind_speed"],
            time_window_sizes=[3, 24, 168],
            lag_periods=[1, 24, 168],
            aggregation_methods=("mean", "std"),
            include_trends=True
        )
        features.update(engineered_groups.get("time_windows", {}))
        features.update(engineered_groups.get("lags", {}))
        seasonal_features = engineered_groups.get("seasonal", {}).copy()
        seasonal_features.pop("is_weekend", None)
        features.update({key: value for key, value in seasonal_features.items() if key not in features})
        features["engineered_features"] = engineered_groups

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


async def create_time_window_features(
    base_features: Union[DataFrame, Dict[str, Any]],
    window_sizes: Sequence[int],
    aggregation_methods: Sequence[str] = ("mean", "std", "min", "max"),
    columns: Optional[Sequence[str]] = None,
    min_periods: int = 1
) -> Dict[str, Any]:
    """Module-level compatibility wrapper for time-window features."""
    service = get_feature_store_service()
    return await service.create_time_window_features(
        base_features,
        window_sizes,
        aggregation_methods,
        columns,
        min_periods
    )


async def create_lag_features(
    base_features: Union[DataFrame, Dict[str, Any]],
    lag_periods: Sequence[int],
    fill_method: str = "forward",
    columns: Optional[Sequence[str]] = None
) -> Dict[str, Any]:
    """Module-level compatibility wrapper for lag features."""
    service = get_feature_store_service()
    return await service.create_lag_features(
        base_features,
        lag_periods,
        fill_method,
        columns
    )


async def create_seasonal_features(
    timestamps: List[datetime],
    include_trends: bool = True,
    include_seasonal_decomposition: bool = False
) -> Dict[str, Any]:
    """Module-level compatibility wrapper for seasonal feature extraction."""
    service = get_feature_store_service()
    return await service.create_seasonal_features(
        timestamps,
        include_trends,
        include_seasonal_decomposition
    )
