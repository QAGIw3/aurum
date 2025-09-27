"""Phase 3: Enhanced forecasting capabilities for >85% accuracy.

This module extends the existing forecasting framework with advanced techniques
to achieve the Phase 3 requirement of >85% forecasting accuracy.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured
from .forecasting import ForecastConfig, ForecastResult, ForecastingEngine


logger = logging.getLogger(__name__)


class EnhancedForecastConfig(ForecastConfig):
    """Enhanced configuration with advanced forecasting options."""
    ensemble_method: str = Field(default="weighted_average", description="Ensemble combination method")
    adaptive_parameters: bool = Field(default=True, description="Enable adaptive parameter tuning")
    feature_engineering: bool = Field(default=True, description="Enable advanced feature engineering")
    outlier_detection: bool = Field(default=True, description="Enable outlier detection and handling")
    regime_detection: bool = Field(default=True, description="Enable regime change detection")
    uncertainty_quantification: bool = Field(default=True, description="Enable uncertainty quantification")
    target_accuracy: float = Field(default=0.85, ge=0.0, le=1.0, description="Target forecast accuracy")


@dataclass
class AccuracyMetrics:
    """Enhanced accuracy metrics for forecast evaluation."""
    mape: float  # Mean Absolute Percentage Error
    smape: float  # Symmetric Mean Absolute Percentage Error
    mase: float  # Mean Absolute Scaled Error
    directional_accuracy: float  # Percentage of correct direction predictions
    prediction_interval_coverage: float  # Coverage of prediction intervals
    forecast_bias: float  # Bias in forecasts
    accuracy_score: float  # Overall accuracy score (0-1)


class EnhancedForecastResult(ForecastResult):
    """Enhanced forecast result with additional metrics and metadata."""
    accuracy_metrics: Optional[AccuracyMetrics] = None
    model_confidence: float = Field(default=0.0, description="Model confidence score")
    regime_indicators: Optional[Dict[str, Any]] = None
    feature_importance: Optional[Dict[str, float]] = None
    ensemble_weights: Optional[Dict[str, float]] = None


class EnhancedForecastingEngine(ForecastingEngine):
    """Enhanced forecasting engine with advanced capabilities for >85% accuracy."""
    
    def __init__(self):
        super().__init__()
        self._ensemble_models = {}
        self._feature_cache = {}
        self._regime_detector = RegimeDetector()
        self._outlier_detector = OutlierDetector()
    
    async def enhanced_forecast(
        self,
        data: Union[pd.Series, np.ndarray],
        config: EnhancedForecastConfig,
        validation_data: Optional[Union[pd.Series, np.ndarray]] = None
    ) -> EnhancedForecastResult:
        """Generate enhanced forecast with advanced accuracy techniques."""
        start_time = datetime.utcnow()
        
        try:
            # Data preprocessing and feature engineering
            processed_data = await self._preprocess_data(data, config)
            
            # Outlier detection and handling
            if config.outlier_detection:
                processed_data = await self._handle_outliers(processed_data)
            
            # Regime detection
            regime_info = None
            if config.regime_detection:
                regime_info = await self._detect_regimes(processed_data)
            
            # Feature engineering
            features = {}
            if config.feature_engineering:
                features = await self._engineer_features(processed_data)
            
            # Generate ensemble forecast
            ensemble_result = await self._generate_ensemble_forecast(
                processed_data, config, features, regime_info
            )
            
            # Calculate accuracy metrics if validation data provided
            accuracy_metrics = None
            if validation_data is not None:
                accuracy_metrics = await self._calculate_enhanced_accuracy(
                    ensemble_result, validation_data
                )
            
            # Calculate model confidence
            model_confidence = await self._calculate_model_confidence(
                ensemble_result, accuracy_metrics
            )
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            await log_structured(
                "enhanced_forecast_completed",
                execution_time=execution_time,
                accuracy_score=accuracy_metrics.accuracy_score if accuracy_metrics else None,
                model_confidence=model_confidence
            )
            
            return EnhancedForecastResult(
                model_type="enhanced_ensemble",
                predictions=ensemble_result.predictions,
                confidence_intervals=ensemble_result.confidence_intervals,
                forecast_dates=ensemble_result.forecast_dates,
                model_params=ensemble_result.model_params,
                training_time=ensemble_result.training_time,
                prediction_time=execution_time,
                accuracy_metrics=accuracy_metrics,
                model_confidence=model_confidence,
                regime_indicators=regime_info,
                feature_importance=features.get("importance", {}),
                ensemble_weights=ensemble_result.model_params.get("ensemble_weights", {})
            )
            
        except Exception as e:
            logger.exception("Enhanced forecasting failed")
            raise RuntimeError(f"Enhanced forecasting failed: {str(e)}")
    
    async def _preprocess_data(
        self, 
        data: Union[pd.Series, np.ndarray], 
        config: EnhancedForecastConfig
    ) -> pd.Series:
        """Advanced data preprocessing."""
        if isinstance(data, np.ndarray):
            data = pd.Series(data)
        
        # Handle missing values with advanced interpolation
        if data.isnull().any():
            data = data.interpolate(method='akima', limit_direction='both')
        
        # Apply smoothing if needed
        if config.seasonality_periods:
            # Apply seasonal adjustment
            data = self._apply_seasonal_adjustment(data, config.seasonality_periods)
        
        return data
    
    async def _handle_outliers(self, data: pd.Series) -> pd.Series:
        """Detect and handle outliers using advanced techniques."""
        outliers = await self._outlier_detector.detect_outliers(data)
        
        if outliers.any():
            # Replace outliers with interpolated values
            data_cleaned = data.copy()
            data_cleaned[outliers] = np.nan
            data_cleaned = data_cleaned.interpolate(method='akima')
            
            await log_structured(
                "outliers_handled",
                outlier_count=outliers.sum(),
                outlier_percentage=float(outliers.sum() / len(data) * 100)
            )
            
            return data_cleaned
        
        return data
    
    async def _detect_regimes(self, data: pd.Series) -> Dict[str, Any]:
        """Detect regime changes in the time series."""
        regime_points = await self._regime_detector.detect_regime_changes(data)
        
        return {
            "regime_change_points": regime_points,
            "current_regime": self._regime_detector.get_current_regime(data),
            "regime_volatility": self._regime_detector.calculate_regime_volatility(data, regime_points)
        }
    
    async def _engineer_features(self, data: pd.Series) -> Dict[str, Any]:
        """Engineer advanced features for forecasting."""
        features = {}
        
        # Technical indicators
        features["sma_12"] = data.rolling(window=12).mean()
        features["sma_24"] = data.rolling(window=24).mean()
        features["ema_12"] = data.ewm(span=12).mean()
        features["volatility"] = data.rolling(window=24).std()
        
        # Trend features
        features["trend"] = self._calculate_trend(data)
        features["momentum"] = data.diff(12)
        
        # Seasonal features
        if len(data) >= 24:
            features["seasonal_decomp"] = self._seasonal_decomposition(data)
        
        # Feature importance calculation (simplified)
        importance = {}
        for feature_name, feature_data in features.items():
            if isinstance(feature_data, pd.Series):
                correlation = abs(data.corr(feature_data))
                importance[feature_name] = correlation if not np.isnan(correlation) else 0.0
        
        features["importance"] = importance
        
        return features
    
    async def _generate_ensemble_forecast(
        self,
        data: pd.Series,
        config: EnhancedForecastConfig,
        features: Dict[str, Any],
        regime_info: Optional[Dict[str, Any]]
    ) -> ForecastResult:
        """Generate ensemble forecast using multiple models."""
        
        # Define ensemble models
        model_types = ["arima", "ets", "xgboost"]
        model_results = {}
        model_weights = {}
        
        # Generate forecasts from individual models
        for model_type in model_types:
            try:
                result = await self.forecast(
                    data, 
                    model_type, 
                    config,
                    include_intervals=True
                )
                model_results[model_type] = result
                
                # Calculate model weight based on historical performance
                weight = await self._calculate_model_weight(model_type, data, config)
                model_weights[model_type] = weight
                
            except Exception as e:
                logger.warning(f"Model {model_type} failed: {str(e)}")
                model_weights[model_type] = 0.0
        
        # Normalize weights
        total_weight = sum(model_weights.values())
        if total_weight > 0:
            model_weights = {k: v/total_weight for k, v in model_weights.items()}
        else:
            # Equal weights if all models failed
            model_weights = {k: 1.0/len(model_types) for k in model_types}
        
        # Combine forecasts using ensemble method
        ensemble_predictions = await self._combine_forecasts(
            model_results, model_weights, config.ensemble_method
        )
        
        # Calculate ensemble confidence intervals
        ensemble_intervals = await self._combine_confidence_intervals(
            model_results, model_weights
        )
        
        return ForecastResult(
            model_type="enhanced_ensemble",
            predictions=ensemble_predictions,
            confidence_intervals=ensemble_intervals,
            forecast_dates=list(model_results.values())[0].forecast_dates if model_results else None,
            model_params={
                "ensemble_weights": model_weights,
                "ensemble_method": config.ensemble_method,
                "component_models": list(model_results.keys())
            },
            training_time=sum(r.training_time for r in model_results.values()),
            prediction_time=0.0  # Will be set by caller
        )
    
    async def _calculate_model_weight(
        self, 
        model_type: str, 
        data: pd.Series, 
        config: EnhancedForecastConfig
    ) -> float:
        """Calculate weight for a model based on historical performance."""
        try:
            # Perform quick backtest to estimate model performance
            if len(data) < 48:  # Need sufficient data for backtesting
                return 1.0
            
            # Use last 25% of data for validation
            split_point = int(len(data) * 0.75)
            train_data = data[:split_point]
            test_data = data[split_point:]
            
            # Quick forecast
            forecast_result = await self.forecast(
                train_data, 
                model_type, 
                config,
                include_intervals=False
            )
            
            # Calculate MAPE on test data
            if len(forecast_result.predictions) >= len(test_data):
                predictions = forecast_result.predictions[:len(test_data)]
                mape = np.mean(np.abs((test_data - predictions) / test_data)) * 100
                
                # Convert MAPE to weight (lower MAPE = higher weight)
                weight = max(0.1, 1.0 - (mape / 100))
                return weight
            
        except Exception as e:
            logger.warning(f"Weight calculation failed for {model_type}: {str(e)}")
        
        return 1.0  # Default weight
    
    async def _combine_forecasts(
        self,
        model_results: Dict[str, ForecastResult],
        weights: Dict[str, float],
        method: str
    ) -> np.ndarray:
        """Combine forecasts from multiple models."""
        if not model_results:
            raise ValueError("No model results to combine")
        
        # Get predictions from all models
        all_predictions = []
        model_weights = []
        
        for model_type, result in model_results.items():
            if result and hasattr(result, 'predictions'):
                all_predictions.append(result.predictions)
                model_weights.append(weights.get(model_type, 0.0))
        
        if not all_predictions:
            raise ValueError("No valid predictions to combine")
        
        # Ensure all predictions have same length
        min_length = min(len(pred) for pred in all_predictions)
        all_predictions = [pred[:min_length] for pred in all_predictions]
        
        predictions_array = np.array(all_predictions)
        weights_array = np.array(model_weights)
        
        if method == "weighted_average":
            # Weighted average
            ensemble_forecast = np.average(predictions_array, axis=0, weights=weights_array)
        elif method == "median":
            # Median combination
            ensemble_forecast = np.median(predictions_array, axis=0)
        elif method == "best_model":
            # Use best performing model
            best_model_idx = np.argmax(weights_array)
            ensemble_forecast = predictions_array[best_model_idx]
        else:
            # Default to simple average
            ensemble_forecast = np.mean(predictions_array, axis=0)
        
        return ensemble_forecast
    
    async def _combine_confidence_intervals(
        self,
        model_results: Dict[str, ForecastResult],
        weights: Dict[str, float]
    ) -> Optional[Tuple[np.ndarray, np.ndarray]]:
        """Combine confidence intervals from multiple models."""
        intervals = []
        model_weights = []
        
        for model_type, result in model_results.items():
            if result and result.confidence_intervals:
                intervals.append(result.confidence_intervals)
                model_weights.append(weights.get(model_type, 0.0))
        
        if not intervals:
            return None
        
        # Combine lower and upper bounds separately
        lower_bounds = [interval[0] for interval in intervals]
        upper_bounds = [interval[1] for interval in intervals]
        
        weights_array = np.array(model_weights)
        combined_lower = np.average(np.array(lower_bounds), axis=0, weights=weights_array)
        combined_upper = np.average(np.array(upper_bounds), axis=0, weights=weights_array)
        
        return (combined_lower, combined_upper)
    
    async def _calculate_enhanced_accuracy(
        self,
        forecast_result: ForecastResult,
        validation_data: Union[pd.Series, np.ndarray]
    ) -> AccuracyMetrics:
        """Calculate enhanced accuracy metrics."""
        if isinstance(validation_data, pd.Series):
            actual = validation_data.values
        else:
            actual = validation_data
        
        predicted = forecast_result.predictions
        
        # Ensure same length
        min_len = min(len(actual), len(predicted))
        actual = actual[:min_len]
        predicted = predicted[:min_len]
        
        # Calculate various accuracy metrics
        mape = np.mean(np.abs((actual - predicted) / actual)) * 100
        smape = np.mean(2 * np.abs(actual - predicted) / (np.abs(actual) + np.abs(predicted))) * 100
        
        # MASE (Mean Absolute Scaled Error)
        naive_forecast = np.roll(actual, 1)[1:]  # Simple naive forecast
        mae = np.mean(np.abs(actual - predicted))
        naive_mae = np.mean(np.abs(actual[1:] - naive_forecast))
        mase = mae / naive_mae if naive_mae != 0 else float('inf')
        
        # Directional accuracy
        actual_direction = np.sign(np.diff(actual))
        predicted_direction = np.sign(np.diff(predicted))
        directional_accuracy = np.mean(actual_direction == predicted_direction) * 100
        
        # Prediction interval coverage (if available)
        pi_coverage = 0.0
        if forecast_result.confidence_intervals:
            lower, upper = forecast_result.confidence_intervals
            lower = lower[:min_len]
            upper = upper[:min_len]
            pi_coverage = np.mean((actual >= lower) & (actual <= upper)) * 100
        
        # Forecast bias
        forecast_bias = np.mean(predicted - actual)
        
        # Overall accuracy score (0-1, higher is better)
        accuracy_score = max(0, 1 - (mape / 100))
        
        return AccuracyMetrics(
            mape=mape,
            smape=smape,
            mase=mase,
            directional_accuracy=directional_accuracy,
            prediction_interval_coverage=pi_coverage,
            forecast_bias=forecast_bias,
            accuracy_score=accuracy_score
        )
    
    async def _calculate_model_confidence(
        self,
        forecast_result: ForecastResult,
        accuracy_metrics: Optional[AccuracyMetrics]
    ) -> float:
        """Calculate model confidence score."""
        confidence_factors = []
        
        # Accuracy-based confidence
        if accuracy_metrics:
            confidence_factors.append(accuracy_metrics.accuracy_score)
            
            # Directional accuracy factor
            dir_accuracy_factor = accuracy_metrics.directional_accuracy / 100
            confidence_factors.append(dir_accuracy_factor)
            
            # Prediction interval coverage factor
            if accuracy_metrics.prediction_interval_coverage > 0:
                pi_factor = min(1.0, accuracy_metrics.prediction_interval_coverage / 95)
                confidence_factors.append(pi_factor)
        
        # Ensemble diversity factor (if available)
        if hasattr(forecast_result, 'model_params') and 'ensemble_weights' in forecast_result.model_params:
            weights = list(forecast_result.model_params['ensemble_weights'].values())
            # Higher diversity (more even weights) = higher confidence
            entropy = -sum(w * np.log(w + 1e-10) for w in weights if w > 0)
            max_entropy = np.log(len(weights))
            diversity_factor = entropy / max_entropy if max_entropy > 0 else 0
            confidence_factors.append(diversity_factor)
        
        # Combine confidence factors
        if confidence_factors:
            return float(np.mean(confidence_factors))
        else:
            return 0.5  # Default confidence
    
    def _apply_seasonal_adjustment(self, data: pd.Series, seasonal_periods: int) -> pd.Series:
        """Apply seasonal adjustment to time series."""
        if len(data) < seasonal_periods * 2:
            return data
        
        # Simple seasonal adjustment using moving average
        seasonal_component = data.groupby(data.index % seasonal_periods).transform('mean')
        adjusted_data = data - seasonal_component + data.mean()
        
        return adjusted_data
    
    def _calculate_trend(self, data: pd.Series) -> pd.Series:
        """Calculate trend component of time series."""
        x = np.arange(len(data))
        coeffs = np.polyfit(x, data, 1)
        trend = coeffs[0] * x + coeffs[1]
        return pd.Series(trend, index=data.index)
    
    def _seasonal_decomposition(self, data: pd.Series) -> Dict[str, pd.Series]:
        """Simple seasonal decomposition."""
        # This is a simplified version - in production would use statsmodels
        period = 24 if len(data) >= 48 else len(data) // 2
        
        # Trend (moving average)
        trend = data.rolling(window=period, center=True).mean()
        
        # Seasonal component
        detrended = data - trend
        seasonal = detrended.groupby(data.index % period).transform('mean')
        
        # Residual
        residual = data - trend - seasonal
        
        return {
            "trend": trend,
            "seasonal": seasonal,
            "residual": residual
        }


class RegimeDetector:
    """Detect regime changes in time series."""
    
    async def detect_regime_changes(self, data: pd.Series) -> List[int]:
        """Detect regime change points."""
        # Simplified regime detection using variance changes
        window_size = min(24, len(data) // 4)
        if window_size < 2:
            return []
        
        variances = []
        for i in range(window_size, len(data) - window_size):
            window_var = data.iloc[i-window_size:i+window_size].var()
            variances.append(window_var)
        
        # Find significant variance changes
        var_changes = np.diff(variances)
        threshold = np.std(var_changes) * 2
        
        regime_points = []
        for i, change in enumerate(var_changes):
            if abs(change) > threshold:
                regime_points.append(i + window_size)
        
        return regime_points
    
    def get_current_regime(self, data: pd.Series) -> str:
        """Determine current regime (high/low volatility)."""
        recent_volatility = data.tail(12).std()
        overall_volatility = data.std()
        
        if recent_volatility > overall_volatility * 1.5:
            return "high_volatility"
        elif recent_volatility < overall_volatility * 0.5:
            return "low_volatility"
        else:
            return "normal"
    
    def calculate_regime_volatility(self, data: pd.Series, regime_points: List[int]) -> Dict[str, float]:
        """Calculate volatility for different regimes."""
        if not regime_points:
            return {"overall": data.std()}
        
        regime_volatilities = {}
        start_idx = 0
        
        for i, point in enumerate(regime_points):
            regime_data = data.iloc[start_idx:point]
            regime_volatilities[f"regime_{i}"] = regime_data.std()
            start_idx = point
        
        # Last regime
        regime_data = data.iloc[start_idx:]
        regime_volatilities[f"regime_{len(regime_points)}"] = regime_data.std()
        
        return regime_volatilities


class OutlierDetector:
    """Detect and handle outliers in time series."""
    
    async def detect_outliers(self, data: pd.Series) -> pd.Series:
        """Detect outliers using modified Z-score method."""
        # Calculate modified Z-score
        median = data.median()
        mad = np.median(np.abs(data - median))
        modified_z_scores = 0.6745 * (data - median) / mad
        
        # Mark outliers (threshold of 3.5)
        outliers = np.abs(modified_z_scores) > 3.5
        
        return pd.Series(outliers, index=data.index)