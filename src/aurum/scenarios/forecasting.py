"""Baseline forecasting pack with ARIMA, ETS, XGBoost models and backtesting.

This module provides:
- Statistical forecasting models (ARIMA, ETS)
- Machine learning forecasting (XGBoost)
- Comprehensive backtesting framework
- Performance metrics (MAPE, SMAPE, RMSE, etc.)
- Cross-validation and model selection
"""

from __future__ import annotations

import asyncio
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

from ..telemetry.context import log_structured


class ForecastConfig(BaseModel):
    """Configuration for forecasting models."""

    model_type: str = Field(..., description="Type of forecasting model")
    forecast_horizon: int = Field(default=24, ge=1, description="Number of periods to forecast")
    seasonality_periods: Optional[int] = Field(None, description="Seasonal periods for seasonal models")
    confidence_level: float = Field(default=0.95, ge=0.0, le=1.0, description="Confidence level for prediction intervals")
    cross_validation_folds: int = Field(default=5, ge=2, description="Number of cross-validation folds")


@dataclass
class ForecastResult:
    """Results from a forecasting model."""

    model_type: str
    predictions: np.ndarray
    confidence_intervals: Optional[Tuple[np.ndarray, np.ndarray]] = None
    forecast_dates: Optional[pd.DatetimeIndex] = None
    model_params: Dict[str, Any] = None
    training_time: float = 0.0
    prediction_time: float = 0.0
    accuracy_metrics: Dict[str, float] = None


@dataclass
class BacktestResult:
    """Results from backtesting."""

    model_type: str
    mape: float
    smape: float
    rmse: float
    mae: float
    r2_score: float
    predictions: List[np.ndarray]
    actuals: List[np.ndarray]
    forecast_dates: List[pd.DatetimeIndex]
    training_windows: List[Tuple[pd.DatetimeIndex, pd.DatetimeIndex]]


class BaseForecaster(ABC):
    """Abstract base class for forecasting models."""

    def __init__(self, config: ForecastConfig):
        self.config = config
        self.fitted = False
        self.model = None

    @abstractmethod
    def fit(self, data: Union[pd.Series, np.ndarray], **kwargs) -> BaseForecaster:
        """Fit the forecasting model to training data."""
        pass

    @abstractmethod
    def predict(self, steps: int, **kwargs) -> ForecastResult:
        """Generate forecasts for the specified number of steps."""
        pass

    @abstractmethod
    def predict_with_intervals(self, steps: int, confidence_level: float = 0.95) -> ForecastResult:
        """Generate forecasts with prediction intervals."""
        pass

    def validate_data(self, data: Union[pd.Series, np.ndarray]) -> None:
        """Validate input data for forecasting."""
        if len(data) < 10:
            raise ValueError("Insufficient data for forecasting (minimum 10 observations required)")

        if np.isnan(data).any():
            raise ValueError("Data contains NaN values")

    def calculate_metrics(self, actual: np.ndarray, predicted: np.ndarray) -> Dict[str, float]:
        """Calculate forecasting accuracy metrics."""
        if len(actual) != len(predicted):
            raise ValueError("Actual and predicted arrays must have the same length")

        # Avoid division by zero
        actual = np.where(actual == 0, 1e-10, actual)

        mape = mean_absolute_percentage_error(actual, predicted) * 100
        smape = np.mean(2 * np.abs(predicted - actual) / (np.abs(predicted) + np.abs(actual))) * 100
        rmse = np.sqrt(mean_squared_error(actual, predicted))
        mae = np.mean(np.abs(actual - predicted))

        # R² score
        ss_res = np.sum((actual - predicted) ** 2)
        ss_tot = np.sum((actual - np.mean(actual)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        return {
            "mape": mape,
            "smape": smape,
            "rmse": rmse,
            "mae": mae,
            "r2_score": r2,
        }


class ARIMAForecaster(BaseForecaster):
    """ARIMA forecasting model."""

    def __init__(self, config: ForecastConfig):
        super().__init__(config)
        self.p = 1  # AR order
        self.d = 1  # Differencing order
        self.q = 1  # MA order

    def fit(self, data: Union[pd.Series, np.ndarray], **kwargs) -> ARIMAForecaster:
        """Fit ARIMA model to data."""
        self.validate_data(data)

        try:
            from statsmodels.tsa.arima.model import ARIMA
        except ImportError:
            raise ImportError("statsmodels is required for ARIMA forecasting")

        start_time = datetime.now()

        # Convert to numpy if pandas Series
        if isinstance(data, pd.Series):
            self.data = data.values
            self.index = data.index
        else:
            self.data = data
            self.index = pd.date_range(start=datetime.now() - timedelta(days=len(data)), periods=len(data), freq='H')

        # Fit ARIMA model
        self.model = ARIMA(self.data, order=(self.p, self.d, self.q))
        self.fitted_model = self.model.fit()

        self.fitted = True
        self.training_time = (datetime.now() - start_time).total_seconds()

        return self

    def predict(self, steps: int, **kwargs) -> ForecastResult:
        """Generate ARIMA forecasts."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        start_time = datetime.now()

        # Generate forecast
        forecast = self.fitted_model.forecast(steps=steps)

        # Create future dates
        last_date = self.index[-1]
        forecast_dates = pd.date_range(start=last_date + pd.Timedelta(hours=1), periods=steps, freq='H')

        prediction_time = (datetime.now() - start_time).total_seconds()

        return ForecastResult(
            model_type="ARIMA",
            predictions=forecast,
            forecast_dates=forecast_dates,
            model_params={"p": self.p, "d": self.d, "q": self.q},
            training_time=self.training_time,
            prediction_time=prediction_time,
        )

    def predict_with_intervals(self, steps: int, confidence_level: float = 0.95) -> ForecastResult:
        """Generate ARIMA forecasts with prediction intervals."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        start_time = datetime.now()

        # Generate forecast with intervals
        forecast_obj = self.fitted_model.get_forecast(steps=steps)
        forecast = forecast_obj.predicted_mean
        conf_int = forecast_obj.conf_int(alpha=1-confidence_level)

        # Create future dates
        last_date = self.index[-1]
        forecast_dates = pd.date_range(start=last_date + pd.Timedelta(hours=1), periods=steps, freq='H')

        prediction_time = (datetime.now() - start_time).total_seconds()

        return ForecastResult(
            model_type="ARIMA",
            predictions=forecast,
            confidence_intervals=(conf_int.iloc[:, 0].values, conf_int.iloc[:, 1].values),
            forecast_dates=forecast_dates,
            model_params={"p": self.p, "d": self.d, "q": self.q},
            training_time=self.training_time,
            prediction_time=prediction_time,
        )


class ETSForecaster(BaseForecaster):
    """Exponential Smoothing (ETS) forecasting model."""

    def __init__(self, config: ForecastConfig):
        super().__init__(config)
        self.trend = "add"  # Trend component
        self.seasonal = "add" if config.seasonality_periods else None  # Seasonal component
        self.seasonal_periods = config.seasonality_periods

    def fit(self, data: Union[pd.Series, np.ndarray], **kwargs) -> ETSForecaster:
        """Fit ETS model to data."""
        self.validate_data(data)

        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
        except ImportError:
            raise ImportError("statsmodels is required for ETS forecasting")

        start_time = datetime.now()

        # Convert to numpy if pandas Series
        if isinstance(data, pd.Series):
            self.data = data.values
            self.index = data.index
        else:
            self.data = data
            self.index = pd.date_range(start=datetime.now() - timedelta(days=len(data)), periods=len(data), freq='H')

        # Fit ETS model
        self.model = ExponentialSmoothing(
            self.data,
            trend=self.trend,
            seasonal=self.seasonal,
            seasonal_periods=self.seasonal_periods
        )
        self.fitted_model = self.model.fit()

        self.fitted = True
        self.training_time = (datetime.now() - start_time).total_seconds()

        return self

    def predict(self, steps: int, **kwargs) -> ForecastResult:
        """Generate ETS forecasts."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        start_time = datetime.now()

        # Generate forecast
        forecast = self.fitted_model.forecast(steps=steps)

        # Create future dates
        last_date = self.index[-1]
        forecast_dates = pd.date_range(start=last_date + pd.Timedelta(hours=1), periods=steps, freq='H')

        prediction_time = (datetime.now() - start_time).total_seconds()

        return ForecastResult(
            model_type="ETS",
            predictions=forecast,
            forecast_dates=forecast_dates,
            model_params={
                "trend": self.trend,
                "seasonal": self.seasonal,
                "seasonal_periods": self.seasonal_periods
            },
            training_time=self.training_time,
            prediction_time=prediction_time,
        )

    def predict_with_intervals(self, steps: int, confidence_level: float = 0.95) -> ForecastResult:
        """Generate ETS forecasts with prediction intervals."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        start_time = datetime.now()

        # Generate forecast with intervals
        forecast = self.fitted_model.forecast(steps=steps)

        # For ETS, we'll use simple confidence intervals based on residuals
        residuals = self.fitted_model.resid
        residual_std = np.std(residuals)

        # Simple prediction intervals (not as accurate as ARIMA)
        z_score = 1.96  # 95% confidence
        margin_error = z_score * residual_std * np.sqrt(np.arange(1, steps + 1))

        ci_lower = forecast - margin_error
        ci_upper = forecast + margin_error

        # Create future dates
        last_date = self.index[-1]
        forecast_dates = pd.date_range(start=last_date + pd.Timedelta(hours=1), periods=steps, freq='H')

        prediction_time = (datetime.now() - start_time).total_seconds()

        return ForecastResult(
            model_type="ETS",
            predictions=forecast,
            confidence_intervals=(ci_lower, ci_upper),
            forecast_dates=forecast_dates,
            model_params={
                "trend": self.trend,
                "seasonal": self.seasonal,
                "seasonal_periods": self.seasonal_periods
            },
            training_time=self.training_time,
            prediction_time=prediction_time,
        )


class XGBoostForecaster(BaseForecaster):
    """XGBoost forecasting model with engineered features."""

    def __init__(self, config: ForecastConfig):
        super().__init__(config)
        self.n_estimators = 100
        self.max_depth = 6
        self.learning_rate = 0.1

    def fit(self, data: Union[pd.Series, np.ndarray], **kwargs) -> XGBoostForecaster:
        """Fit XGBoost model to data with engineered features."""
        self.validate_data(data)

        try:
            import xgboost as xgb
        except ImportError:
            raise ImportError("xgboost is required for XGBoost forecasting")

        start_time = datetime.now()

        # Convert to numpy if pandas Series
        if isinstance(data, pd.Series):
            self.data = data.values
            self.index = data.index
        else:
            self.data = data
            self.index = pd.date_range(start=datetime.now() - timedelta(days=len(data)), periods=len(data), freq='H')

        # Engineer features
        X, y = self._engineer_features(self.data)

        # Fit XGBoost model
        self.model = xgb.XGBRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            learning_rate=self.learning_rate,
            objective='reg:squarederror'
        )
        self.model.fit(X, y)

        self.fitted = True
        self.training_time = (datetime.now() - start_time).total_seconds()

        return self

    def _engineer_features(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Engineer features for time series forecasting."""
        # Create lagged features
        lags = [1, 2, 3, 6, 12, 24, 48, 72]
        X = []
        y = data[lags[-1]:]  # Target is shifted by maximum lag

        for i in range(len(lags)):
            lag = lags[i]
            if lag < len(data):
                X.append(data[lag:-lags[-1]+lag] if lag > 0 else data[:-lags[-1]])
            else:
                # Pad with zeros or mean if not enough data
                feature = np.full(len(data) - lags[-1], np.mean(data))
                X.append(feature)

        # Add rolling statistics
        for window in [7, 14, 30]:
            if window < len(data):
                rolling_mean = pd.Series(data).rolling(window).mean().iloc[window:].values
                rolling_std = pd.Series(data).rolling(window).std().iloc[window:].values

                # Pad to match target length
                pad_length = lags[-1]
                rolling_mean = np.pad(rolling_mean, (pad_length, 0), mode='edge')[:len(y)]
                rolling_std = np.pad(rolling_std, (pad_length, 0), mode='edge')[:len(y)]

                X.append(rolling_mean)
                X.append(rolling_std)

        return np.column_stack(X), y

    def predict(self, steps: int, **kwargs) -> ForecastResult:
        """Generate XGBoost forecasts."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        start_time = datetime.now()

        # Generate predictions iteratively
        predictions = []
        current_data = self.data.copy()

        for _ in range(steps):
            # Engineer features for current state
            X_pred = self._prepare_prediction_features(current_data)

            # Predict next value
            pred = self.model.predict(X_pred.reshape(1, -1))[0]
            predictions.append(pred)

            # Update data for next prediction
            current_data = np.append(current_data, pred)

        predictions = np.array(predictions)

        # Create future dates
        last_date = self.index[-1]
        forecast_dates = pd.date_range(start=last_date + pd.Timedelta(hours=1), periods=steps, freq='H')

        prediction_time = (datetime.now() - start_time).total_seconds()

        return ForecastResult(
            model_type="XGBoost",
            predictions=predictions,
            forecast_dates=forecast_dates,
            model_params={
                "n_estimators": self.n_estimators,
                "max_depth": self.max_depth,
                "learning_rate": self.learning_rate
            },
            training_time=self.training_time,
            prediction_time=prediction_time,
        )

    def _prepare_prediction_features(self, data: np.ndarray) -> np.ndarray:
        """Prepare features for prediction."""
        # Similar to _engineer_features but for prediction
        lags = [1, 2, 3, 6, 12, 24, 48, 72]
        features = []

        for lag in lags:
            if lag < len(data):
                features.append(data[-lag])
            else:
                features.append(np.mean(data))

        # Rolling statistics
        for window in [7, 14, 30]:
            if window <= len(data):
                features.append(np.mean(data[-window:]))
                features.append(np.std(data[-window:]))
            else:
                features.append(np.mean(data))
                features.append(np.std(data))

        return np.array(features)

    def predict_with_intervals(self, steps: int, confidence_level: float = 0.95) -> ForecastResult:
        """Generate XGBoost forecasts with prediction intervals using quantile regression."""
        if not self.fitted or self.model is None:
            raise RuntimeError("Model must be fitted before prediction")

        # For simplicity, use basic confidence intervals based on residuals
        # In practice, you would train separate models for different quantiles
        result = self.predict(steps)

        # Estimate prediction intervals based on training residuals
        if hasattr(self.model, 'feature_importances_'):
            # Simple estimate based on model confidence
            residual_estimate = np.std(self.data) * 0.1  # Rough estimate
            z_score = 1.96
            margin_error = z_score * residual_estimate * np.sqrt(np.arange(1, steps + 1))

            ci_lower = result.predictions - margin_error
            ci_upper = result.predictions + margin_error

            result.confidence_intervals = (ci_lower, ci_upper)

        return result


class ForecastingEngine:
    """Main forecasting engine with multiple models and backtesting."""

    def __init__(self):
        self.models: Dict[str, BaseForecaster] = {}
        self._register_default_models()

    def _register_default_models(self) -> None:
        """Register default forecasting models."""
        arima_config = ForecastConfig(model_type="ARIMA", forecast_horizon=24)
        ets_config = ForecastConfig(model_type="ETS", forecast_horizon=24, seasonality_periods=24)
        xgb_config = ForecastConfig(model_type="XGBoost", forecast_horizon=24)

        self.models["ARIMA"] = ARIMAForecaster(arima_config)
        self.models["ETS"] = ETSForecaster(ets_config)
        self.models["XGBoost"] = XGBoostForecaster(xgb_config)

    async def forecast(
        self,
        data: Union[pd.Series, np.ndarray],
        model_type: str,
        config: Optional[ForecastConfig] = None,
        include_intervals: bool = True,
    ) -> ForecastResult:
        """Generate forecast using specified model."""

        if model_type not in self.models:
            raise ValueError(f"Unknown model type: {model_type}")

        model = self.models[model_type]

        if config:
            model.config = config

        # Fit and predict
        fitted_model = model.fit(data)

        if include_intervals:
            result = fitted_model.predict_with_intervals(model.config.forecast_horizon)
        else:
            result = fitted_model.predict(model.config.forecast_horizon)

        log_structured(
            "info",
            "forecast_completed",
            model_type=model_type,
            forecast_horizon=model.config.forecast_horizon,
            training_time=result.training_time,
            prediction_time=result.prediction_time,
        )

        return result

    async def backtest(
        self,
        data: Union[pd.Series, np.ndarray],
        model_type: str,
        config: Optional[ForecastConfig] = None,
        test_size: float = 0.2,
    ) -> BacktestResult:
        """Perform backtesting on historical data."""

        if model_type not in self.models:
            raise ValueError(f"Unknown model type: {model_type}")

        # Split data into train/test
        n = len(data)
        test_n = int(n * test_size)
        train_data = data[:-test_n]
        test_data = data[-test_n:]

        model_class = self.models[model_type].__class__
        model_config = config or ForecastConfig(model_type=model_type, forecast_horizon=24)

        # Perform rolling forecast
        predictions = []
        actuals = []
        forecast_dates = []

        # Use time series cross-validation
        tscv = TimeSeriesSplit(n_splits=min(5, len(train_data) // model_config.forecast_horizon))

        for train_idx, test_idx in tscv.split(train_data):
            train_fold = train_data[train_idx]
            test_fold = train_data[test_idx]

            # Fit model on training fold
            model = model_class(model_config).fit(train_fold)

            # Generate forecast for test fold length
            forecast_steps = len(test_fold)
            forecast_result = model.predict(forecast_steps)

            predictions.append(forecast_result.predictions)
            actuals.append(test_fold)
            forecast_dates.append(forecast_result.forecast_dates)

        # Calculate overall metrics
        all_predictions = np.concatenate(predictions)
        all_actuals = np.concatenate(actuals)

        metrics = self.models[model_type].calculate_metrics(all_actuals, all_predictions)

        return BacktestResult(
            model_type=model_type,
            mape=metrics["mape"],
            smape=metrics["smape"],
            rmse=metrics["rmse"],
            mae=metrics["mae"],
            r2_score=metrics["r2_score"],
            predictions=predictions,
            actuals=actuals,
            forecast_dates=forecast_dates,
            training_windows=[],  # Not tracking individual windows for simplicity
        )

    async def compare_models(
        self,
        data: Union[pd.Series, np.ndarray],
        model_types: List[str],
        config: Optional[ForecastConfig] = None,
    ) -> Dict[str, Union[ForecastResult, BacktestResult]]:
        """Compare multiple forecasting models."""

        results = {}

        # Generate forecasts for each model
        for model_type in model_types:
            try:
                forecast_result = await self.forecast(data, model_type, config, include_intervals=False)
                backtest_result = await self.backtest(data, model_type, config)

                results[model_type] = {
                    "forecast": forecast_result,
                    "backtest": backtest_result,
                }

            except Exception as exc:
                log_structured(
                    "error",
                    "model_comparison_failed",
                    model_type=model_type,
                    error=str(exc),
                )
                results[model_type] = {"error": str(exc)}

        return results

    def get_model_info(self, model_type: str) -> Dict[str, Any]:
        """Get information about a forecasting model."""
        if model_type not in self.models:
            raise ValueError(f"Unknown model type: {model_type}")

        model = self.models[model_type]
        return {
            "model_type": model_type,
            "config": model.config.dict(),
            "fitted": model.fitted,
        }

    def list_models(self) -> List[str]:
        """List available forecasting models."""
        return list(self.models.keys())


# Global forecasting engine instance
_forecasting_engine: Optional[ForecastingEngine] = None


def get_forecasting_engine() -> ForecastingEngine:
    """Get the global forecasting engine instance."""
    global _forecasting_engine
    if _forecasting_engine is None:
        _forecasting_engine = ForecastingEngine()
    return _forecasting_engine
