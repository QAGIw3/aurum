"""Curve calibration tools: smoothing, de-spiking, cap-floor logic, and audit deltas.

This module provides:
- Statistical smoothing algorithms for curve data
- Spike detection and removal with configurable thresholds
- Price cap and floor enforcement with business rules
- Comprehensive audit trail for all calibration changes
- Integration with curve versioning and provenance tracking
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from scipy import interpolate, signal
from scipy.stats import zscore

from ..telemetry.context import log_structured


class CalibrationConfig(BaseModel):
    """Configuration for curve calibration."""

    smoothing_method: str = Field(default="savgol", description="Smoothing method: savgol, exponential, moving_average")
    smoothing_window: int = Field(default=5, ge=3, description="Smoothing window size")
    outlier_threshold: float = Field(default=3.0, ge=1.0, description="Z-score threshold for outlier detection")
    spike_removal_method: str = Field(default="median_filter", description="Spike removal method")
    spike_threshold: float = Field(default=2.0, ge=0.5, description="Spike detection threshold")
    enable_cap_floor: bool = Field(default=True, description="Enable price cap/floor enforcement")
    price_cap: Optional[float] = Field(None, description="Maximum allowed price")
    price_floor: Optional[float] = Field(None, description="Minimum allowed price")
    preserve_peaks_valleys: bool = Field(default=True, description="Preserve important peaks and valleys")
    audit_changes: bool = Field(default=True, description="Track all calibration changes")


@dataclass
class CalibrationAuditEntry:
    """Audit entry for curve calibration changes."""

    timestamp: datetime
    curve_key: str
    operation: str  # 'smoothing', 'spike_removal', 'cap_floor', 'outlier_removal'
    parameters: Dict[str, Any]
    original_values: Dict[int, float]  # index -> original_value
    new_values: Dict[int, float]  # index -> new_value
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CalibrationResult:
    """Results from curve calibration."""

    original_curve: pd.Series
    calibrated_curve: pd.Series
    audit_entries: List[CalibrationAuditEntry]
    quality_metrics: Dict[str, float]
    calibration_summary: Dict[str, Any]


class CurveCalibrator:
    """Advanced curve calibration with multiple algorithms and audit tracking."""

    def __init__(self, config: CalibrationConfig):
        self.config = config
        self.audit_log: List[CalibrationAuditEntry] = []

    def calibrate_curve(
        self,
        curve_data: Union[pd.Series, np.ndarray],
        curve_key: str = "unknown",
        timestamps: Optional[pd.DatetimeIndex] = None,
    ) -> CalibrationResult:
        """Apply complete calibration pipeline to a curve."""

        # Convert to pandas Series if numpy array
        if isinstance(curve_data, np.ndarray):
            if timestamps is None:
                timestamps = pd.date_range(
                    start=datetime.now() - timedelta(hours=len(curve_data)),
                    periods=len(curve_data),
                    freq='H'
                )
            curve_series = pd.Series(curve_data, index=timestamps)
        else:
            curve_series = curve_data.copy()

        # Store original for comparison
        original_curve = curve_series.copy()

        # Apply calibration steps
        calibrated_curve = curve_series.copy()

        # 1. Outlier removal
        if self.config.outlier_threshold > 0:
            calibrated_curve, outlier_audit = self._remove_outliers(calibrated_curve, curve_key)
            self.audit_log.extend(outlier_audit)

        # 2. Spike removal
        if self.config.spike_threshold > 0:
            calibrated_curve, spike_audit = self._remove_spikes(calibrated_curve, curve_key)
            self.audit_log.extend(spike_audit)

        # 3. Smoothing
        calibrated_curve, smoothing_audit = self._apply_smoothing(calibrated_curve, curve_key)
        self.audit_log.extend(smoothing_audit)

        # 4. Cap/floor enforcement
        if self.config.enable_cap_floor:
            calibrated_curve, capfloor_audit = self._apply_cap_floor(calibrated_curve, curve_key)
            self.audit_log.extend(capfloor_audit)

        # 5. Final quality check
        quality_metrics = self._calculate_quality_metrics(original_curve, calibrated_curve)

        # 6. Peak/valley preservation
        if self.config.preserve_peaks_valleys:
            calibrated_curve = self._preserve_important_features(original_curve, calibrated_curve, curve_key)

        return CalibrationResult(
            original_curve=original_curve,
            calibrated_curve=calibrated_curve,
            audit_entries=self.audit_log.copy(),
            quality_metrics=quality_metrics,
            calibration_summary=self._generate_calibration_summary()
        )

    def _remove_outliers(self, curve: pd.Series, curve_key: str) -> Tuple[pd.Series, List[CalibrationAuditEntry]]:
        """Remove statistical outliers from the curve."""
        audit_entries = []

        # Calculate z-scores
        z_scores = np.abs(zscore(curve.fillna(curve.median())))
        outlier_mask = z_scores > self.config.outlier_threshold

        if outlier_mask.any():
            # Replace outliers with rolling median
            rolling_median = curve.rolling(
                window=min(7, len(curve)),
                center=True
            ).median()

            original_values = {}
            new_values = {}

            for idx in curve.index[outlier_mask]:
                original_values[idx] = curve.loc[idx]
                curve.loc[idx] = rolling_median.loc[idx]
                new_values[idx] = curve.loc[idx]

            audit_entry = CalibrationAuditEntry(
                timestamp=datetime.now(),
                curve_key=curve_key,
                operation="outlier_removal",
                parameters={"threshold": self.config.outlier_threshold, "method": "zscore"},
                original_values=original_values,
                new_values=new_values,
                reason=f"Removed {outlier_mask.sum()} outliers above threshold {self.config.outlier_threshold}",
                metadata={"outlier_indices": curve.index[outlier_mask].tolist()}
            )
            audit_entries.append(audit_entry)

        return curve, audit_entries

    def _remove_spikes(self, curve: pd.Series, curve_key: str) -> Tuple[pd.Series, List[CalibrationAuditEntry]]:
        """Remove price spikes using various methods."""
        audit_entries = []

        if self.config.spike_removal_method == "median_filter":
            # Use median filter to remove spikes
            window_size = min(5, len(curve) // 4)
            if window_size >= 3:
                filtered_curve = signal.medfilt(curve.values, kernel_size=window_size)

                # Find spikes (where original differs significantly from filtered)
                diff = np.abs(curve.values - filtered_curve)
                spike_mask = diff > (self.config.spike_threshold * np.std(diff))

                if spike_mask.any():
                    original_values = {}
                    new_values = {}

                    for i, idx in enumerate(curve.index[spike_mask]):
                        original_values[idx] = curve.loc[idx]
                        curve.loc[idx] = filtered_curve[i]
                        new_values[idx] = curve.loc[idx]

                    audit_entry = CalibrationAuditEntry(
                        timestamp=datetime.now(),
                        curve_key=curve_key,
                        operation="spike_removal",
                        parameters={
                            "method": "median_filter",
                            "window_size": window_size,
                            "threshold": self.config.spike_threshold
                        },
                        original_values=original_values,
                        new_values=new_values,
                        reason=f"Removed {spike_mask.sum()} spikes using median filter",
                        metadata={"spike_indices": curve.index[spike_mask].tolist()}
                    )
                    audit_entries.append(audit_entry)

        elif self.config.spike_removal_method == "threshold_based":
            # Simple threshold-based spike removal
            curve_mean = curve.mean()
            curve_std = curve.std()

            spike_mask = np.abs(curve - curve_mean) > (self.config.spike_threshold * curve_std)

            if spike_mask.any():
                original_values = {}
                new_values = {}

                for idx in curve.index[spike_mask]:
                    original_values[idx] = curve.loc[idx]
                    # Replace with rolling mean
                    rolling_mean = curve.rolling(3, center=True).mean()
                    curve.loc[idx] = rolling_mean.loc[idx]
                    new_values[idx] = curve.loc[idx]

                audit_entry = CalibrationAuditEntry(
                    timestamp=datetime.now(),
                    curve_key=curve_key,
                    operation="spike_removal",
                    parameters={
                        "method": "threshold_based",
                        "threshold": self.config.spike_threshold
                    },
                    original_values=original_values,
                    new_values=new_values,
                    reason=f"Removed {spike_mask.sum()} spikes using threshold method",
                    metadata={"spike_indices": curve.index[spike_mask].tolist()}
                )
                audit_entries.append(audit_entry)

        return curve, audit_entries

    def _apply_smoothing(self, curve: pd.Series, curve_key: str) -> Tuple[pd.Series, List[CalibrationAuditEntry]]:
        """Apply smoothing to the curve."""
        audit_entries = []
        original_curve = curve.copy()

        if self.config.smoothing_method == "savgol":
            # Savitzky-Golay filter
            window_size = min(self.config.smoothing_window, len(curve) // 2 * 2 - 1)
            if window_size >= 3:
                smoothed = signal.savgol_filter(
                    curve.values,
                    window_length=window_size,
                    polyorder=min(2, window_size - 1)
                )
                curve.values[:] = smoothed

        elif self.config.smoothing_method == "exponential":
            # Exponential moving average
            alpha = 2 / (self.config.smoothing_window + 1)
            curve.values[:] = curve.ewm(alpha=alpha).mean().values

        elif self.config.smoothing_method == "moving_average":
            # Simple moving average
            curve.values[:] = curve.rolling(
                window=self.config.smoothing_window,
                center=True
            ).mean().fillna(method='bfill').fillna(method='ffill').values

        # Check if smoothing made significant changes
        significant_changes = self._detect_significant_changes(original_curve, curve)
        if significant_changes:
            audit_entry = CalibrationAuditEntry(
                timestamp=datetime.now(),
                curve_key=curve_key,
                operation="smoothing",
                parameters={
                    "method": self.config.smoothing_method,
                    "window": self.config.smoothing_window
                },
                original_values={},
                new_values={},
                reason=f"Applied {self.config.smoothing_method} smoothing",
                metadata={"significant_changes": len(significant_changes)}
            )
            audit_entries.append(audit_entry)

        return curve, audit_entries

    def _apply_cap_floor(self, curve: pd.Series, curve_key: str) -> Tuple[pd.Series, List[CalibrationAuditEntry]]:
        """Apply price caps and floors."""
        audit_entries = []
        original_curve = curve.copy()

        if self.config.price_cap is not None:
            # Apply cap
            capped_mask = curve > self.config.price_cap
            if capped_mask.any():
                original_values = {}
                new_values = {}

                for idx in curve.index[capped_mask]:
                    original_values[idx] = curve.loc[idx]
                    curve.loc[idx] = self.config.price_cap
                    new_values[idx] = curve.loc[idx]

                audit_entry = CalibrationAuditEntry(
                    timestamp=datetime.now(),
                    curve_key=curve_key,
                    operation="cap_floor",
                    parameters={"action": "cap", "cap_value": self.config.price_cap},
                    original_values=original_values,
                    new_values=new_values,
                    reason=f"Applied price cap of {self.config.price_cap}",
                    metadata={"capped_points": capped_mask.sum()}
                )
                audit_entries.append(audit_entry)

        if self.config.price_floor is not None:
            # Apply floor
            floored_mask = curve < self.config.price_floor
            if floored_mask.any():
                original_values = {}
                new_values = {}

                for idx in curve.index[floored_mask]:
                    original_values[idx] = curve.loc[idx]
                    curve.loc[idx] = self.config.price_floor
                    new_values[idx] = curve.loc[idx]

                audit_entry = CalibrationAuditEntry(
                    timestamp=datetime.now(),
                    curve_key=curve_key,
                    operation="cap_floor",
                    parameters={"action": "floor", "floor_value": self.config.price_floor},
                    original_values=original_values,
                    new_values=new_values,
                    reason=f"Applied price floor of {self.config.price_floor}",
                    metadata={"floored_points": floored_mask.sum()}
                )
                audit_entries.append(audit_entry)

        return curve, audit_entries

    def _preserve_important_features(
        self,
        original_curve: pd.Series,
        calibrated_curve: pd.Series,
        curve_key: str
    ) -> pd.Series:
        """Preserve important peaks and valleys in the curve."""

        # Find significant peaks and valleys in original curve
        peaks, _ = signal.find_peaks(original_curve.values, prominence=0.1)
        valleys, _ = signal.find_peaks(-original_curve.values, prominence=0.1)

        # For significant features, blend back some original values
        result = calibrated_curve.copy()

        for peak_idx in peaks:
            if peak_idx < len(result):
                # Blend original and calibrated values for peaks
                original_val = original_curve.iloc[peak_idx]
                calibrated_val = calibrated_curve.iloc[peak_idx]
                if abs(original_val - calibrated_val) > 0.05 * abs(calibrated_val):
                    # Preserve some of the original peak
                    result.iloc[peak_idx] = 0.7 * calibrated_val + 0.3 * original_val

        for valley_idx in valleys:
            if valley_idx < len(result):
                # Blend original and calibrated values for valleys
                original_val = original_curve.iloc[valley_idx]
                calibrated_val = calibrated_curve.iloc[valley_idx]
                if abs(original_val - calibrated_val) > 0.05 * abs(calibrated_val):
                    # Preserve some of the original valley
                    result.iloc[valley_idx] = 0.7 * calibrated_val + 0.3 * original_val

        return result

    def _detect_significant_changes(
        self,
        original: pd.Series,
        calibrated: pd.Series,
        threshold: float = 0.1
    ) -> List[int]:
        """Detect significant changes between original and calibrated curves."""
        relative_change = np.abs((calibrated - original) / (original + 1e-8))
        return np.where(relative_change > threshold)[0].tolist()

    def _calculate_quality_metrics(
        self,
        original: pd.Series,
        calibrated: pd.Series
    ) -> Dict[str, float]:
        """Calculate quality metrics for calibration."""

        # Basic statistics
        original_mean = original.mean()
        calibrated_mean = calibrated.mean()

        # Mean absolute percentage error
        mape = np.mean(np.abs((calibrated - original) / (original + 1e-8))) * 100

        # Root mean square error
        rmse = np.sqrt(np.mean((calibrated - original) ** 2))

        # Signal-to-noise ratio improvement
        original_noise = np.std(original)
        calibrated_noise = np.std(calibrated)
        snr_improvement = (original_noise - calibrated_noise) / original_noise if original_noise > 0 else 0

        # Volatility reduction
        original_volatility = np.std(np.diff(original))
        calibrated_volatility = np.std(np.diff(calibrated))
        volatility_reduction = (
            (original_volatility - calibrated_volatility) / original_volatility
            if original_volatility > 0 else 0
        )

        return {
            "mape": mape,
            "rmse": rmse,
            "snr_improvement": snr_improvement,
            "volatility_reduction": volatility_reduction,
            "mean_change": calibrated_mean - original_mean,
            "std_change": calibrated.std() - original.std(),
        }

    def _generate_calibration_summary(self) -> Dict[str, Any]:
        """Generate summary of calibration operations."""
        operations = {}
        for entry in self.audit_log:
            if entry.operation not in operations:
                operations[entry.operation] = 0
            operations[entry.operation] += 1

        return {
            "total_operations": len(self.audit_log),
            "operations_by_type": operations,
            "calibration_config": self.config.dict(),
            "timestamp": datetime.now().isoformat(),
        }

    def get_calibration_report(self) -> Dict[str, Any]:
        """Generate comprehensive calibration report."""
        return {
            "config": self.config.dict(),
            "audit_log": [
                {
                    "timestamp": entry.timestamp.isoformat(),
                    "curve_key": entry.curve_key,
                    "operation": entry.operation,
                    "parameters": entry.parameters,
                    "changes_count": len(entry.original_values),
                    "reason": entry.reason,
                }
                for entry in self.audit_log
            ],
            "summary": self._generate_calibration_summary(),
        }


class BatchCurveCalibrator:
    """Batch processing for multiple curves."""

    def __init__(self, config: CalibrationConfig):
        self.config = config
        self.calibrators: Dict[str, CurveCalibrator] = {}

    async def calibrate_multiple_curves(
        self,
        curve_data_dict: Dict[str, Union[pd.Series, np.ndarray]],
        curve_configs: Optional[Dict[str, CalibrationConfig]] = None,
    ) -> Dict[str, CalibrationResult]:
        """Calibrate multiple curves in batch."""

        results = {}

        for curve_key, curve_data in curve_data_dict.items():
            # Use curve-specific config or default
            config = curve_configs.get(curve_key, self.config) if curve_configs else self.config

            # Create calibrator for this curve
            calibrator = CurveCalibrator(config)
            self.calibrators[curve_key] = calibrator

            # Calibrate curve
            result = calibrator.calibrate_curve(curve_data, curve_key)
            results[curve_key] = result

        log_structured(
            "info",
            "batch_curve_calibration_completed",
            num_curves=len(results),
            total_audit_entries=sum(len(result.audit_entries) for result in results.values()),
        )

        return results

    async def calibrate_curve_family(
        self,
        curve_family_data: Dict[str, Dict[str, Any]],
        family_config: Optional[CalibrationConfig] = None,
    ) -> Dict[str, CalibrationResult]:
        """Calibrate all curves in a family with shared parameters."""

        if family_config is None:
            family_config = self.config

        # Apply family-specific adjustments to config
        adjusted_configs = {}
        for curve_key in curve_family_data.keys():
            config = family_config.copy()
            # Adjust parameters based on curve characteristics
            if "price" in curve_key.lower():
                config.price_cap = config.price_cap or 1000.0
                config.price_floor = config.price_floor or -100.0
            adjusted_configs[curve_key] = config

        return await self.calibrate_multiple_curves(curve_family_data, adjusted_configs)


class CurveCalibrationEngine:
    """Main engine for curve calibration with versioning and persistence."""

    def __init__(self):
        self.default_config = CalibrationConfig()
        self.batch_processor = BatchCurveCalibrator(self.default_config)

    async def calibrate_with_versioning(
        self,
        curve_data: Union[pd.Series, np.ndarray],
        curve_key: str,
        version_id: str,
        config: Optional[CalibrationConfig] = None,
    ) -> CalibrationResult:
        """Calibrate curve with version tracking."""

        if config is None:
            config = self.default_config

        calibrator = CurveCalibrator(config)
        result = calibrator.calibrate_curve(curve_data, curve_key)

        # In a real implementation, this would save to versioned storage
        log_structured(
            "info",
            "curve_calibration_with_versioning",
            curve_key=curve_key,
            version_id=version_id,
            operations=len(result.audit_entries),
        )

        return result

    async def compare_calibration_methods(
        self,
        curve_data: Union[pd.Series, np.ndarray],
        curve_key: str,
        methods: List[str] = None,
    ) -> Dict[str, CalibrationResult]:
        """Compare different calibration methods."""

        if methods is None:
            methods = ["savgol", "exponential", "moving_average"]

        results = {}

        for method in methods:
            config = CalibrationConfig(smoothing_method=method)
            calibrator = CurveCalibrator(config)
            result = calibrator.calibrate_curve(curve_data, curve_key)
            results[method] = result

        return results

    def get_calibration_recommendations(
        self,
        curve_data: Union[pd.Series, np.ndarray],
        curve_key: str,
    ) -> Dict[str, Any]:
        """Get recommendations for curve calibration parameters."""

        # Analyze curve characteristics
        if isinstance(curve_data, np.ndarray):
            data_std = np.std(curve_data)
            data_mean = np.mean(curve_data)
        else:
            data_std = curve_data.std()
            data_mean = curve_data.mean()

        # Recommend configuration based on curve characteristics
        recommendations = {
            "volatility": data_std / abs(data_mean) if data_mean != 0 else 0,
            "recommended_smoothing": "savgol" if data_std / abs(data_mean) > 0.1 else "moving_average",
            "recommended_window": max(5, min(15, int(len(curve_data) / 10))),
            "outlier_threshold": 3.0 if data_std / abs(data_mean) > 0.2 else 2.5,
            "spike_threshold": 2.0 if data_std / abs(data_mean) > 0.1 else 1.5,
        }

        # Price-specific recommendations
        if "price" in curve_key.lower():
            recommendations.update({
                "enable_cap_floor": True,
                "price_cap": data_mean * 3,  # 3x mean as cap
                "price_floor": max(0, data_mean * 0.1),  # 10% of mean as floor
            })

        return recommendations


# Global calibration engine instance
_calibration_engine: Optional[CurveCalibrationEngine] = None


def get_calibration_engine() -> CurveCalibrationEngine:
    """Get the global curve calibration engine instance."""
    global _calibration_engine
    if _calibration_engine is None:
        _calibration_engine = CurveCalibrationEngine()
    return _calibration_engine
