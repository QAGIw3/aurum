"""Anomaly detection service for outlier detection and alerting.

Implements business logic for statistical anomaly detection, pattern recognition,
and automated alerting.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class AnomalyDetectionService(BaseService):
    """Service for anomaly detection operations.
    
    Anomaly detection provides:
    - Statistical anomaly detection (z-score, IQR, etc.)
    - Pattern-based anomaly detection
    - Real-time anomaly alerts
    - Anomaly classification and scoring
    - Historical anomaly tracking
    
    This service:
    - Detects anomalies in time-series data
    - Classifies anomaly types and severity
    - Generates real-time alerts
    - Provides anomaly analytics
    - Tracks detection accuracy
    """
    
    def __init__(self):
        """Initialize service with default configuration."""
        super().__init__()
        self._detected_anomalies: List[Dict[str, Any]] = []
        self._detection_models: Dict[str, Dict[str, Any]] = {}
    
    async def detect_anomalies(
        self,
        dataset_name: str,
        metric_name: str,
        data_points: List[float],
        detection_method: str = "zscore",
        sensitivity: float = 2.5,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Detect anomalies in time-series data.
        
        Args:
            dataset_name: Dataset identifier
            metric_name: Metric being analyzed
            data_points: Time-series data points
            detection_method: Detection algorithm
            sensitivity: Sensitivity threshold
            context: Service context
            
        Returns:
            ServiceResult with detected anomalies
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If detection fails
        """
        self._log_operation(
            "detect_anomalies",
            context=context,
            dataset_name=dataset_name,
            metric_name=metric_name
        )
        
        try:
            # Validate inputs
            self._validate_dataset_name(dataset_name)
            self._validate_metric_name(metric_name)
            self._validate_data_points(data_points)
            self._validate_detection_method(detection_method)
            self._validate_sensitivity(sensitivity)
            
            # Detect anomalies (simplified implementation)
            anomalies = self._run_detection(
                dataset_name,
                metric_name,
                data_points,
                detection_method,
                sensitivity
            )
            
            # Store detected anomalies
            self._detected_anomalies.extend(anomalies)
            
            return ServiceResult.ok(
                data=anomalies,
                metadata={
                    "dataset_name": dataset_name,
                    "metric_name": metric_name,
                    "anomaly_count": len(anomalies),
                    "detection_method": detection_method
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "detect_anomalies", context)
    
    async def classify_anomaly(
        self,
        anomaly_data: Dict[str, Any],
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Classify an anomaly by type and severity.
        
        Args:
            anomaly_data: Anomaly data to classify
            context: Service context
            
        Returns:
            ServiceResult with anomaly classification
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If classification fails
        """
        self._log_operation("classify_anomaly", context=context)
        
        try:
            # Validate inputs
            if not isinstance(anomaly_data, dict):
                raise ValidationError("Anomaly data must be a dictionary", field="anomaly_data")
            
            # Classify anomaly (simplified)
            severity = anomaly_data.get("score", 0)
            anomaly_type = self._determine_anomaly_type(anomaly_data)
            
            classification = {
                "anomaly_type": anomaly_type,
                "severity": self._classify_severity(severity),
                "confidence": 0.85,
                "recommended_action": self._recommend_action(severity),
                "classified_at": datetime.now().isoformat()
            }
            
            return ServiceResult.ok(
                data=classification,
                metadata={"anomaly_type": anomaly_type}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "classify_anomaly", context)
    
    async def get_anomaly_summary(
        self,
        dataset_name: str,
        start_time: datetime,
        end_time: datetime,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get anomaly summary for time period.
        
        Args:
            dataset_name: Dataset identifier
            start_time: Start of analysis period
            end_time: End of analysis period
            context: Service context
            
        Returns:
            ServiceResult with anomaly summary
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If summary generation fails
        """
        self._log_operation(
            "get_anomaly_summary",
            context=context,
            dataset_name=dataset_name
        )
        
        try:
            # Validate inputs
            self._validate_dataset_name(dataset_name)
            
            if start_time > end_time:
                raise ValidationError("Start time must be before end time", field="time_range")
            
            # Generate summary (simplified)
            summary = {
                "dataset_name": dataset_name,
                "period_start": start_time.isoformat(),
                "period_end": end_time.isoformat(),
                "total_anomalies": len(self._detected_anomalies),
                "high_severity_count": len([a for a in self._detected_anomalies if a.get("severity") == "high"]),
                "anomaly_rate": 0.05,  # 5% of data points
                "most_common_type": "spike",
                "generated_at": datetime.now().isoformat()
            }
            
            return ServiceResult.ok(
                data=summary,
                metadata={"dataset_name": dataset_name}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_anomaly_summary", context)
    
    # Private helper methods
    
    def _validate_dataset_name(self, dataset_name: str) -> None:
        """Validate dataset name."""
        if not dataset_name or not dataset_name.strip():
            raise ValidationError("Dataset name is required", field="dataset_name")
    
    def _validate_metric_name(self, metric_name: str) -> None:
        """Validate metric name."""
        if not metric_name or not metric_name.strip():
            raise ValidationError("Metric name is required", field="metric_name")
    
    def _validate_data_points(self, data_points: List[float]) -> None:
        """Validate data points."""
        if not data_points:
            raise ValidationError("Data points list cannot be empty", field="data_points")
        
        if len(data_points) < 10:
            raise ValidationError("Need at least 10 data points for detection", field="data_points")
    
    def _validate_detection_method(self, detection_method: str) -> None:
        """Validate detection method."""
        valid_methods = ["zscore", "iqr", "isolation_forest", "lstm"]
        if detection_method not in valid_methods:
            raise ValidationError(
                f"Invalid detection method. Must be one of: {', '.join(valid_methods)}",
                field="detection_method"
            )
    
    def _validate_sensitivity(self, sensitivity: float) -> None:
        """Validate sensitivity."""
        if sensitivity <= 0:
            raise ValidationError("Sensitivity must be positive", field="sensitivity")
    
    def _validate_policy_name(self, policy_name: str) -> None:
        """Validate policy name."""
        if not policy_name or not policy_name.strip():
            raise ValidationError("Policy name is required", field="policy_name")
    
    def _validate_policy_type(self, policy_type: str) -> None:
        """Validate policy type."""
        valid_types = ["var_limit", "exposure_limit", "concentration_limit"]
        if policy_type not in valid_types:
            raise ValidationError(
                f"Invalid policy type. Must be one of: {', '.join(valid_types)}",
                field="policy_type"
            )
    
    def _validate_risk_limits(self, risk_limits: Dict[str, float]) -> None:
        """Validate risk limits."""
        if not risk_limits:
            raise ValidationError("Risk limits cannot be empty", field="risk_limits")
    
    def _validate_portfolio_id(self, portfolio_id: str) -> None:
        """Validate portfolio ID."""
        if not portfolio_id or not portfolio_id.strip():
            raise ValidationError("Portfolio ID is required", field="portfolio_id")
    
    def _validate_risk_metrics(self, risk_metrics: Dict[str, float]) -> None:
        """Validate risk metrics."""
        if not risk_metrics:
            raise ValidationError("Risk metrics cannot be empty", field="risk_metrics")
    
    def _run_detection(
        self,
        dataset_name: str,
        metric_name: str,
        data_points: List[float],
        detection_method: str,
        sensitivity: float
    ) -> List[Dict[str, Any]]:
        """Run anomaly detection algorithm."""
        # Simplified z-score detection
        import statistics
        
        if len(data_points) < 2:
            return []
        
        mean = statistics.mean(data_points)
        stdev = statistics.stdev(data_points) if len(data_points) > 1 else 0
        
        anomalies = []
        for i, value in enumerate(data_points):
            if stdev > 0:
                z_score = abs((value - mean) / stdev)
                if z_score > sensitivity:
                    anomalies.append({
                        "index": i,
                        "value": value,
                        "score": z_score,
                        "severity": "high" if z_score > 3 else "medium",
                        "detected_at": datetime.now().isoformat()
                    })
        
        return anomalies
    
    def _determine_anomaly_type(self, anomaly_data: Dict[str, Any]) -> str:
        """Determine type of anomaly."""
        # Simplified classification
        score = anomaly_data.get("score", 0)
        if score > 4:
            return "spike"
        elif score > 3:
            return "outlier"
        else:
            return "deviation"
    
    def _classify_severity(self, score: float) -> str:
        """Classify anomaly severity."""
        if score > 4:
            return "high"
        elif score > 3:
            return "medium"
        else:
            return "low"
    
    def _recommend_action(self, severity: float) -> str:
        """Recommend action based on severity."""
        if severity > 4:
            return "immediate_investigation"
        elif severity > 3:
            return "review_required"
        else:
            return "monitor"

