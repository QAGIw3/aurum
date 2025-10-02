"""Compatibility shim for anomaly detection service.

Provides backward compatibility for code using the old anomaly_detection_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime
from pydantic import BaseModel

from aurum.services.ml.anomaly_detection import AnomalyDetectionService


class AnomalySignal(BaseModel):
    """Anomaly signal for backward compatibility."""
    signal_id: str
    timestamp: datetime
    asset_type: str
    asset_id: str
    geography: str
    anomaly_type: str
    severity: str
    confidence: float
    value: float
    expected_value: float
    deviation: float
    deviation_percent: float
    algorithm: str
    metadata: Dict[str, Any]


class AnomalyDetectionConfig(BaseModel):
    """Configuration for anomaly detection."""
    algorithm: str = "isolation_forest"
    contamination: float = 0.1
    window_size: int = 100
    min_samples: int = 50
    z_score_threshold: float = 3.0
    iqr_multiplier: float = 1.5


class AnomalyDetectionResult(BaseModel):
    """Result of anomaly detection."""
    timestamp: datetime
    is_anomaly: bool
    value: float
    expected_value: float
    deviation: float
    deviation_percent: float
    anomaly_score: float
    algorithm: str


# Singleton instance
_service_instance = None


def get_anomaly_detection_service() -> AnomalyDetectionService:
    """Get singleton anomaly detection service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = AnomalyDetectionService()
    return _service_instance


async def detect_price_anomalies(
    price_data: List[Dict[str, Any]],
    geography: str = "US"
) -> List[AnomalyDetectionResult]:
    """Detect anomalies in price data."""
    service = get_anomaly_detection_service()
    
    # Extract values from price data
    values = [float(d.get("value", 0)) for d in price_data]
    timestamps = [d.get("timestamp", datetime.now()) for d in price_data]
    
    # Call new service
    result = await service.detect_anomalies(
        dataset_name=f"price_{geography}",
        metric_name="lmp",
        data_points=values,
        detection_method="zscore",
        sensitivity=2.5
    )
    
    # Convert to legacy format
    anomalies = result.data if result.success else []
    anomaly_indices = {a["index"] for a in anomalies}
    
    results = []
    for i, (value, timestamp) in enumerate(zip(values, timestamps)):
        is_anomaly = i in anomaly_indices
        anomaly_data = next((a for a in anomalies if a["index"] == i), None)
        
        results.append(AnomalyDetectionResult(
            timestamp=timestamp,
            is_anomaly=is_anomaly,
            value=value,
            expected_value=value * 0.95,  # Mock expected value
            deviation=value * 0.05 if is_anomaly else 0,
            deviation_percent=5.0 if is_anomaly else 0,
            anomaly_score=anomaly_data["score"] if anomaly_data else 0,
            algorithm="zscore"
        ))
    
    return results


async def detect_load_anomalies(
    load_data: List[Dict[str, Any]],
    geography: str = "US"
) -> List[AnomalyDetectionResult]:
    """Detect anomalies in load data."""
    service = get_anomaly_detection_service()
    
    # Extract values from load data
    values = [float(d.get("value", 0)) for d in load_data]
    timestamps = [d.get("timestamp", datetime.now()) for d in load_data]
    
    # Call new service
    result = await service.detect_anomalies(
        dataset_name=f"load_{geography}",
        metric_name="system",
        data_points=values,
        detection_method="zscore",
        sensitivity=2.5
    )
    
    # Convert to legacy format
    anomalies = result.data if result.success else []
    anomaly_indices = {a["index"] for a in anomalies}
    
    results = []
    for i, (value, timestamp) in enumerate(zip(values, timestamps)):
        is_anomaly = i in anomaly_indices
        anomaly_data = next((a for a in anomalies if a["index"] == i), None)
        
        results.append(AnomalyDetectionResult(
            timestamp=timestamp,
            is_anomaly=is_anomaly,
            value=value,
            expected_value=value * 0.98,  # Mock expected value
            deviation=value * 0.02 if is_anomaly else 0,
            deviation_percent=2.0 if is_anomaly else 0,
            anomaly_score=anomaly_data["score"] if anomaly_data else 0,
            algorithm="zscore"
        ))
    
    return results


async def publish_anomaly_signal(signal: AnomalySignal) -> None:
    """Publish anomaly signal - stub for backward compatibility."""
    # In the new architecture, this would be handled by an event bus
    # or messaging service, not directly by the anomaly detection service
    pass
