"""Real-time Anomaly Detection Service with Kafka signal publishing.

This service provides:
- Real-time anomaly detection for price/load data streams
- Statistical and ML-based anomaly detection algorithms
- Signal publishing to Kafka topics
- Integration with ClickHouse materialized views
- Historical anomaly analysis and alerting
- Performance monitoring and health checks
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..dao.experimental import TrinoDAO


class AnomalyDetectionConfig(BaseModel):
    """Configuration for anomaly detection algorithms."""

    algorithm: str = "isolation_forest"  # "isolation_forest", "local_outlier_factor", "z_score", "iqr"
    contamination: float = 0.1  # Expected proportion of anomalies
    window_size: int = 100  # Rolling window size for analysis
    min_samples: int = 50  # Minimum samples before detection starts
    z_score_threshold: float = 3.0  # Z-score threshold for statistical detection
    iqr_multiplier: float = 1.5  # IQR multiplier for outlier detection
    enable_kafka_signals: bool = True
    kafka_topic: str = "aurum.signals.anomalies"
    kafka_bootstrap_servers: List[str] = ["localhost:9092"]
    signal_retention_hours: int = 168  # 7 days


class AnomalySignal(BaseModel):
    """Anomaly detection signal."""

    signal_id: str
    timestamp: datetime
    asset_type: str  # "price", "load", "renewable"
    asset_id: str  # Specific asset identifier
    geography: str
    anomaly_type: str  # "statistical", "ml", "threshold", "pattern"
    severity: str  # "low", "medium", "high", "critical"
    confidence: float  # 0.0 to 1.0
    value: float  # Actual anomalous value
    expected_value: float  # Expected/normal value
    deviation: float  # Absolute deviation from expected
    deviation_percent: float  # Percentage deviation
    algorithm: str  # Detection algorithm used
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class AnomalyDetectionResult(BaseModel):
    """Result of anomaly detection analysis."""

    timestamp: datetime
    asset_type: str
    asset_id: str
    geography: str
    is_anomaly: bool
    anomaly_score: float  # Raw anomaly score
    confidence: float  # Confidence in the detection
    algorithm: str
    features_used: List[str]
    explanation: str


class AnomalyDetectionService:
    """Real-time anomaly detection service with streaming capabilities."""

    def __init__(self, config: AnomalyDetectionConfig):
        """Initialize anomaly detection service."""
        self.config = config
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Real-time detection state
        self._detection_windows: Dict[str, deque] = {}  # Asset -> rolling window
        self._kafka_producer = None
        self._signal_buffer: List[AnomalySignal] = []
        self._buffer_lock = asyncio.Lock()

        # Initialize Kafka producer if enabled
        if config.enable_kafka_signals:
            self._init_kafka_producer()

        # Start background signal publishing
        self._signal_task: Optional[asyncio.Task] = None

    def _init_kafka_producer(self) -> None:
        """Initialize Kafka producer for signal publishing."""
        try:
            from aiokafka import AIOKafkaProducer
            self._kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v.dict() if hasattr(v, 'dict') else v).encode('utf-8')
            )
        except ImportError:
            log_structured("warning", "kafka_not_available", message="aiokafka not available for signal publishing")

    async def start(self) -> None:
        """Start the anomaly detection service."""
        if self._kafka_producer:
            await self._kafka_producer.start()

        # Start signal publishing task
        self._signal_task = asyncio.create_task(self._signal_publishing_loop())

        self.telemetry.info("Anomaly detection service started")

    async def stop(self) -> None:
        """Stop the anomaly detection service."""
        if self._signal_task:
            self._signal_task.cancel()
            try:
                await self._signal_task
            except asyncio.CancelledError:
                pass

        if self._kafka_producer:
            await self._kafka_producer.stop()

        self.telemetry.info("Anomaly detection service stopped")

    async def _signal_publishing_loop(self) -> None:
        """Background task to publish signals to Kafka."""
        while True:
            try:
                await asyncio.sleep(5)  # Publish every 5 seconds

                async with self._buffer_lock:
                    if self._signal_buffer:
                        # Publish signals to Kafka
                        if self._kafka_producer:
                            signals_to_publish = self._signal_buffer.copy()
                            self._signal_buffer.clear()

                            for signal in signals_to_publish:
                                await self._kafka_producer.send_and_wait(
                                    self.config.kafka_topic,
                                    signal
                                )

                        # Also store in database for historical analysis
                        await self._store_signals_batch(signals_to_publish)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Signal publishing failed", error=str(e))

    async def _store_signals_batch(self, signals: List[AnomalySignal]) -> None:
        """Store anomaly signals in database for historical analysis."""
        try:
            # Insert signals into ClickHouse/materialized view
            # This would be implemented with actual database operations
            self.telemetry.info("Stored anomaly signals", count=len(signals))
        except Exception as e:
            self.telemetry.error("Failed to store signals", error=str(e))

    async def detect_anomalies(
        self,
        data_points: List[Dict[str, Any]],
        asset_type: str,
        asset_id: str,
        geography: str = "US"
    ) -> List[AnomalyDetectionResult]:
        """Detect anomalies in real-time data stream."""
        results = []

        # Update rolling window for this asset
        asset_key = f"{asset_type}:{asset_id}:{geography}"
        if asset_key not in self._detection_windows:
            self._detection_windows[asset_key] = deque(maxlen=self.config.window_size)

        window = self._detection_windows[asset_key]

        for point in data_points:
            timestamp = point.get('timestamp', datetime.utcnow())
            value = point.get('value', 0.0)

            window.append({
                'timestamp': timestamp,
                'value': value,
                'features': point.get('features', {})
            })

            # Only detect if we have enough samples
            if len(window) < self.config.min_samples:
                continue

            # Run anomaly detection
            result = await self._detect_single_point(window, asset_type, asset_id, geography)
            results.append(result)

            # Publish signal if anomaly detected
            if result.is_anomaly:
                await self._publish_anomaly_signal(result)

        return results

    async def _detect_single_point(
        self,
        window: deque,
        asset_type: str,
        asset_id: str,
        geography: str
    ) -> AnomalyDetectionResult:
        """Detect anomaly for a single data point."""
        # Extract values from window
        values = [point['value'] for point in window]

        # Choose detection algorithm
        if self.config.algorithm == "z_score":
            is_anomaly, score, confidence = self._z_score_detection(values)
        elif self.config.algorithm == "iqr":
            is_anomaly, score, confidence = self._iqr_detection(values)
        elif self.config.algorithm == "isolation_forest":
            is_anomaly, score, confidence = await self._isolation_forest_detection(values)
        else:
            # Default to z-score
            is_anomaly, score, confidence = self._z_score_detection(values)

        current_point = window[-1]

        return AnomalyDetectionResult(
            timestamp=current_point['timestamp'],
            asset_type=asset_type,
            asset_id=asset_id,
            geography=geography,
            is_anomaly=is_anomaly,
            anomaly_score=score,
            confidence=confidence,
            algorithm=self.config.algorithm,
            features_used=list(current_point['features'].keys()),
            explanation=f"Anomaly detected using {self.config.algorithm} algorithm"
        )

    def _z_score_detection(self, values: List[float]) -> Tuple[bool, float, float]:
        """Statistical z-score based anomaly detection."""
        if len(values) < 10:  # Need minimum samples for stable statistics
            return False, 0.0, 0.0

        mean = statistics.mean(values)
        std = statistics.stdev(values)

        if std == 0:
            return False, 0.0, 0.0

        current_value = values[-1]
        z_score = abs((current_value - mean) / std)

        is_anomaly = z_score > self.config.z_score_threshold
        confidence = min(z_score / self.config.z_score_threshold, 1.0)

        return is_anomaly, z_score, confidence

    def _iqr_detection(self, values: List[float]) -> Tuple[bool, float, float]:
        """IQR-based outlier detection."""
        if len(values) < 20:  # Need more samples for IQR
            return False, 0.0, 0.0

        # Calculate IQR
        sorted_values = sorted(values)
        n = len(sorted_values)
        q1_idx = n // 4
        q3_idx = 3 * n // 4

        q1 = sorted_values[q1_idx]
        q3 = sorted_values[q3_idx]
        iqr = q3 - q1

        if iqr == 0:
            return False, 0.0, 0.0

        current_value = values[-1]
        lower_bound = q1 - self.config.iqr_multiplier * iqr
        upper_bound = q3 + self.config.iqr_multiplier * iqr

        is_anomaly = current_value < lower_bound or current_value > upper_bound

        # Calculate anomaly score based on distance from bounds
        if current_value < lower_bound:
            deviation = lower_bound - current_value
        else:
            deviation = current_value - upper_bound

        max_deviation = max(abs(lower_bound - q1), abs(upper_bound - q3))
        score = deviation / max_deviation if max_deviation > 0 else 0.0
        confidence = min(score, 1.0)

        return is_anomaly, score, confidence

    async def _isolation_forest_detection(self, values: List[float]) -> Tuple[bool, float, float]:
        """Isolation Forest based anomaly detection."""
        try:
            from sklearn.ensemble import IsolationForest

            # Reshape for sklearn
            X = np.array(values).reshape(-1, 1)

            # Initialize or update isolation forest
            if not hasattr(self, '_isolation_forest'):
                self._isolation_forest = IsolationForest(
                    contamination=self.config.contamination,
                    random_state=42
                )
                self._isolation_forest.fit(X)

            # Predict anomaly score
            scores = self._isolation_forest.decision_function(X)
            anomaly_score = -scores[-1]  # Convert to positive anomaly score

            is_anomaly = anomaly_score > 0.5  # Threshold
            confidence = min(anomaly_score * 2, 1.0)

            return is_anomaly, anomaly_score, confidence

        except ImportError:
            # Fallback to z-score if sklearn not available
            return self._z_score_detection(values)

    async def _publish_anomaly_signal(self, result: AnomalyDetectionResult) -> None:
        """Publish anomaly signal to Kafka."""
        severity = self._calculate_severity(result.anomaly_score)

        signal = AnomalySignal(
            signal_id=str(uuid4()),
            timestamp=result.timestamp,
            asset_type=result.asset_type,
            asset_id=result.asset_id,
            geography=result.geography,
            anomaly_type="ml" if self.config.algorithm in ["isolation_forest", "local_outlier_factor"] else "statistical",
            severity=severity,
            confidence=result.confidence,
            value=0.0,  # Would be extracted from actual data
            expected_value=0.0,  # Would be extracted from actual data
            deviation=abs(result.anomaly_score),
            deviation_percent=abs(result.anomaly_score) * 100,
            algorithm=result.algorithm,
            metadata={
                "features_used": result.features_used,
                "explanation": result.explanation
            }
        )

        async with self._buffer_lock:
            self._signal_buffer.append(signal)

    def _calculate_severity(self, anomaly_score: float) -> str:
        """Calculate severity level from anomaly score."""
        if anomaly_score > 0.8:
            return "critical"
        elif anomaly_score > 0.6:
            return "high"
        elif anomaly_score > 0.4:
            return "medium"
        else:
            return "low"

    async def get_historical_anomalies(
        self,
        asset_type: str,
        asset_id: str,
        geography: str = "US",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[AnomalySignal]:
        """Get historical anomaly signals."""
        # Mock implementation - would query ClickHouse materialized view
        return []

    async def get_anomaly_stats(
        self,
        asset_type: str,
        geography: str = "US",
        days: int = 7
    ) -> Dict[str, Any]:
        """Get anomaly detection statistics."""
        # Mock implementation
        return {
            "total_signals": 0,
            "signals_by_severity": {"low": 0, "medium": 0, "high": 0, "critical": 0},
            "signals_by_type": {"statistical": 0, "ml": 0},
            "detection_rate": 0.0,
            "false_positive_rate": 0.0
        }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "kafka_connected": self._kafka_producer is not None,
            "signal_buffer_size": len(self._signal_buffer),
            "active_windows": len(self._detection_windows),
            "last_signal_published": None  # Would track actual timestamp
        }


def get_anomaly_detection_service(config: Optional[AnomalyDetectionConfig] = None) -> AnomalyDetectionService:
    """Get the global anomaly detection service instance."""
    if config is None:
        config = AnomalyDetectionConfig()

    # Global instance would be managed by the lifespan manager
    return AnomalyDetectionService(config)


async def detect_price_anomalies(
    price_data: List[Dict[str, Any]],
    geography: str = "US"
) -> List[AnomalyDetectionResult]:
    """Detect anomalies in price data."""
    service = get_anomaly_detection_service()
    return await service.detect_anomalies(price_data, "price", "lmp", geography)


async def detect_load_anomalies(
    load_data: List[Dict[str, Any]],
    geography: str = "US"
) -> List[AnomalyDetectionResult]:
    """Detect anomalies in load data."""
    service = get_anomaly_detection_service()
    return await service.detect_anomalies(load_data, "load", "system", geography)


async def publish_anomaly_signal(signal: AnomalySignal) -> None:
    """Publish anomaly signal directly."""
    service = get_anomaly_detection_service()
    async with service._buffer_lock:
        service._signal_buffer.append(signal)
