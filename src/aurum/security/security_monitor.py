"""Security monitoring and anomaly detection system."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass, field

from aurum.observability.metrics import (
    increment_alerts_fired,
    increment_alerts_suppressed,
    observe_alert_processing_duration,
    PROMETHEUS_AVAILABLE,
)

logger = logging.getLogger(__name__)


class SecurityEventType(str, Enum):
    """Types of security events."""
    SUSPICIOUS_LOGIN = "suspicious_login"
    BRUTE_FORCE_ATTACK = "brute_force_attack"
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    DATA_EXFILTRATION = "data_exfiltration"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    ANOMALOUS_BEHAVIOR = "anomalous_behavior"
    RATE_LIMIT_VIOLATION = "rate_limit_violation"
    SESSION_HIJACKING = "session_hijacking"
    MALICIOUS_PAYLOAD = "malicious_payload"
    CROSS_TENANT_ACCESS = "cross_tenant_access"


class SecuritySeverity(str, Enum):
    """Security event severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyType(str, Enum):
    """Types of anomalies detected."""
    UNUSUAL_LOGIN_TIME = "unusual_login_time"
    UNUSUAL_ACCESS_PATTERN = "unusual_access_pattern"
    UNUSUAL_DATA_VOLUME = "unusual_data_volume"
    UNUSUAL_ERROR_RATE = "unusual_error_rate"
    UNUSUAL_SESSION_DURATION = "unusual_session_duration"
    GEOGRAPHIC_ANOMALY = "geographic_anomaly"
    DEVICE_ANOMALY = "device_anomaly"
    USER_BEHAVIOR_ANOMALY = "user_behavior_anomaly"


@dataclass
class SecurityEvent:
    """Security event record."""
    event_id: str
    event_type: SecurityEventType
    severity: SecuritySeverity
    timestamp: datetime
    user_id: Optional[str]
    tenant_id: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]
    resource: str
    operation: str
    details: Dict[str, Any]
    risk_score: float  # 0.0 to 1.0
    confidence: float  # 0.0 to 1.0
    source: str = "security_monitor"


@dataclass
class AnomalyDetectionResult:
    """Result of anomaly detection."""
    anomaly_type: AnomalyType
    severity: SecuritySeverity
    score: float  # Anomaly score (0.0 to 1.0)
    confidence: float  # Detection confidence (0.0 to 1.0)
    baseline_value: float
    observed_value: float
    threshold: float
    context: Dict[str, Any]
    timestamp: datetime


class UserBehaviorProfile:
    """Profile of normal user behavior for anomaly detection."""

    def __init__(self, user_id: str, tenant_id: Optional[str] = None):
        self.user_id = user_id
        self.tenant_id = tenant_id

        # Login patterns
        self.login_times: deque[datetime] = deque(maxlen=100)
        self.login_ips: deque[str] = deque(maxlen=50)
        self.login_locations: deque[str] = deque(maxlen=20)

        # Access patterns
        self.access_patterns: Dict[str, deque[datetime]] = defaultdict(lambda: deque(maxlen=100))
        self.error_rates: deque[float] = deque(maxlen=50)
        self.session_durations: deque[float] = deque(maxlen=50)

        # Data access patterns
        self.data_access_volumes: deque[int] = deque(maxlen=30)
        self.data_access_patterns: Dict[str, deque[datetime]] = defaultdict(lambda: deque(maxlen=50))

        # Timestamps
        self.last_updated = datetime.now()
        self.created_at = datetime.now()

    def update_login_pattern(self, timestamp: datetime, ip_address: str, location: str) -> None:
        """Update login behavior profile."""
        self.login_times.append(timestamp)
        self.login_ips.append(ip_address)
        self.login_locations.append(location)
        self.last_updated = datetime.now()

    def update_access_pattern(self, resource: str, timestamp: datetime) -> None:
        """Update resource access pattern."""
        self.access_patterns[resource].append(timestamp)
        self.last_updated = datetime.now()

    def update_error_rate(self, error_rate: float) -> None:
        """Update error rate pattern."""
        self.error_rates.append(error_rate)
        self.last_updated = datetime.now()

    def update_session_duration(self, duration_seconds: float) -> None:
        """Update session duration pattern."""
        self.session_durations.append(duration_seconds)
        self.last_updated = datetime.now()

    def update_data_access(self, resource: str, volume: int, timestamp: datetime) -> None:
        """Update data access pattern."""
        self.data_access_volumes.append(volume)
        self.data_access_patterns[resource].append(timestamp)
        self.last_updated = datetime.now()

    def is_anomalous_login_time(self, timestamp: datetime) -> float:
        """Check if login time is anomalous."""
        if len(self.login_times) < 5:
            return 0.0  # Insufficient data

        # Simple heuristic: check if login is outside normal hours (9 AM - 6 PM)
        hour = timestamp.hour
        normal_hours = range(9, 18)  # 9 AM to 6 PM

        if hour not in normal_hours:
            # Check if this user has logged in outside normal hours before
            unusual_logins = sum(1 for dt in self.login_times if dt.hour not in normal_hours)
            unusual_ratio = unusual_logins / len(self.login_times)

            if unusual_ratio < 0.1:  # Less than 10% of logins are outside normal hours
                return 0.8  # High anomaly score

        return 0.0  # Normal

    def is_anomalous_location(self, ip_address: str) -> float:
        """Check if login location is anomalous."""
        if len(self.login_ips) < 3:
            return 0.0  # Insufficient data

        # Check if IP is new
        if ip_address not in self.login_ips:
            # This is the first time seeing this IP
            return 0.7  # Medium-high anomaly score

        return 0.0  # Known IP

    def is_anomalous_access_pattern(self, resource: str, timestamp: datetime) -> float:
        """Check if resource access pattern is anomalous."""
        access_times = self.access_patterns[resource]

        if len(access_times) < 5:
            return 0.0  # Insufficient data

        # Calculate access frequency
        time_diffs = []
        for i in range(1, len(access_times)):
            diff = (access_times[i] - access_times[i-1]).total_seconds()
            time_diffs.append(diff)

        if not time_diffs:
            return 0.0

        avg_interval = sum(time_diffs) / len(time_diffs)

        # Check if current access is much more frequent than usual
        if time_diffs and len(time_diffs) >= 2:
            last_interval = time_diffs[-1]
            if last_interval < (avg_interval * 0.1):  # 10x faster than average
                return 0.8  # High anomaly score

        return 0.0  # Normal

    def get_average_error_rate(self) -> float:
        """Get average error rate."""
        if not self.error_rates:
            return 0.0
        return sum(self.error_rates) / len(self.error_rates)

    def get_average_session_duration(self) -> float:
        """Get average session duration."""
        if not self.session_durations:
            return 0.0
        return sum(self.session_durations) / len(self.session_durations)

    def get_average_data_volume(self) -> float:
        """Get average data access volume."""
        if not self.data_access_volumes:
            return 0.0
        return sum(self.data_access_volumes) / len(self.data_access_volumes)


class SecurityMonitor:
    """Comprehensive security monitoring and anomaly detection."""

    def __init__(self):
        self.user_profiles: Dict[str, UserBehaviorProfile] = {}
        self.security_events: List[SecurityEvent] = []
        self.anomaly_detection_enabled = True
        self.intrusion_detection_enabled = True

        # Rate limiting for security events
        self.event_rate_limits: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

        # Suspicious activity tracking
        self.suspicious_activities: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Anomaly thresholds
        self.anomaly_thresholds = {
            AnomalyType.UNUSUAL_LOGIN_TIME: 0.7,
            AnomalyType.GEOGRAPHIC_ANOMALY: 0.8,
            AnomalyType.UNUSUAL_ACCESS_PATTERN: 0.6,
            AnomalyType.UNUSUAL_DATA_VOLUME: 0.7,
            AnomalyType.UNUSUAL_ERROR_RATE: 0.5,
            AnomalyType.UNUSUAL_SESSION_DURATION: 0.6,
        }

    def get_or_create_user_profile(self, user_id: str, tenant_id: Optional[str] = None) -> UserBehaviorProfile:
        """Get or create user behavior profile."""
        profile_key = f"{user_id}:{tenant_id}" if tenant_id else user_id

        if profile_key not in self.user_profiles:
            self.user_profiles[profile_key] = UserBehaviorProfile(user_id, tenant_id)

        return self.user_profiles[profile_key]

    def record_login_event(
        self,
        user_id: str,
        tenant_id: Optional[str],
        ip_address: str,
        user_agent: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[SecurityEvent]:
        """Record login event and check for anomalies."""
        if not self.anomaly_detection_enabled:
            return None

        timestamp = timestamp or datetime.now()
        profile = self.get_or_create_user_profile(user_id, tenant_id)

        # Update profile
        profile.update_login_pattern(timestamp, ip_address, "unknown")  # Location would be resolved from IP

        # Check for anomalies
        anomalies = []

        # Login time anomaly
        login_time_score = profile.is_anomalous_login_time(timestamp)
        if login_time_score > self.anomaly_thresholds[AnomalyType.UNUSUAL_LOGIN_TIME]:
            anomalies.append(AnomalyDetectionResult(
                anomaly_type=AnomalyType.UNUSUAL_LOGIN_TIME,
                severity=SecuritySeverity.MEDIUM,
                score=login_time_score,
                confidence=0.8,
                baseline_value=0.0,  # Normal during business hours
                observed_value=1.0,  # Outside business hours
                threshold=self.anomaly_thresholds[AnomalyType.UNUSUAL_LOGIN_TIME],
                context={"timestamp": timestamp.isoformat(), "hour": timestamp.hour},
                timestamp=timestamp
            ))

        # Geographic anomaly
        geo_score = profile.is_anomalous_location(ip_address)
        if geo_score > self.anomaly_thresholds[AnomalyType.GEOGRAPHIC_ANOMALY]:
            anomalies.append(AnomalyDetectionResult(
                anomaly_type=AnomalyType.GEOGRAPHIC_ANOMALY,
                severity=SecuritySeverity.HIGH,
                score=geo_score,
                confidence=0.9,
                baseline_value=0.0,  # Known location
                observed_value=1.0,  # Unknown location
                threshold=self.anomaly_thresholds[AnomalyType.GEOGRAPHIC_ANOMALY],
                context={"ip_address": ip_address, "known_ips": list(profile.login_ips)},
                timestamp=timestamp
            ))

        # Create security events for anomalies
        for anomaly in anomalies:
            event = SecurityEvent(
                event_id=f"security-{timestamp.timestamp()}-{hash(str(anomaly.to_dict()))}",
                event_type=SecurityEventType.ANOMALOUS_BEHAVIOR,
                severity=anomaly.severity,
                timestamp=timestamp,
                user_id=user_id,
                tenant_id=tenant_id,
                ip_address=ip_address,
                user_agent=user_agent,
                resource="authentication",
                operation="login",
                details=anomaly.context,
                risk_score=anomaly.score,
                confidence=anomaly.confidence,
                source="security_monitor"
            )

            self.security_events.append(event)
            return event

        return None

    def record_api_access(
        self,
        user_id: str,
        tenant_id: Optional[str],
        method: str,
        path: str,
        status_code: int,
        duration_seconds: float,
        ip_address: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> Optional[SecurityEvent]:
        """Record API access and check for anomalies."""
        if not self.anomaly_detection_enabled:
            return None

        timestamp = timestamp or datetime.now()
        profile = self.get_or_create_user_profile(user_id, tenant_id)

        # Update profile
        profile.update_access_pattern(path, timestamp)

        # Check for rate limiting violations
        if self._check_rate_limit_violation(user_id, path, timestamp):
            event = SecurityEvent(
                event_id=f"ratelimit-{timestamp.timestamp()}-{user_id}",
                event_type=SecurityEventType.RATE_LIMIT_VIOLATION,
                severity=SecuritySeverity.MEDIUM,
                timestamp=timestamp,
                user_id=user_id,
                tenant_id=tenant_id,
                ip_address=ip_address or "",
                user_agent="",
                resource=path,
                operation=method,
                details={"status_code": status_code, "duration_seconds": duration_seconds},
                risk_score=0.6,
                confidence=0.9,
                source="security_monitor"
            )

            self.security_events.append(event)
            return event

        return None

    def record_data_access(
        self,
        user_id: str,
        tenant_id: Optional[str],
        resource: str,
        operation: str,
        record_count: int,
        timestamp: Optional[datetime] = None
    ) -> Optional[SecurityEvent]:
        """Record data access and check for anomalies."""
        if not self.anomaly_detection_enabled:
            return None

        timestamp = timestamp or datetime.now()
        profile = self.get_or_create_user_profile(user_id, tenant_id)

        # Update profile
        profile.update_data_access(resource, record_count, timestamp)

        # Check for unusual data volume
        avg_volume = profile.get_average_data_volume()
        if avg_volume > 0:
            volume_ratio = record_count / avg_volume
            if volume_ratio > 5.0:  # 5x normal volume
                event = SecurityEvent(
                    event_id=f"data-volume-{timestamp.timestamp()}-{user_id}",
                    event_type=SecurityEventType.DATA_EXFILTRATION,
                    severity=SecuritySeverity.HIGH,
                    timestamp=timestamp,
                    user_id=user_id,
                    tenant_id=tenant_id,
                    ip_address="",
                    user_agent="",
                    resource=resource,
                    operation=operation,
                    details={
                        "record_count": record_count,
                        "average_volume": avg_volume,
                        "volume_ratio": volume_ratio
                    },
                    risk_score=0.8,
                    confidence=0.7,
                    source="security_monitor"
                )

                self.security_events.append(event)
                return event

        return None

    def _check_rate_limit_violation(self, user_id: str, resource: str, timestamp: datetime) -> bool:
        """Check if user is violating rate limits."""
        rate_limit_key = f"{user_id}:{resource}"
        access_times = self.event_rate_limits[rate_limit_key]

        # Simple rate limiting: check requests in last 60 seconds
        cutoff_time = timestamp - timedelta(seconds=60)
        recent_requests = [t for t in access_times if t > cutoff_time]

        # Allow up to 10 requests per minute
        return len(recent_requests) > 10

    async def detect_intrusion(self, context: Dict[str, Any]) -> Optional[SecurityEvent]:
        """Detect potential intrusion attempts."""
        if not self.intrusion_detection_enabled:
            return None

        # Check for common intrusion patterns
        ip_address = context.get("ip_address")
        user_id = context.get("user_id")

        if ip_address and user_id:
            # Check for rapid failed login attempts
            failed_logins = self.suspicious_activities[f"failed_login:{ip_address}"]

            if len(failed_logins) > 5:  # 5 failed logins
                # Check if within short time window
                recent_failures = [f for f in failed_logins if datetime.now() - f["timestamp"] < timedelta(minutes=5)]
                if len(recent_failures) > 3:
                    event = SecurityEvent(
                        event_id=f"intrusion-{datetime.now().timestamp()}-{ip_address}",
                        event_type=SecurityEventType.BRUTE_FORCE_ATTACK,
                        severity=SecuritySeverity.CRITICAL,
                        timestamp=datetime.now(),
                        user_id=user_id,
                        tenant_id=context.get("tenant_id"),
                        ip_address=ip_address,
                        user_agent=context.get("user_agent"),
                        resource="authentication",
                        operation="login",
                        details={
                            "failed_attempts": len(recent_failures),
                            "time_window_minutes": 5
                        },
                        risk_score=0.9,
                        confidence=0.95,
                        source="intrusion_detection"
                    )

                    return event

        return None

    def get_security_summary(self) -> Dict[str, Any]:
        """Get security monitoring summary."""
        recent_events = [
            event for event in self.security_events
            if datetime.now() - event.timestamp < timedelta(hours=24)
        ]

        return {
            "total_users_monitored": len(self.user_profiles),
            "total_security_events": len(self.security_events),
            "recent_events_24h": len(recent_events),
            "events_by_severity": {
                severity.value: len([e for e in recent_events if e.severity == severity])
                for severity in SecuritySeverity
            },
            "events_by_type": {
                event_type.value: len([e for e in recent_events if e.event_type == event_type])
                for event_type in SecurityEventType
            },
            "high_risk_events": len([e for e in recent_events if e.risk_score > 0.8]),
            "suspicious_activities": dict(self.suspicious_activities),
            "anomaly_detection_enabled": self.anomaly_detection_enabled,
            "intrusion_detection_enabled": self.intrusion_detection_enabled
        }

    def get_anomaly_summary(self) -> Dict[str, Any]:
        """Get anomaly detection summary."""
        recent_events = [
            event for event in self.security_events
            if event.event_type == SecurityEventType.ANOMALOUS_BEHAVIOR
            and datetime.now() - event.timestamp < timedelta(hours=24)
        ]

        return {
            "total_anomalies_detected": len(recent_events),
            "anomalies_by_type": {},
            "users_with_anomalies": len(set(e.user_id for e in recent_events if e.user_id)),
            "high_confidence_anomalies": len([e for e in recent_events if e.confidence > 0.8])
        }


# Global security monitor instance
_security_monitor: Optional[SecurityMonitor] = None


def get_security_monitor() -> SecurityMonitor:
    """Get global security monitor instance."""
    global _security_monitor
    if _security_monitor is None:
        _security_monitor = SecurityMonitor()
    return _security_monitor


async def record_security_event(event: SecurityEvent) -> None:
    """Record a security event."""
    monitor = get_security_monitor()
    monitor.security_events.append(event)

    # Record in Prometheus
    if PROMETHEUS_AVAILABLE:
        await increment_alerts_fired(
            event.event_type.value,
            event.severity.value,
            "security_monitor"
        )

    logger.warning(
        "Security event recorded",
        extra={
            "event_type": event.event_type.value,
            "severity": event.severity.value,
            "user_id": event.user_id,
            "tenant_id": event.tenant_id,
            "risk_score": event.risk_score,
            "confidence": event.confidence
        }
    )


async def process_api_security_check(
    user_id: str,
    tenant_id: Optional[str],
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float,
    ip_address: Optional[str] = None
) -> None:
    """Process API request for security monitoring."""
    monitor = get_security_monitor()
    await monitor.record_api_access(
        user_id=user_id,
        tenant_id=tenant_id,
        method=method,
        path=path,
        status_code=status_code,
        duration_seconds=duration_seconds,
        ip_address=ip_address
    )


async def process_login_security_check(
    user_id: str,
    tenant_id: Optional[str],
    ip_address: str,
    user_agent: str,
    success: bool
) -> Optional[SecurityEvent]:
    """Process login attempt for security monitoring."""
    monitor = get_security_monitor()

    if success:
        return await monitor.record_login_event(
            user_id=user_id,
            tenant_id=tenant_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
    else:
        # Record failed login attempt
        suspicious_key = f"failed_login:{ip_address}"
        monitor.suspicious_activities[suspicious_key].append({
            "user_id": user_id,
            "timestamp": datetime.now(),
            "user_agent": user_agent
        })

        # Check for intrusion
        intrusion_event = await monitor.detect_intrusion({
            "user_id": user_id,
            "tenant_id": tenant_id,
            "ip_address": ip_address,
            "user_agent": user_agent
        })

        return intrusion_event
