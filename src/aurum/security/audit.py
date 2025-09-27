"""Enhanced security audit logging and monitoring for Phase 4 enterprise compliance."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

from ..telemetry.context import (
    get_correlation_id,
    get_request_id,
    get_tenant_id,
    log_structured,
)

# Audit logger
AUDIT_LOGGER = logging.getLogger("aurum.security.audit")


class AuditEventType(str, Enum):
    """Enhanced audit event types for enterprise compliance."""
    # Authentication events
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    SESSION_EXPIRED = "session_expired"
    MFA_CHALLENGE = "mfa_challenge"
    MFA_SUCCESS = "mfa_success"
    MFA_FAILURE = "mfa_failure"
    PASSWORD_CHANGE = "password_change"
    ACCOUNT_LOCKED = "account_locked"
    
    # Authorization events  
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REVOKED = "role_revoked"
    
    # Data access events
    DATA_ACCESS = "data_access"
    DATA_EXPORT = "data_export"
    DATA_MODIFICATION = "data_modification"
    DATA_DELETION = "data_deletion"
    BULK_OPERATION = "bulk_operation"
    
    # System events
    SYSTEM_CONFIG_CHANGE = "system_config_change"
    SECURITY_POLICY_CHANGE = "security_policy_change"
    TENANT_CONFIG_CHANGE = "tenant_config_change"
    FEATURE_FLAG_CHANGE = "feature_flag_change"
    
    # Compliance events
    COMPLIANCE_VIOLATION = "compliance_violation"
    AUDIT_LOG_ACCESS = "audit_log_access"
    RETENTION_POLICY_APPLIED = "retention_policy_applied"
    DATA_BREACH_DETECTED = "data_breach_detected"
    
    # Administrative events
    USER_CREATED = "user_created"
    USER_DELETED = "user_deleted"
    USER_MODIFIED = "user_modified"
    TENANT_CREATED = "tenant_created"
    TENANT_DELETED = "tenant_deleted"
    TENANT_MODIFIED = "tenant_modified"


class AuditSeverity(str, Enum):
    """Audit event severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Structured audit event for enterprise compliance."""
    
    event_id: str
    event_type: AuditEventType
    timestamp: datetime
    severity: AuditSeverity
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    resource: Optional[str] = None
    action: Optional[str] = None
    outcome: str = "success"  # success, failure, unknown
    details: Optional[Dict[str, Any]] = None
    correlation_id: Optional[str] = None
    request_id: Optional[str] = None
    compliance_relevant: bool = True
    retention_period_days: int = 2555  # 7 years default for compliance
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)


class EnterpriseSecurityAudit:
    """Enhanced security audit system for enterprise compliance (Phase 4)."""

    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or "security_audit.log"
        self.audit_events: List[AuditEvent] = []
        self.compliance_alerts: List[Dict[str, Any]] = []
        self._ensure_log_file()
        self._event_counter = 0

    def _ensure_log_file(self):
        """Ensure the audit log file exists and is writable."""
        try:
            Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
            Path(self.log_file).touch()
        except Exception as e:
            # Fallback to console logging if file logging fails
            print(f"Failed to create audit log file {self.log_file}: {e}")

    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        self._event_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"audit_{timestamp}_{self._event_counter:06d}"

    def log_audit_event(
        self,
        event_type: AuditEventType,
        severity: AuditSeverity = AuditSeverity.MEDIUM,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        session_id: Optional[str] = None,
        resource: Optional[str] = None,
        action: Optional[str] = None,
        outcome: str = "success",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        compliance_relevant: bool = True
    ) -> AuditEvent:
        """Log an enhanced audit event with full enterprise compliance data.
        
        Creates a comprehensive audit event with all necessary information
        for enterprise compliance requirements (SOC2, GDPR, etc.)
        """
        # Get context from telemetry
        request_id = get_request_id()
        correlation_id = get_correlation_id()
        context_tenant_id = get_tenant_id()

        # Use context tenant_id if not provided
        tenant_id = tenant_id or context_tenant_id

        # Create audit event
        audit_event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=event_type,
            timestamp=datetime.now(),
            severity=severity,
            user_id=user_id,
            tenant_id=tenant_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent,
            resource=resource,
            action=action,
            outcome=outcome,
            details=details or {},
            correlation_id=correlation_id,
            request_id=request_id,
            compliance_relevant=compliance_relevant
        )

        # Store event for analysis
        self.audit_events.append(audit_event)

        # Log to structured logger
        log_structured(
            severity.value,
            "enterprise_security_audit",
            **audit_event.to_dict()
        )

        # Log to audit file
        self._log_to_file(audit_event.to_dict())

        # Check for compliance violations
        self._check_compliance_violations(audit_event)

        return audit_event

    def _check_compliance_violations(self, event: AuditEvent) -> None:
        """Check for potential compliance violations."""
        violations = []
        
        # Check for suspicious patterns
        if event.event_type == AuditEventType.LOGIN_FAILURE:
            recent_failures = self._count_recent_events(
                AuditEventType.LOGIN_FAILURE, 
                event.user_id, 
                minutes=10
            )
            if recent_failures > 5:
                violations.append("Excessive login failures detected")
        
        # Check for privilege escalation
        if event.event_type == AuditEventType.PRIVILEGE_ESCALATION:
            violations.append("Privilege escalation event requires review")
        
        # Check for data export during off-hours
        if event.event_type == AuditEventType.DATA_EXPORT:
            if event.timestamp.hour < 6 or event.timestamp.hour > 22:
                violations.append("Data export during off-hours")
        
        # Log violations
        for violation in violations:
            self.compliance_alerts.append({
                "alert_id": f"alert_{self._generate_event_id()}",
                "timestamp": datetime.now().isoformat(),
                "severity": "high",
                "violation": violation,
                "related_event": event.event_id,
                "requires_review": True
            })

    def _count_recent_events(self, event_type: AuditEventType, user_id: str, minutes: int) -> int:
        """Count recent events of specific type for a user."""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        return len([
            event for event in self.audit_events
            if (event.event_type == event_type and 
                event.user_id == user_id and 
                event.timestamp > cutoff)
        ])

    def get_compliance_report(self, tenant_id: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate compliance report for a tenant."""
        tenant_events = [
            event for event in self.audit_events
            if (event.tenant_id == tenant_id and 
                start_date <= event.timestamp <= end_date and
                event.compliance_relevant)
        ]
        
        # Categorize events
        event_summary = {}
        for event in tenant_events:
            event_type = event.event_type.value
            if event_type not in event_summary:
                event_summary[event_type] = {"count": 0, "failures": 0}
            event_summary[event_type]["count"] += 1
            if event.outcome == "failure":
                event_summary[event_type]["failures"] += 1
        
        # Get compliance alerts for this tenant
        tenant_alerts = [
            alert for alert in self.compliance_alerts
            if any(event.tenant_id == tenant_id for event in tenant_events 
                   if event.event_id == alert.get("related_event"))
        ]
        
        return {
            "tenant_id": tenant_id,
            "report_period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "total_events": len(tenant_events),
            "event_summary": event_summary,
            "compliance_alerts": tenant_alerts,
            "risk_score": self._calculate_risk_score(tenant_events, tenant_alerts)
        }

    def _calculate_risk_score(self, events: List[AuditEvent], alerts: List[Dict[str, Any]]) -> float:
        """Calculate risk score based on events and alerts."""
        if not events:
            return 0.0
        
        # Base score from alert count
        alert_score = len(alerts) * 10
        
        # Add score for failed events
        failed_events = len([e for e in events if e.outcome == "failure"])
        failure_score = (failed_events / len(events)) * 50
        
        # Add score for critical events
        critical_events = len([e for e in events if e.severity == AuditSeverity.CRITICAL])
        critical_score = critical_events * 5
        
        total_score = min(alert_score + failure_score + critical_score, 100.0)
        return round(total_score, 2)

    # Legacy compatibility method
    def log_security_event(
        self,
        event_type: str,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        resource: str = "",
        action: str = "",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        severity: str = "info"
    ) -> None:
        """Legacy method for backward compatibility."""
        # Convert to new enum format
        try:
            audit_event_type = AuditEventType(event_type)
        except ValueError:
            audit_event_type = AuditEventType.DATA_ACCESS  # Default
        
        try:
            audit_severity = AuditSeverity(severity)
        except ValueError:
            audit_severity = AuditSeverity.MEDIUM  # Default
        
        self.log_audit_event(
            event_type=audit_event_type,
            severity=audit_severity,
            user_id=user_id,
            tenant_id=tenant_id,
            resource=resource,
            action=action,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details
        )

    def _log_to_file(self, audit_event: Dict[str, Any]) -> None:
        """Log audit event to file."""
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(audit_event, default=str) + '\n')
        except Exception as e:
            AUDIT_LOGGER.error(f"Failed to write to audit log file: {e}")

    # Convenience methods for common audit events
    def log_auth_success(
        self,
        user_id: str,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        auth_method: str = "jwt"
    ) -> AuditEvent:
        """Log successful authentication."""
        return self.log_audit_event(
            event_type=AuditEventType.LOGIN_SUCCESS,
            severity=AuditSeverity.LOW,
            user_id=user_id,
            tenant_id=tenant_id,
            session_id=session_id,
            action="authenticate",
            ip_address=ip_address,
            user_agent=user_agent,
            details={"auth_method": auth_method}
        )

    def log_auth_failure(
        self,
        user_id: str,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        reason: str = "invalid_credentials"
    ) -> AuditEvent:
        """Log failed authentication."""
        return self.log_audit_event(
            event_type=AuditEventType.LOGIN_FAILURE,
            severity=AuditSeverity.MEDIUM,
            user_id=user_id,
            tenant_id=tenant_id,
            action="authenticate",
            outcome="failure",
            ip_address=ip_address,
            user_agent=user_agent,
            details={"failure_reason": reason}
        )

    def log_data_access(
        self,
        user_id: str,
        tenant_id: str,
        resource: str,
        action: str,
        record_count: Optional[int] = None,
        ip_address: Optional[str] = None
    ) -> AuditEvent:
        """Log data access event."""
        details = {}
        if record_count is not None:
            details["record_count"] = record_count
            
        return self.log_audit_event(
            event_type=AuditEventType.DATA_ACCESS,
            severity=AuditSeverity.LOW,
            user_id=user_id,
            tenant_id=tenant_id,
            resource=resource,
            action=action,
            ip_address=ip_address,
            details=details
        )

    def log_compliance_violation(
        self,
        user_id: Optional[str],
        tenant_id: str,
        violation_type: str,
        resource: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> AuditEvent:
        """Log compliance violation."""
        violation_details = {"violation_type": violation_type}
        if details:
            violation_details.update(details)
            
        return self.log_audit_event(
            event_type=AuditEventType.COMPLIANCE_VIOLATION,
            severity=AuditSeverity.HIGH,
            user_id=user_id,
            tenant_id=tenant_id,
            resource=resource,
            action="violation_detected",
            outcome="failure",
            details=violation_details
        )


# Global audit instance for backward compatibility
security_audit = EnterpriseSecurityAudit()

# Legacy alias
SecurityAudit = EnterpriseSecurityAudit
