"""Security audit logging and monitoring for security events."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path

from ..telemetry.context import get_request_id, get_correlation_id, get_tenant_id, log_structured


# Audit logger
AUDIT_LOGGER = logging.getLogger("aurum.security.audit")


class SecurityAudit:
    """Security audit system for tracking security events."""

    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or "security_audit.log"
        self._ensure_log_file()

    def _ensure_log_file(self):
        """Ensure the audit log file exists and is writable."""
        try:
            Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
            Path(self.log_file).touch()
        except Exception as e:
            # Fallback to console logging if file logging fails
            print(f"Failed to create audit log file {self.log_file}: {e}")

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
        """Log a security event with structured data.

        Args:
            event_type: Type of security event (auth_success, auth_failure, access_denied, etc.)
            user_id: User identifier
            tenant_id: Tenant identifier
            resource: Resource being accessed
            action: Action being performed
            ip_address: Client IP address
            user_agent: Client user agent
            details: Additional event details
            severity: Event severity (debug, info, warning, error, critical)
        """
        # Get context from telemetry
        request_id = get_request_id()
        correlation_id = get_correlation_id()
        context_tenant_id = get_tenant_id()

        # Use context tenant_id if not provided
        tenant_id = tenant_id or context_tenant_id

        # Build audit event
        audit_event = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "severity": severity,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "resource": resource,
            "action": action,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "request_id": request_id,
            "correlation_id": correlation_id,
            "details": details or {}
        }

        # Log to structured logger
        log_structured(
            severity,
            "security_audit",
            **audit_event
        )

        # Log to audit file
        self._log_to_file(audit_event)

    def log_auth_success(
        self,
        user_id: str,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        auth_method: str = "jwt"
    ) -> None:
        """Log successful authentication."""
        self.log_security_event(
            event_type="auth_success",
            user_id=user_id,
            tenant_id=tenant_id,
            action="authenticate",
            ip_address=ip_address,
            user_agent=user_agent,
            details={"auth_method": auth_method},
            severity="info"
        )

    def log_auth_failure(
        self,
        reason: str,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        auth_method: str = "jwt"
    ) -> None:
        """Log failed authentication."""
        self.log_security_event(
            event_type="auth_failure",
            user_id=user_id,
            tenant_id=tenant_id,
            action="authenticate",
            ip_address=ip_address,
            user_agent=user_agent,
            details={"auth_method": auth_method, "reason": reason},
            severity="warning"
        )

    def log_access_denied(
        self,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        resource: str = "",
        action: str = "",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        reason: Optional[str] = None
    ) -> None:
        """Log access denied event."""
        self.log_security_event(
            event_type="access_denied",
            user_id=user_id,
            tenant_id=tenant_id,
            resource=resource,
            action=action,
            ip_address=ip_address,
            user_agent=user_agent,
            details={"reason": reason},
            severity="warning"
        )

    def log_suspicious_activity(
        self,
        activity_type: str,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log suspicious activity."""
        self.log_security_event(
            event_type="suspicious_activity",
            user_id=user_id,
            tenant_id=tenant_id,
            action=activity_type,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details,
            severity="error"
        )

    def log_rate_limit_violation(
        self,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        endpoint: str = "",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        limit_type: str = "requests_per_minute"
    ) -> None:
        """Log rate limit violation."""
        self.log_security_event(
            event_type="rate_limit_violation",
            user_id=user_id,
            tenant_id=tenant_id,
            resource=endpoint,
            action="rate_limited",
            ip_address=ip_address,
            user_agent=user_agent,
            details={"limit_type": limit_type},
            severity="warning"
        )

    def log_security_incident(
        self,
        incident_type: str,
        severity: str,
        description: str,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a security incident."""
        self.log_security_event(
            event_type="security_incident",
            user_id=user_id,
            tenant_id=tenant_id,
            action=incident_type,
            ip_address=ip_address,
            details={
                "description": description,
                "evidence": evidence,
                "incident_severity": severity
            },
            severity=severity
        )

    def _log_to_file(self, audit_event: Dict[str, Any]) -> None:
        """Log audit event to file."""
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                # Write JSON line
                json_line = json.dumps(audit_event, default=str, separators=(',', ':'))
                f.write(json_line + '\n')
        except Exception as e:
            # Fallback to console if file logging fails
            print(f"Failed to write to audit log: {e}")
            AUDIT_LOGGER.error(f"Failed to write audit event to file: {e}")


# Global audit instance
_audit_instance: Optional[SecurityAudit] = None


def get_audit_logger() -> SecurityAudit:
    """Get the global security audit logger."""
    global _audit_instance
    if _audit_instance is None:
        _audit_instance = SecurityAudit()
    return _audit_instance


def log_security_event(*args, **kwargs) -> None:
    """Convenience function to log security events."""
    get_audit_logger().log_security_event(*args, **kwargs)


def log_auth_success(*args, **kwargs) -> None:
    """Convenience function to log auth success."""
    get_audit_logger().log_auth_success(*args, **kwargs)


def log_auth_failure(*args, **kwargs) -> None:
    """Convenience function to log auth failure."""
    get_audit_logger().log_auth_failure(*args, **kwargs)


def log_access_denied(*args, **kwargs) -> None:
    """Convenience function to log access denied."""
    get_audit_logger().log_access_denied(*args, **kwargs)


def log_suspicious_activity(*args, **kwargs) -> None:
    """Convenience function to log suspicious activity."""
    get_audit_logger().log_suspicious_activity(*args, **kwargs)


def log_rate_limit_violation(*args, **kwargs) -> None:
    """Convenience function to log rate limit violation."""
    get_audit_logger().log_rate_limit_violation(*args, **kwargs)


def log_security_incident(*args, **kwargs) -> None:
    """Convenience function to log security incident."""
    get_audit_logger().log_security_incident(*args, **kwargs)
