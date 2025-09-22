"""
Audit logging for external data API routes.

This module provides comprehensive audit logging for all external data operations
including API calls, data access, and administrative actions.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from uuid import uuid4

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import structlog

logger = structlog.get_logger(__name__)

class AuditEvent(BaseModel):
    """Model for audit log events."""
    event_id: str
    timestamp: datetime
    event_type: str  # api_call, data_access, admin_action, security_event
    user_id: Optional[str]
    service: str = "external-data"
    resource: str
    operation: str
    status: str  # success, failure, warning
    details: Dict[str, Any]
    ip_address: Optional[str]
    user_agent: Optional[str]
    session_id: Optional[str]
    correlation_id: Optional[str]

class ExternalDataAuditLogger:
    """Enhanced audit logger for external data operations with compliance support."""

    def __init__(self):
        self.audit_logger = logging.getLogger("audit.external_data")
        self.audit_logger.setLevel(logging.INFO)

        # Add handler for audit logs if not already present
        if not self.audit_logger.handlers:
            handler = logging.FileHandler("/var/log/aurum/external_data_audit.log")
            handler.setFormatter(
                logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            )
            self.audit_logger.addHandler(handler)

        # Compliance logging
        self.compliance_logger = logging.getLogger("compliance.external_data")
        self.compliance_logger.setLevel(logging.INFO)

        if not self.compliance_logger.handlers:
            compliance_handler = logging.FileHandler("/var/log/aurum/compliance_audit.log")
            compliance_handler.setFormatter(
                logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            )
            self.compliance_logger.addHandler(compliance_handler)

        # Security event logging
        self.security_logger = logging.getLogger("security.external_data")
        self.security_logger.setLevel(logging.WARNING)

        if not self.security_logger.handlers:
            security_handler = logging.FileHandler("/var/log/aurum/security_events.log")
            security_handler.setFormatter(
                logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            )
            self.security_logger.addHandler(security_handler)

    def log_event(self, event: AuditEvent):
        """Log an audit event."""
        log_entry = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "event_type": event.event_type,
            "user_id": event.user_id,
            "service": event.service,
            "resource": event.resource,
            "operation": event.operation,
            "status": event.status,
            "details": event.details,
            "ip_address": event.ip_address,
            "user_agent": event.user_agent,
            "session_id": event.session_id,
            "correlation_id": event.correlation_id,
        }

        if event.status == "failure":
            self.audit_logger.warning(json.dumps(log_entry))
        else:
            self.audit_logger.info(json.dumps(log_entry))

    def log_api_call(
        self,
        request: Request,
        response: Response,
        user_id: Optional[str] = None,
        resource: str = "api",
        operation: str = "unknown",
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        """Log an API call audit event."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="api_call",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status="success" if response.status_code < 400 else "failure",
            details=details or {},
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            session_id=request.headers.get("session-id"),
            correlation_id=correlation_id or request.headers.get("x-correlation-id")
        )
        self.log_event(event)

    def log_data_access(
        self,
        user_id: Optional[str],
        resource: str,
        operation: str,
        details: Dict[str, Any],
        status: str = "success",
        correlation_id: Optional[str] = None
    ):
        """Log a data access audit event."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="data_access",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status=status,
            details=details,
            correlation_id=correlation_id
        )
        self.log_event(event)

    def log_admin_action(
        self,
        user_id: str,
        resource: str,
        operation: str,
        details: Dict[str, Any],
        status: str = "success",
        correlation_id: Optional[str] = None
    ):
        """Log an administrative action audit event."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="admin_action",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status=status,
            details=details,
            correlation_id=correlation_id
        )
        self.log_event(event)

    def log_security_event(
        self,
        event_type: str,
        details: Dict[str, Any],
        user_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ):
        """Log a security event."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="security_event",
            user_id=user_id,
            resource="security",
            operation=event_type,
            status="warning",
            details=details,
            correlation_id=correlation_id
        )
        self.log_event(event)

    def log_compliance_event(self, event: AuditEvent):
        """Log a compliance-related audit event."""
        log_entry = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "event_type": event.event_type,
            "user_id": event.user_id,
            "service": event.service,
            "resource": event.resource,
            "operation": event.operation,
            "status": event.status,
            "details": event.details,
            "ip_address": event.ip_address,
            "user_agent": event.user_agent,
            "session_id": event.session_id,
            "correlation_id": event.correlation_id,
            "compliance": True
        }

        self.compliance_logger.info(json.dumps(log_entry))

    def log_security_event(self, event: AuditEvent):
        """Log a security-related audit event."""
        log_entry = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "event_type": event.event_type,
            "user_id": event.user_id,
            "service": event.service,
            "resource": event.resource,
            "operation": event.operation,
            "status": event.status,
            "details": event.details,
            "ip_address": event.ip_address,
            "user_agent": event.user_agent,
            "session_id": event.session_id,
            "correlation_id": event.correlation_id,
            "security": True
        }

        self.security_logger.warning(json.dumps(log_entry))

    def log_tenant_isolation_violation(
        self,
        user_id: str,
        tenant_id: str,
        attempted_tenant_id: str,
        resource: str,
        operation: str,
        details: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        """Log tenant isolation violation."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="tenant_isolation_violation",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status="failure",
            details={
                "user_tenant_id": tenant_id,
                "attempted_tenant_id": attempted_tenant_id,
                **details
            },
            correlation_id=correlation_id
        )
        self.log_security_event(event)

    def log_privilege_escalation_attempt(
        self,
        user_id: str,
        requested_permission: str,
        user_permissions: List[str],
        resource: str,
        operation: str,
        details: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        """Log privilege escalation attempt."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="privilege_escalation",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status="failure",
            details={
                "requested_permission": requested_permission,
                "user_permissions": user_permissions,
                **details
            },
            correlation_id=correlation_id
        )
        self.log_security_event(event)

    def log_data_classification_access(
        self,
        user_id: str,
        tenant_id: str,
        resource: str,
        classification_level: str,
        operation: str,
        details: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        """Log data classification access for compliance."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="data_classification_access",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status="success",
            details={
                "tenant_id": tenant_id,
                "classification_level": classification_level,
                **details
            },
            correlation_id=correlation_id
        )
        self.log_compliance_event(event)

    def log_admin_action_audit(
        self,
        user_id: str,
        tenant_id: str,
        action: str,
        resource: str,
        details: Dict[str, Any],
        status: str = "success",
        correlation_id: Optional[str] = None
    ):
        """Log administrative action for audit trail."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="admin_action",
            user_id=user_id,
            resource=resource,
            operation=action,
            status=status,
            details={
                "tenant_id": tenant_id,
                **details
            },
            correlation_id=correlation_id
        )
        self.log_compliance_event(event)

    def log_cross_tenant_data_access(
        self,
        user_id: str,
        user_tenant_id: str,
        accessed_tenant_id: str,
        resource: str,
        operation: str,
        details: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        """Log cross-tenant data access attempts."""
        event = AuditEvent(
            event_id=str(uuid4()),
            timestamp=datetime.now(timezone.utc),
            event_type="cross_tenant_access",
            user_id=user_id,
            resource=resource,
            operation=operation,
            status="failure" if user_tenant_id != accessed_tenant_id else "success",
            details={
                "user_tenant_id": user_tenant_id,
                "accessed_tenant_id": accessed_tenant_id,
                **details
            },
            correlation_id=correlation_id
        )

        if user_tenant_id != accessed_tenant_id:
            self.log_security_event(event)
        else:
            self.log_event(event)

    def get_audit_report(
        self,
        start_time: datetime,
        end_time: datetime,
        event_types: Optional[List[str]] = None,
        user_ids: Optional[List[str]] = None,
        tenant_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Generate audit report for compliance."""
        # This would normally query audit logs from storage
        # For now, return placeholder implementation
        return []

    def get_security_incident_report(
        self,
        start_time: datetime,
        end_time: datetime,
        severity_levels: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Generate security incident report."""
        # This would normally query security events from storage
        # For now, return placeholder implementation
        return []


# Global audit logger instance
audit_logger = ExternalDataAuditLogger()

async def audit_middleware(request: Request, call_next):
    """FastAPI middleware for audit logging."""
    # Generate correlation ID if not present
    correlation_id = request.headers.get("x-correlation-id")
    if not correlation_id:
        correlation_id = str(uuid4())
        # Add to response headers
        response = await call_next(request)
        response.headers["x-correlation-id"] = correlation_id
        return response

    response = await call_next(request)

    # Log the API call
    audit_logger.log_api_call(
        request=request,
        response=response,
        user_id=getattr(request.state, "user_id", None),
        resource=f"{request.method} {request.url.path}",
        operation=request.url.path,
        details={
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "response_status": response.status_code,
            "response_size": len(response.body) if hasattr(response, "body") else 0
        },
        correlation_id=correlation_id
    )

    return response

def log_mapping_change(
    user_id: str,
    provider: str,
    series_id: str,
    curve_key: str,
    operation: str,
    details: Dict[str, Any]
):
    """Log series-curve mapping changes."""
    audit_logger.log_admin_action(
        user_id=user_id,
        resource=f"mapping:{provider}:{series_id}",
        operation=operation,
        details=details,
        status="success"
    )

def log_external_data_access(
    user_id: str,
    provider: str,
    dataset: str,
    operation: str,
    record_count: int,
    details: Dict[str, Any]
):
    """Log external data access events."""
    audit_logger.log_data_access(
        user_id=user_id,
        resource=f"external:{provider}:{dataset}",
        operation=operation,
        details={
            "record_count": record_count,
            **details
        },
        status="success"
    )

def log_security_violation(
    violation_type: str,
    details: Dict[str, Any],
    user_id: Optional[str] = None,
    correlation_id: Optional[str] = None
):
    """Log security violations."""
    audit_logger.log_security_event(
        event_type=violation_type,
        details=details,
        user_id=user_id,
        correlation_id=correlation_id
    )
