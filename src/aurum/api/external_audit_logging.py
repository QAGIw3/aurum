"""
Audit logging for external data API routes.

This module provides comprehensive audit logging for all external data operations
including API calls, data access, and administrative actions.

Environment:
- `AURUM_AUDIT_LOG_DIR`: directory to write audit/compliance/security logs. If not
  writable, logs fall back to stdout.

Notes:
- A correlation ID is ensured for every request (header `x-correlation-id` or
  generated) and echoed on the response.
- Loggers: `audit.external_data` (info/warn), `compliance.external_data` (info),
  `security.external_data` (warning) — suitable for forwarding to SIEM.
"""

import json
import logging
import os
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
from uuid import uuid4

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aurum.core.settings import (
    ExternalAuditSettings,
    AuditSinkType,
    AuditKafkaSettings,
    AuditClickHouseSettings,
)

LOGGER = logging.getLogger(__name__)

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


class AuditSink:
    """Base interface for pluggable audit sinks."""

    name = "audit-sink"

    def is_enabled(self) -> bool:  # pragma: no cover - simple default
        return True

    def emit(self, event: AuditEvent, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class KafkaAuditSink(AuditSink):
    """Emit audit events to Kafka topics."""

    name = "kafka"

    def __init__(self, config: AuditKafkaSettings):
        self.config = config
        self._producer = None
        self._driver: Optional[str] = None
        self._enabled = False
        self._lock = threading.Lock()

        bootstrap = (config.bootstrap_servers or "").strip()
        if not bootstrap:
            LOGGER.info("Kafka audit sink disabled: bootstrap servers not configured")
            return

        try:
            from confluent_kafka import Producer  # type: ignore[import]

            conf = {
                "bootstrap.servers": bootstrap,
                "client.id": config.client_id,
                "enable.idempotence": True,
                "compression.codec": (config.compression or "gzip"),
                "acks": config.acks,
                "security.protocol": config.security_protocol,
            }
            if config.sasl_username and config.sasl_password:
                conf.update({
                    "sasl.username": config.sasl_username,
                    "sasl.password": config.sasl_password,
                })
                if config.sasl_mechanism:
                    conf["sasl.mechanism"] = config.sasl_mechanism
            try:
                self._producer = Producer(conf)
                self._driver = "confluent"
                self._enabled = True
                LOGGER.info("Kafka audit sink initialised using confluent-kafka")
                return
            except Exception:  # pragma: no cover - runtime dependency failure
                LOGGER.warning("Failed to initialise confluent-kafka producer for audit sink", exc_info=True)
        except ImportError:
            Producer = None  # type: ignore[assignment]  # pragma: no cover - import path only

        try:
            from kafka import KafkaProducer  # type: ignore[import]

            kwargs: Dict[str, Any] = {
                "bootstrap_servers": [server.strip() for server in bootstrap.split(",") if server.strip()],
                "client_id": config.client_id,
                "value_serializer": lambda value: value,
                "compression_type": config.compression,
                "acks": config.acks,
            }
            if config.security_protocol:
                kwargs["security_protocol"] = config.security_protocol
            if config.sasl_username and config.sasl_password:
                kwargs["sasl_plain_username"] = config.sasl_username
                kwargs["sasl_plain_password"] = config.sasl_password
            if config.sasl_mechanism:
                kwargs["sasl_mechanism"] = config.sasl_mechanism
            try:
                self._producer = KafkaProducer(**{k: v for k, v in kwargs.items() if v is not None})
                self._driver = "kafka-python"
                self._enabled = True
                LOGGER.info("Kafka audit sink initialised using kafka-python")
            except Exception:  # pragma: no cover - runtime dependency failure
                LOGGER.warning("Failed to initialise kafka-python producer for audit sink", exc_info=True)
        except ImportError:
            KafkaProducer = None  # type: ignore[assignment]  # pragma: no cover

        if not self._enabled:
            LOGGER.warning(
                "Kafka audit sink requested but no Kafka client libraries are available; sink disabled."
            )

    def is_enabled(self) -> bool:
        return bool(self._enabled and self._producer)

    def emit(self, event: AuditEvent, payload: Dict[str, Any]) -> None:
        if not self.is_enabled():
            return
        message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        try:
            with self._lock:
                if self._driver == "confluent":
                    try:
                        self._producer.produce(self.config.topic, message)
                    finally:
                        self._producer.poll(0)
                else:
                    future = self._producer.send(self.config.topic, message)
                    if hasattr(future, "add_errback"):
                        future.add_errback(lambda exc: LOGGER.debug("Kafka audit sink send failed: %s", exc))
        except BufferError:  # pragma: no cover - bounded buffer backpressure
            LOGGER.warning("Kafka audit sink buffer full; dropping audit event")
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.warning("Kafka audit sink failed to emit event", exc_info=True)


class ClickHouseAuditSink(AuditSink):
    """Emit audit events to ClickHouse over HTTP JSONEachRow."""

    name = "clickhouse"

    def __init__(self, config: AuditClickHouseSettings):
        self.config = config
        self._session = None
        self._enabled = False
        endpoint = (config.endpoint or "").rstrip("/")
        if not endpoint:
            LOGGER.info("ClickHouse audit sink disabled: endpoint not configured")
            return
        try:
            import requests  # type: ignore[import]
        except ImportError:
            LOGGER.warning("ClickHouse audit sink requested but 'requests' is not installed; sink disabled.")
            return

        session = requests.Session()
        if config.username and config.password:
            session.auth = (config.username, config.password)
        session.headers.update({"Content-Type": "application/json"})
        self._requests = requests
        self._session = session
        self._endpoint = endpoint
        self._enabled = True

    def is_enabled(self) -> bool:
        return bool(self._enabled and self._session)

    def emit(self, event: AuditEvent, payload: Dict[str, Any]) -> None:
        if not self.is_enabled():
            return

        table = f"{self.config.database}.{self.config.table}"
        payload_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        query = f"INSERT INTO {table} FORMAT JSONEachRow"

        try:
            response = self._session.post(
                f"{self._endpoint}/",
                params={"query": query},
                data=f"{payload_line}\n",
                timeout=self.config.timeout_seconds,
            )
            if response.status_code >= 400:
                LOGGER.warning(
                    "ClickHouse audit sink failed with status %s: %s",
                    response.status_code,
                    response.text[:256],
                )
        except self._requests.RequestException:  # type: ignore[attr-defined]
            LOGGER.warning("ClickHouse audit sink request failed", exc_info=True)

class ExternalDataAuditLogger:
    """Enhanced audit logger for external data operations with compliance support."""

    def __init__(self, config: ExternalAuditSettings | None = None):
        self.config = config or ExternalAuditSettings()
        self.audit_logger = logging.getLogger("audit.external_data")
        self.audit_logger.setLevel(logging.INFO)

        # Add handler for audit logs if not already present
        if not self.audit_logger.handlers:
            self.audit_logger.addHandler(self._build_handler("external_data_audit.log"))

        # Compliance logging
        self.compliance_logger = logging.getLogger("compliance.external_data")
        self.compliance_logger.setLevel(logging.INFO)

        if not self.compliance_logger.handlers:
            self.compliance_logger.addHandler(self._build_handler("compliance_audit.log"))

        # Security event logging
        self.security_logger = logging.getLogger("security.external_data")
        self.security_logger.setLevel(logging.WARNING)

        if not self.security_logger.handlers:
            self.security_logger.addHandler(self._build_handler("security_events.log"))

        self._sinks: list[AuditSink] = []
        self._configure_sinks()

    def _build_handler(self, filename: str) -> logging.Handler:
        """Create a log handler, preferring a writable directory.

        Uses AURUM_AUDIT_LOG_DIR if set, else tries /var/log/aurum, else falls back to stdout.
        """
        log_dir = self.config.log_dir or "/var/log/aurum"
        try:
            path = Path(log_dir)
            path.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(str(path / filename))
        except Exception:
            handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        return handler

    def register_sink(self, sink: AuditSink) -> None:
        """Register an additional sink for downstream audit delivery."""
        self._sinks.append(sink)
        LOGGER.info("Registered audit sink %s", getattr(sink, "name", sink.__class__.__name__))

    def _configure_sinks(self) -> None:
        for sink_type in self.config.sinks:
            if sink_type is AuditSinkType.FILE:
                continue
            try:
                if sink_type is AuditSinkType.KAFKA:
                    sink = KafkaAuditSink(self.config.kafka)
                elif sink_type is AuditSinkType.CLICKHOUSE:
                    sink = ClickHouseAuditSink(self.config.clickhouse)
                else:
                    LOGGER.warning("Unknown audit sink '%s'; skipping", sink_type)
                    continue
            except Exception:
                LOGGER.warning("Failed to initialise audit sink '%s'", sink_type, exc_info=True)
                continue
            if sink.is_enabled():
                self.register_sink(sink)
            else:
                LOGGER.info(
                    "Audit sink %s disabled by configuration",
                    getattr(sink, "name", sink_type),
                )

    def _dispatch_to_sinks(self, event: AuditEvent, payload: Dict[str, Any]) -> None:
        if not self._sinks:
            return
        for sink in self._sinks:
            try:
                sink.emit(event, payload)
            except Exception:
                LOGGER.warning(
                    "Audit sink %s failed to emit event %s",
                    getattr(sink, "name", sink.__class__.__name__),
                    event.event_id,
                    exc_info=True,
                )

    def log_event(self, event: AuditEvent):
        """Log an audit event."""
        payload = event.model_dump(mode="json")
        message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

        if event.status == "failure":
            self.audit_logger.warning(message)
        else:
            self.audit_logger.info(message)

        self._dispatch_to_sinks(event, payload)

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
        """Log a security event (high-level API)."""
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
        self._emit_security_event(event)

    def _emit_compliance_event(self, event: AuditEvent):
        """Log a compliance-related audit event."""
        payload = event.model_dump(mode="json")
        payload["compliance"] = True
        self.compliance_logger.info(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        self._dispatch_to_sinks(event, payload)

    # Public helper to log compliance events; used by callers expecting a public API
    def log_compliance_event(self, event: AuditEvent):
        """Public wrapper for emitting compliance events.

        Some call sites reference `log_compliance_event`; provide a stable
        public method that delegates to the internal emitter.
        """
        self._emit_compliance_event(event)

    def _emit_security_event(self, event: AuditEvent):
        """Log a security-related audit event (low-level)."""
        payload = event.model_dump(mode="json")
        payload["security"] = True
        self.security_logger.warning(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        self._dispatch_to_sinks(event, payload)

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
        self._emit_security_event(event)

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
        self._emit_security_event(event)

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
        self._emit_compliance_event(event)

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
            self._emit_security_event(event)
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


def configure_external_audit_logger(config: ExternalAuditSettings) -> ExternalDataAuditLogger:
    """Configure the global audit logger with the provided settings."""
    global audit_logger
    audit_logger = ExternalDataAuditLogger(config)
    return audit_logger

async def audit_middleware(request: Request, call_next):
    """FastAPI middleware for audit logging.

    Ensures every request receives a correlation ID and is logged regardless of
    whether the incoming request provided one.
    """
    # Resolve or create correlation ID
    correlation_id = request.headers.get("x-correlation-id") or str(uuid4())

    response = await call_next(request)

    # Ensure correlation ID is present on the response
    try:
        response.headers["x-correlation-id"] = correlation_id
    except Exception:
        pass

    # Log the API call
    try:
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
    except Exception:
        # Never break the request due to logging errors
        pass

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
