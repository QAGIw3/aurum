from datetime import datetime, timezone

import pytest

from aurum.api.external_audit_logging import (
    AuditEvent,
    AuditSink,
    ExternalDataAuditLogger,
    configure_external_audit_logger,
)
from aurum.core.settings import ExternalAuditSettings, AuditSinkType


class _StubSink(AuditSink):
    def __init__(self) -> None:
        self.events = []

    def emit(self, event: AuditEvent, payload: dict) -> None:  # type: ignore[override]
        self.events.append((event, payload))


def _base_event(**overrides) -> AuditEvent:
    defaults = {
        "event_id": "evt-1",
        "timestamp": datetime.now(timezone.utc),
        "event_type": "api_call",
        "user_id": "user-123",
        "service": "external-data",
        "resource": "GET /v1/external",
        "operation": "GET /v1/external",
        "status": "success",
        "details": {"source": "test"},
        "ip_address": "127.0.0.1",
        "user_agent": "pytest",
        "session_id": "session-abc",
        "correlation_id": "corr-xyz",
    }
    defaults.update(overrides)
    return AuditEvent(**defaults)


def test_log_event_dispatches_registered_sinks():
    settings = ExternalAuditSettings(
        enabled=True,
        sinks=(AuditSinkType.FILE,),
    )
    audit_logger = ExternalDataAuditLogger(settings)
    stub_sink = _StubSink()
    audit_logger.register_sink(stub_sink)

    audit_logger.log_event(_base_event())

    assert stub_sink.events, "expected sink to receive at least one event"
    _, payload = stub_sink.events[0]
    assert payload["event_id"] == "evt-1"
    assert payload["event_type"] == "api_call"


def test_configure_external_audit_logger_updates_global():
    import aurum.api.external_audit_logging as module

    original_logger = module.audit_logger
    try:
        settings = ExternalAuditSettings(
            enabled=True,
            sinks=(AuditSinkType.FILE,),
        )
        configured = configure_external_audit_logger(settings)
        assert module.audit_logger is configured
        assert module.audit_logger.config.enabled is True
    finally:
        module.audit_logger = original_logger
