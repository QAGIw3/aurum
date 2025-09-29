"""Event-driven architecture primitives (event store, streaming, sagas)."""

from .event_store import (
    AppendResult,
    EventRecord,
    EventStore,
    ExpectedVersion,
    NewEvent,
    SnapshotRecord,
    TimescaleEventStore,
)

__all__ = [
    "AppendResult",
    "EventRecord",
    "EventStore",
    "ExpectedVersion",
    "NewEvent",
    "SnapshotRecord",
    "TimescaleEventStore",
]
