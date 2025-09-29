"""Event store abstractions and TimescaleDB-backed implementation."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Mapping, MutableMapping, Sequence

from aurum.api.database.timescale_client import AsyncTimescaleClient, get_timescale_client
from aurum.observability.tracing import get_current_span_id, get_current_trace_id

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NewEvent:
    """Command-side representation of an event awaiting persistence."""

    event_type: str
    aggregate_type: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any] | None = None
    occurred_at: datetime | None = None
    schema_version: int = 1
    event_id: str | None = None


@dataclass(frozen=True)
class EventRecord:
    """Immutable event record returned from the event store."""

    stream_id: str
    sequence: int
    event_id: str
    event_type: str
    aggregate_type: str
    payload: Any
    metadata: Mapping[str, Any]
    occurred_at: datetime
    recorded_at: datetime
    schema_version: int = 1


@dataclass(frozen=True)
class SnapshotRecord:
    """Snapshot of aggregate state used to accelerate rehydration."""

    stream_id: str
    version: int
    state: Any
    metadata: Mapping[str, Any]
    recorded_at: datetime


@dataclass(frozen=True)
class AppendResult:
    """Outcome of appending a batch of events to a stream."""

    stream_id: str
    events: tuple[EventRecord, ...]
    last_sequence: int


class ExpectedVersion(Enum):
    """Concurrency hints supplied by command handlers."""

    ANY = "any"
    NO_STREAM = "no_stream"


class ConcurrencyError(RuntimeError):
    """Raised when optimistic concurrency expectations are violated."""


class EventStore(ABC):
    """Abstract event store contract."""

    @abstractmethod
    async def append(
        self,
        stream_id: str,
        events: Sequence[NewEvent],
        *,
        expected_version: ExpectedVersion | int = ExpectedVersion.ANY,
    ) -> AppendResult:
        """Append events to a stream atomically."""

    @abstractmethod
    async def load_stream(
        self,
        stream_id: str,
        *,
        after: int = 0,
        limit: int | None = None,
    ) -> Sequence[EventRecord]:
        """Load events for a single stream ordered by sequence."""

    @abstractmethod
    async def load_range(
        self,
        stream_id: str,
        *,
        start: int = 1,
        end: int | None = None,
    ) -> AsyncIterator[EventRecord]:
        """Iterate over a sequence window for a stream."""

    @abstractmethod
    async def store_snapshot(
        self,
        snapshot: SnapshotRecord,
    ) -> None:
        """Persist the latest snapshot for a stream."""

    @abstractmethod
    async def get_snapshot(
        self,
        stream_id: str,
    ) -> SnapshotRecord | None:
        """Return the most recent snapshot for a stream."""


class TimescaleEventStore(EventStore):
    """TimescaleDB-backed event store with optimistic concurrency."""

    _DDL_STATEMENTS: tuple[str, ...] = (
        """
        CREATE TABLE IF NOT EXISTS event_store_events (
            stream_id TEXT NOT NULL,
            sequence BIGINT NOT NULL,
            event_id UUID NOT NULL,
            event_type TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            metadata JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            schema_version INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (stream_id, sequence),
            UNIQUE (event_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS event_store_metadata (
            event_id UUID PRIMARY KEY,
            correlation_id TEXT,
            causation_id TEXT,
            trace_id TEXT,
            attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS event_store_snapshots (
            stream_id TEXT PRIMARY KEY,
            version BIGINT NOT NULL,
            state JSONB NOT NULL,
            metadata JSONB NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS event_store_sagas (
            saga_id TEXT PRIMARY KEY,
            saga_type TEXT NOT NULL,
            state JSONB NOT NULL,
            status TEXT NOT NULL,
            version BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    )

    def __init__(
        self,
        client: AsyncTimescaleClient | None = None,
    ) -> None:
        self._client = client or get_timescale_client()
        self._schema_lock = asyncio.Lock()
        self._schema_ready = False

    async def append(
        self,
        stream_id: str,
        events: Sequence[NewEvent],
        *,
        expected_version: ExpectedVersion | int = ExpectedVersion.ANY,
    ) -> AppendResult:
        if not events:
            current = await self._current_version(stream_id)
            return AppendResult(stream_id=stream_id, events=tuple(), last_sequence=current or 0)

        await self._ensure_schema()
        current_version = await self._current_version(stream_id)
        expected_numeric = await self._coerce_expected_version(expected_version, current_version)

        now = datetime.now(timezone.utc)

        await self._client._ensure_pool()  # type: ignore[attr-defined]
        pool = self._client._pool  # type: ignore[attr-defined]
        if pool is None:
            raise RuntimeError("Timescale connection pool is not available")

        persisted: list[EventRecord] = []
        async with pool.connection() as conn:  # type: ignore[attr-defined]
            async with conn.transaction():
                next_sequence = expected_numeric
                for new_event in events:
                    next_sequence += 1
                    event_id = new_event.event_id or str(uuid.uuid4())
                    occurred_at = new_event.occurred_at or now
                    metadata = self._enrich_metadata(new_event.metadata)
                    payload_json = self._json_dumps(new_event.payload)
                    metadata_json = self._json_dumps(metadata)

                    await conn.execute(  # type: ignore[attr-defined]
                        """
                        INSERT INTO event_store_events (
                            stream_id, sequence, event_id, event_type, aggregate_type,
                            payload, metadata, occurred_at, recorded_at, schema_version
                        ) VALUES (%(stream_id)s, %(sequence)s, %(event_id)s, %(event_type)s,
                                 %(aggregate_type)s, %(payload)s::jsonb, %(metadata)s::jsonb,
                                 %(occurred_at)s, %(recorded_at)s, %(schema_version)s)
                        """,
                        {
                            "stream_id": stream_id,
                            "sequence": next_sequence,
                            "event_id": event_id,
                            "event_type": new_event.event_type,
                            "aggregate_type": new_event.aggregate_type,
                            "payload": payload_json,
                            "metadata": metadata_json,
                            "occurred_at": occurred_at,
                            "recorded_at": now,
                            "schema_version": new_event.schema_version,
                        },
                    )

                    await conn.execute(  # type: ignore[attr-defined]
                        """
                        INSERT INTO event_store_metadata (
                            event_id, correlation_id, causation_id, trace_id, attributes
                        ) VALUES (
                            %(event_id)s, %(correlation_id)s, %(causation_id)s, %(trace_id)s,
                            %(attributes)s::jsonb
                        )
                        ON CONFLICT (event_id) DO UPDATE
                        SET correlation_id = EXCLUDED.correlation_id,
                            causation_id = EXCLUDED.causation_id,
                            trace_id = EXCLUDED.trace_id,
                            attributes = EXCLUDED.attributes,
                            recorded_at = NOW()
                        """,
                        {
                            "event_id": event_id,
                            "correlation_id": metadata.get("correlation_id"),
                            "causation_id": metadata.get("causation_id"),
                            "trace_id": metadata.get("trace_id"),
                            "attributes": self._json_dumps(metadata),
                        },
                    )

                    persisted.append(
                        EventRecord(
                            stream_id=stream_id,
                            sequence=next_sequence,
                            event_id=event_id,
                            event_type=new_event.event_type,
                            aggregate_type=new_event.aggregate_type,
                            payload=new_event.payload,
                            metadata=metadata,
                            occurred_at=occurred_at,
                            recorded_at=now,
                            schema_version=new_event.schema_version,
                        )
                    )

        return AppendResult(
            stream_id=stream_id,
            events=tuple(persisted),
            last_sequence=persisted[-1].sequence,
        )

    async def load_stream(
        self,
        stream_id: str,
        *,
        after: int = 0,
        limit: int | None = None,
    ) -> Sequence[EventRecord]:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT stream_id, sequence, event_id, event_type, aggregate_type,
                   payload, metadata, occurred_at, recorded_at, schema_version
            FROM event_store_events
            WHERE stream_id = %(stream_id)s AND sequence > %(after)s
            ORDER BY sequence ASC
            LIMIT %(limit)s
            """,
            {
                "stream_id": stream_id,
                "after": after,
                "limit": limit or 10_000,
            },
        )
        return tuple(self._row_to_event(row) for row in rows)

    async def load_range(
        self,
        stream_id: str,
        *,
        start: int = 1,
        end: int | None = None,
    ) -> AsyncIterator[EventRecord]:
        await self._ensure_schema()
        params = {
            "stream_id": stream_id,
            "start": start,
            "end": end if end is not None else 9_223_372_036_854_775_807,
        }
        sql = """
            SELECT stream_id, sequence, event_id, event_type, aggregate_type,
                   payload, metadata, occurred_at, recorded_at, schema_version
            FROM event_store_events
            WHERE stream_id = %(stream_id)s
              AND sequence BETWEEN %(start)s AND %(end)s
            ORDER BY sequence ASC
        """
        rows = await self._client.execute_query(sql, params)
        for row in rows:
            yield self._row_to_event(row)

    async def store_snapshot(self, snapshot: SnapshotRecord) -> None:
        await self._ensure_schema()
        await self._client.execute(
            """
            INSERT INTO event_store_snapshots (
                stream_id, version, state, metadata, recorded_at
            ) VALUES (
                %(stream_id)s, %(version)s, %(state)s::jsonb, %(metadata)s::jsonb, %(recorded_at)s
            )
            ON CONFLICT (stream_id) DO UPDATE
            SET version = EXCLUDED.version,
                state = EXCLUDED.state,
                metadata = EXCLUDED.metadata,
                recorded_at = EXCLUDED.recorded_at
            """,
            {
                "stream_id": snapshot.stream_id,
                "version": snapshot.version,
                "state": self._json_dumps(snapshot.state),
                "metadata": self._json_dumps(snapshot.metadata),
                "recorded_at": snapshot.recorded_at,
            },
        )

    async def get_snapshot(self, stream_id: str) -> SnapshotRecord | None:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT stream_id, version, state, metadata, recorded_at
            FROM event_store_snapshots
            WHERE stream_id = %(stream_id)s
            """,
            {"stream_id": stream_id},
        )
        if not rows:
            return None
        row = rows[0]
        return SnapshotRecord(
            stream_id=row["stream_id"],
            version=int(row["version"]),
            state=self._json_value(row["state"]),
            metadata=self._json_mapping(row["metadata"]),
            recorded_at=row["recorded_at"],
        )

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            for ddl in self._DDL_STATEMENTS:
                await self._client.execute(ddl)
            self._schema_ready = True

    async def _current_version(self, stream_id: str) -> int | None:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT MAX(sequence) AS max_sequence
            FROM event_store_events
            WHERE stream_id = %(stream_id)s
            """,
            {"stream_id": stream_id},
        )
        if not rows:
            return None
        max_sequence = rows[0].get("max_sequence")
        return int(max_sequence) if max_sequence is not None else None

    async def _coerce_expected_version(
        self,
        expected: ExpectedVersion | int,
        current: int | None,
    ) -> int:
        if isinstance(expected, int):
            if current is None and expected not in {0, -1}:
                raise ConcurrencyError(
                    f"Stream has no events but expected version {expected}"
                )
            if current is not None and current != expected:
                raise ConcurrencyError(
                    f"Expected version {expected} but found {current}"
                )
            return current or expected

        if expected is ExpectedVersion.NO_STREAM:
            if current not in (None, 0):
                raise ConcurrencyError(
                    f"Stream already exists at version {current}"
                )
            return 0

        # ExpectedVersion.ANY
        return current or 0

    def _enrich_metadata(
        self,
        metadata: Mapping[str, Any] | None,
    ) -> MutableMapping[str, Any]:
        enriched: MutableMapping[str, Any]
        if metadata is None:
            enriched = {}
        else:
            enriched = dict(metadata)
        enriched.setdefault("trace_id", get_current_trace_id())
        enriched.setdefault("span_id", get_current_span_id())
        return enriched

    @staticmethod
    def _json_mapping(value: Any) -> Mapping[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        try:
            data = json.loads(value)
            return data if isinstance(data, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _json_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list, tuple)):
            return value
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return value

    @staticmethod
    def _row_to_event(row: Mapping[str, Any]) -> EventRecord:
        return EventRecord(
            stream_id=row["stream_id"],
            sequence=int(row["sequence"]),
            event_id=str(row["event_id"]),
            event_type=row["event_type"],
            aggregate_type=row["aggregate_type"],
            payload=TimescaleEventStore._json_value(row["payload"]),
            metadata=TimescaleEventStore._json_mapping(row["metadata"]),
            occurred_at=row["occurred_at"],
            recorded_at=row["recorded_at"],
            schema_version=int(row["schema_version"]),
        )

    @staticmethod
    def _json_dumps(value: Any) -> str:
        def _default(obj: Any) -> str:
            if isinstance(obj, datetime):
                return obj.isoformat()
            return str(obj)

        return json.dumps(value, default=_default)


__all__ = [
    "AppendResult",
    "ConcurrencyError",
    "EventRecord",
    "EventStore",
    "ExpectedVersion",
    "NewEvent",
    "SnapshotRecord",
    "TimescaleEventStore",
]
