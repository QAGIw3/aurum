
"""Checkpoint stores for external data collectors."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

try:  # Optional dependency - imported lazily for typing only
    import psycopg  # type: ignore
except Exception:  # pragma: no cover - allow usage without psycopg installed
    psycopg = None  # type: ignore[assignment]

try:  # Optional dependency - redis client
    import redis  # type: ignore
except Exception:  # pragma: no cover - allow usage without redis installed
    redis = None  # type: ignore[assignment]

try:  # Optional dependency - watermark store
    from ...data_ingestion.watermark_store import WatermarkStore
except Exception:  # pragma: no cover - allow usage without watermark store
    WatermarkStore = None  # type: ignore[assignment]


@dataclass
class Checkpoint:
    """Serialized checkpoint state for a provider/series pair."""

    provider: str
    series_id: str
    last_timestamp: Optional[datetime] = None
    last_page: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def copy(self, **updates: Any) -> "Checkpoint":
        state = {
            "provider": self.provider,
            "series_id": self.series_id,
            "last_timestamp": self.last_timestamp,
            "last_page": self.last_page,
            "metadata": dict(self.metadata),
        }
        state.update(updates)
        return Checkpoint(**state)


class CheckpointStore:
    """Abstract checkpoint store interface."""

    def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:  # pragma: no cover - interface
        raise NotImplementedError

    def set(self, checkpoint: Checkpoint) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class RedisCheckpointStore(CheckpointStore):
    """Checkpoint store backed by Redis hashes."""

    def __init__(self, client: Any, *, namespace: str = "aurum:collect:checkpoint") -> None:
        self._client = client
        self._namespace = namespace.rstrip(":")

    def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:
        key = self._key(provider, series_id)
        raw = self._client.hgetall(key)
        if not raw:
            return None
        decoded = {
            self._decode(key_bytes): self._decode(value)
            for key_bytes, value in raw.items()
        }
        last_timestamp = self._parse_timestamp(decoded.get("last_timestamp"))
        last_page = decoded.get("last_page") or None
        metadata_str = decoded.get("metadata")
        metadata = self._parse_metadata(metadata_str)
        return Checkpoint(
            provider=provider,
            series_id=series_id,
            last_timestamp=last_timestamp,
            last_page=last_page,
            metadata=metadata,
        )

    def set(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.provider, checkpoint.series_id)
        payload = {
            "last_timestamp": checkpoint.last_timestamp.isoformat()
            if checkpoint.last_timestamp
            else "",
            "last_page": checkpoint.last_page or "",
            "metadata": json.dumps(checkpoint.metadata or {}),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._client.hset(key, mapping=payload)

    def delete(self, provider: str, series_id: str) -> None:
        key = self._key(provider, series_id)
        self._client.delete(key)

    def _key(self, provider: str, series_id: str) -> str:
        provider_slug = provider.replace(":", "_")
        series_slug = series_id.replace(":", "_")
        return f"{self._namespace}:{provider_slug}:{series_slug}"

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _parse_metadata(value: Optional[str]) -> Dict[str, Any]:
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}


class WatermarkCheckpointStore(CheckpointStore):
    """Checkpoint store backed by the centralized watermark store."""

    def __init__(self, watermark_store: Optional[Any] = None) -> None:
        if WatermarkStore is None:
            raise RuntimeError("WatermarkStore not available")
        self._watermark_store = watermark_store or WatermarkStore()

    async def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:
        """Get checkpoint from watermark store."""
        # Use provider as source and series_id as table
        watermark = await self._watermark_store.get_watermark(provider, series_id)
        if watermark is None:
            return None

        # Create checkpoint from watermark
        return Checkpoint(
            provider=provider,
            series_id=series_id,
            last_timestamp=watermark if isinstance(watermark, datetime) else None,
            last_page=None,
            metadata={"watermark_value": str(watermark) if not isinstance(watermark, datetime) else None}
        )

    async def set(self, checkpoint: Checkpoint) -> None:
        """Set checkpoint in watermark store."""
        # Use current timestamp as watermark value for checkpoints
        current_time = datetime.now(timezone.utc)
        metadata = dict(checkpoint.metadata)
        metadata.update({
            "checkpoint_provider": checkpoint.provider,
            "checkpoint_series_id": checkpoint.series_id,
            "checkpoint_last_page": checkpoint.last_page,
        })

        # Set watermark with current timestamp
        await self._watermark_store.set_watermark(
            source=checkpoint.provider,
            table=checkpoint.series_id,
            value=current_time,
            metadata=metadata
        )

    async def delete(self, provider: str, series_id: str) -> None:
        """Delete checkpoint from watermark store."""
        # Note: WatermarkStore doesn't have a delete method, so we'll set a very old watermark
        old_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        await self._watermark_store.set_watermark(
            source=provider,
            table=series_id,
            value=old_time,
            metadata={"deleted": True}
        )


class PostgresCheckpointStore(CheckpointStore):
    """Checkpoint store backed by Postgres."""

    TABLE_NAME = "external_collect_checkpoints"

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        connection_factory: Optional[Callable[[], Any]] = None,
        ensure_schema: bool = True,
    ) -> None:
        if connection_factory is not None:
            self._connection_factory = connection_factory
            self._manage_close = False
        else:
            if psycopg is None:  # pragma: no cover - runtime guard
                raise RuntimeError("psycopg is required for PostgresCheckpointStore")
            resolved_dsn = dsn or os.getenv("AURUM_APP_DB_DSN") or self._default_dsn()

            def factory() -> Any:
                return psycopg.connect(resolved_dsn)  # type: ignore[attr-defined]

            self._connection_factory = factory
            self._manage_close = True

        if ensure_schema:
            self._ensure_table()

    async def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:
        query = f"""
            SELECT last_timestamp, last_page, metadata
            FROM {self.TABLE_NAME}
            WHERE provider = %s AND series_id = %s
        """
        async with self._connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (provider, series_id))
                row = await cur.fetchone()
        if not row:
            return None
        last_timestamp, last_page, metadata = row
        if metadata is None:
            metadata_dict: Dict[str, Any] = {}
        elif isinstance(metadata, str):
            metadata_dict = self._parse_metadata(metadata)
        else:
            metadata_dict = dict(metadata)
        return Checkpoint(
            provider=provider,
            series_id=series_id,
            last_timestamp=last_timestamp,
            last_page=last_page,
            metadata=metadata_dict,
        )

    async def set(self, checkpoint: Checkpoint) -> None:
        query = f"""
            INSERT INTO {self.TABLE_NAME} (provider, series_id, last_timestamp, last_page, metadata, updated_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, now())
            ON CONFLICT (provider, series_id)
            DO UPDATE SET
                last_timestamp = EXCLUDED.last_timestamp,
                last_page = EXCLUDED.last_page,
                metadata = EXCLUDED.metadata,
                updated_at = now()
        """
        metadata_json = json.dumps(checkpoint.metadata or {})
        async with self._connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    query,
                    (
                        checkpoint.provider,
                        checkpoint.series_id,
                        checkpoint.last_timestamp,
                        checkpoint.last_page,
                        metadata_json,
                    ),
                )
            if hasattr(conn, "commit"):
                await conn.commit()

    async def delete(self, provider: str, series_id: str) -> None:
        query = f"DELETE FROM {self.TABLE_NAME} WHERE provider = %s AND series_id = %s"
        async with self._connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (provider, series_id))
            if hasattr(conn, "commit"):
                await conn.commit()

    async def _ensure_table(self) -> None:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                provider TEXT NOT NULL,
                series_id TEXT NOT NULL,
                last_timestamp TIMESTAMPTZ,
                last_page TEXT,
                metadata JSONB DEFAULT '{{}}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (provider, series_id)
            )
        """
        async with self._connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(ddl)
            if hasattr(conn, "commit"):
                await conn.commit()

    @asynccontextmanager
    async def _connection(self):
        conn = self._connection_factory()
        try:
            yield conn
        finally:
            if self._manage_close and hasattr(conn, "close"):
                await conn.close()

    @staticmethod
    def _default_dsn() -> str:
        user = os.getenv("POSTGRES_USER", "aurum")
        password = os.getenv("POSTGRES_PASSWORD", "aurum")
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "aurum")
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"

    @staticmethod
    def _parse_metadata(value: str) -> Dict[str, Any]:
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}


__all__ = [
    "Checkpoint",
    "CheckpointStore",
    "PostgresCheckpointStore",
    "RedisCheckpointStore",
]
