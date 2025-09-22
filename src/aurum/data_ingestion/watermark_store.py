"""Idempotent watermark store for incremental data ingestion."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import redis
from redis.exceptions import ConnectionError, TimeoutError

from ..config import settings
from ..database import get_async_session
from ..models.watermark import Watermark


class WatermarkStore:
    """Store and retrieve watermarks for incremental data ingestion."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """Initialize watermark store with Redis client."""
        self.redis_client = redis_client or redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        self.redis_prefix = "watermark:"

    async def get_watermark(
        self,
        source: str,
        table: str,
        default_value: Optional[Union[str, datetime]] = None
    ) -> Optional[Union[str, datetime]]:
        """Get the latest watermark for a source/table combination."""
        try:
            # Try Redis first for fast access
            redis_key = f"{self.redis_prefix}{source}:{table}"
            watermark_str = self.redis_client.get(redis_key)

            if watermark_str:
                try:
                    return datetime.fromisoformat(watermark_str)
                except ValueError:
                    return watermark_str

        except (ConnectionError, TimeoutError):
            # Fallback to database if Redis is unavailable
            pass

        # Fallback to database
        async with get_async_session() as session:
            watermark = await session.get(Watermark, {"source": source, "table": table})
            if watermark:
                return watermark.last_processed_at or watermark.last_value

        return default_value

    async def set_watermark(
        self,
        source: str,
        table: str,
        value: Union[str, datetime],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Set a watermark value for a source/table combination."""
        try:
            # Store in Redis for fast access
            redis_key = f"{self.redis_prefix}{source}:{table}"

            if isinstance(value, datetime):
                value_str = value.isoformat()
            else:
                value_str = str(value)

            # Set with TTL (24 hours)
            self.redis_client.setex(redis_key, 86400, value_str)

        except (ConnectionError, TimeoutError):
            # Continue without Redis
            pass

        # Store in database for persistence
        async with get_async_session() as session:
            watermark = await session.get(Watermark, {"source": source, "table": table})

            if not watermark:
                watermark = Watermark(
                    source=source,
                    table=table,
                    last_value=value_str if not isinstance(value, datetime) else None,
                    last_processed_at=value if isinstance(value, datetime) else None,
                    metadata=metadata or {},
                )
                session.add(watermark)
            else:
                if isinstance(value, datetime):
                    watermark.last_processed_at = value
                else:
                    watermark.last_value = value_str
                watermark.metadata = metadata or {}
                watermark.updated_at = datetime.now(timezone.utc)

            await session.commit()
            return True

    async def update_watermark_after_processing(
        self,
        source: str,
        table: str,
        processed_count: int,
        success: bool = True,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update watermark after successful data processing."""
        current_time = datetime.now(timezone.utc)

        # Get current watermark
        current_watermark = await self.get_watermark(source, table)
        if not current_watermark:
            current_watermark = current_time - timedelta(hours=24)  # Default to 24 hours ago

        # Prepare metadata
        update_metadata = {
            "processed_count": processed_count,
            "success": success,
            "last_processing_time": current_time.isoformat(),
            **(metadata or {})
        }

        if error_message:
            update_metadata["error_message"] = error_message

        # Set new watermark (typically current time for time-based watermarks)
        return await self.set_watermark(
            source=source,
            table=table,
            value=current_time,
            metadata=update_metadata
        )

    async def get_processing_window(
        self,
        source: str,
        table: str,
        window_hours: int = 24
    ) -> Dict[str, datetime]:
        """Get the processing window for incremental loads."""
        current_watermark = await self.get_watermark(source, table)

        if not current_watermark:
            # No previous watermark, start from beginning
            start_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
            end_time = datetime.now(timezone.utc)
        elif isinstance(current_watermark, datetime):
            start_time = current_watermark
            end_time = datetime.now(timezone.utc)
        else:
            # String-based watermark, need custom logic
            start_time = current_watermark
            end_time = None

        # Don't go back more than the specified window
        min_start_time = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        if start_time < min_start_time:
            start_time = min_start_time

        return {
            "start_time": start_time,
            "end_time": end_time,
        }

    async def get_sources_by_watermark_age(
        self,
        max_age_hours: int = 48
    ) -> List[Dict[str, Any]]:
        """Get sources that haven't been updated recently."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

        # Query database for stale watermarks
        async with get_async_session() as session:
            query = """
            SELECT source, table, updated_at
            FROM watermarks
            WHERE updated_at < :cutoff_time
            ORDER BY updated_at ASC
            """
            result = await session.execute(query, {"cutoff_time": cutoff_time})
            rows = result.fetchall()

        return [
            {
                "source": row.source,
                "table": row.table,
                "last_updated": row.updated_at,
                "hours_since_update": (datetime.now(timezone.utc) - row.updated_at).total_seconds() / 3600
            }
            for row in rows
        ]

    async def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Clean up old watermark records."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

        async with get_async_session() as session:
            result = await session.execute(
                "DELETE FROM watermarks WHERE updated_at < :cutoff_date",
                {"cutoff_date": cutoff_date}
            )
            deleted_count = result.rowcount
            await session.commit()

        return deleted_count

    async def get_watermark_statistics(self) -> Dict[str, Any]:
        """Get statistics about watermark usage."""
        async with get_async_session() as session:
            # Total watermarks
            total_result = await session.execute("SELECT COUNT(*) FROM watermarks")
            total_count = total_result.scalar()

            # Sources by update frequency
            freq_result = await session.execute("""
                SELECT source, COUNT(*) as table_count, MAX(updated_at) as last_update
                FROM watermarks
                GROUP BY source
                ORDER BY last_update DESC
            """)
            sources = freq_result.fetchall()

            # Recent activity (last 24 hours)
            recent_result = await session.execute("""
                SELECT COUNT(*) FROM watermarks
                WHERE updated_at > :since_time
            """, {"since_time": datetime.now(timezone.utc) - timedelta(hours=24)})
            recent_count = recent_result.scalar()

        return {
            "total_watermarks": total_count,
            "sources": [
                {
                    "source": row.source,
                    "table_count": row.table_count,
                    "last_update": row.last_update
                }
                for row in sources
            ],
            "recent_activity_count": recent_count,
        }


# Convenience functions for common patterns
async def get_incremental_window(
    source: str,
    table: str,
    lookback_hours: int = 48,
    default_start: Optional[datetime] = None
) -> Dict[str, datetime]:
    """Get incremental processing window with sensible defaults."""
    store = WatermarkStore()

    if default_start:
        # Use provided default start time
        start_time = default_start
        end_time = datetime.now(timezone.utc)
    else:
        # Get window from watermark store
        window = await store.get_processing_window(source, table, lookback_hours)
        start_time = window["start_time"]
        end_time = window["end_time"]

    return {
        "start_time": start_time,
        "end_time": end_time,
    }


async def update_watermark_success(
    source: str,
    table: str,
    processed_count: int,
    **metadata
) -> bool:
    """Update watermark after successful processing."""
    store = WatermarkStore()
    return await store.update_watermark_after_processing(
        source=source,
        table=table,
        processed_count=processed_count,
        success=True,
        metadata=metadata
    )


async def update_watermark_failure(
    source: str,
    table: str,
    error_message: str,
    **metadata
) -> bool:
    """Update watermark after failed processing."""
    store = WatermarkStore()
    return await store.update_watermark_after_processing(
        source=source,
        table=table,
        processed_count=0,
        success=False,
        error_message=error_message,
        metadata=metadata
    )


# Data source specific watermark helpers
class EIAWatermarkStore:
    """Specialized watermark store for EIA data sources."""

    def __init__(self):
        self.store = WatermarkStore()

    async def get_series_watermark(self, series_id: str) -> Optional[datetime]:
        """Get watermark for EIA series."""
        return await self.store.get_watermark("eia", f"series_{series_id}")

    async def set_series_watermark(self, series_id: str, timestamp: datetime) -> bool:
        """Set watermark for EIA series."""
        return await self.store.set_watermark(
            "eia",
            f"series_{series_id}",
            timestamp,
            metadata={"series_id": series_id, "data_source": "eia"}
        )


class FREDWatermarkStore:
    """Specialized watermark store for FRED data sources."""

    def __init__(self):
        self.store = WatermarkStore()

    async def get_series_watermark(self, series_id: str) -> Optional[datetime]:
        """Get watermark for FRED series."""
        return await self.store.get_watermark("fred", f"series_{series_id}")

    async def set_series_watermark(self, series_id: str, timestamp: datetime) -> bool:
        """Set watermark for FRED series."""
        return await self.store.set_watermark(
            "fred",
            f"series_{series_id}",
            timestamp,
            metadata={"series_id": series_id, "data_source": "fred"}
        )


class NOAAWatermarkStore:
    """Specialized watermark store for NOAA data sources."""

    def __init__(self):
        self.store = WatermarkStore()

    async def get_station_watermark(self, station_id: str) -> Optional[datetime]:
        """Get watermark for NOAA station."""
        return await self.store.get_watermark("noaa", f"station_{station_id}")

    async def set_station_watermark(self, station_id: str, timestamp: datetime) -> bool:
        """Set watermark for NOAA station."""
        return await self.store.set_watermark(
            "noaa",
            f"station_{station_id}",
            timestamp,
            metadata={"station_id": station_id, "data_source": "noaa"}
        )
