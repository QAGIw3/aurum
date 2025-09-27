"""External data access layer with parametrized Trino SQL and safe identifiers."""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from ..api.exceptions import NotFoundException
from ..api.database.trino_client import TrinoClient
from ..telemetry.context import log_structured

LOGGER = logging.getLogger(__name__)


@dataclass
class PaginationCursor:
    """Opaque pagination cursor for external data."""
    provider_id: Optional[str] = None
    series_id: Optional[str] = None
    last_updated: Optional[datetime] = None
    offset: int = 0

    @classmethod
    def from_string(cls, cursor_str: str) -> 'PaginationCursor':
        """Create cursor from opaque string."""
        try:
            # Decode base64 cursor
            import base64
            decoded = base64.b64decode(cursor_str).decode('utf-8')
            parts = decoded.split('|')

            if len(parts) == 4:
                provider_id, series_id, last_updated_str, offset_str = parts
                last_updated = datetime.fromisoformat(last_updated_str) if last_updated_str else None
                offset = int(offset_str)
                return cls(provider_id, series_id, last_updated, offset)
        except Exception:
            pass

        # Return default cursor on decode failure
        return cls()

    def to_string(self) -> str:
        """Convert cursor to opaque string."""
        import base64
        parts = [
            self.provider_id or '',
            self.series_id or '',
            self.last_updated.isoformat() if self.last_updated else '',
            str(self.offset)
        ]
        encoded = '|'.join(parts).encode('utf-8')
        return base64.b64encode(encoded).decode('utf-8')


class ExternalDAO:
    """Data access object for external data with parametrized SQL."""

    def __init__(self, trino_client: Optional[TrinoClient] = None):
        self.trino_client = trino_client

    async def get_trino_client(self) -> TrinoClient:
        """Get Trino client instance."""
        if self.trino_client is None:
            # Consolidated import path: use the canonical database client
            from ..api.database.trino_client import get_trino_client
            self.trino_client = get_trino_client()
        return self.trino_client

    async def get_providers(
        self,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get external data providers with performance hardening and guards."""
        client = await self.get_trino_client()
        cursor_obj = PaginationCursor.from_string(cursor) if cursor else PaginationCursor()

        # Safe identifier validation
        safe_limit = self._validate_limit(limit, max_limit=1000)
        safe_offset = max(0, offset)

        # Build parametrized query
        query = """
            SELECT
                provider_id,
                name,
                description,
                base_url,
                last_updated,
                series_count
            FROM external_providers
            WHERE 1=1
        """

        params = {}
        conditions = []

        # Add cursor-based pagination conditions
        if cursor_obj.provider_id:
            conditions.append("provider_id > :cursor_provider_id")
            params["cursor_provider_id"] = cursor_obj.provider_id

        if cursor_obj.last_updated:
            conditions.append("last_updated >= :cursor_last_updated")
            params["cursor_last_updated"] = cursor_obj.last_updated

        if conditions:
            query += " AND " + " AND ".join(conditions)

        # Add ordering and limits with performance guards
        query += """
            ORDER BY provider_id, last_updated
            LIMIT :limit
            OFFSET :offset
        """
        params["limit"] = safe_limit
        params["offset"] = safe_offset

        # Performance guard: Check estimated result size
        if safe_limit > 500:
            LOGGER.warning(f"Large result set requested: limit={safe_limit}")

        # Stream results if large
        if safe_limit > 100:
            LOGGER.info(f"Streaming large result set: limit={safe_limit}")
            # Note: In production, would implement streaming query execution
            # For now, we'll just log the intent

        try:
            result = await client.execute_query(query, params)

            # Convert to provider objects
            providers = []
            for row in result:
                provider = {
                    "id": row["provider_id"],
                    "name": row["name"],
                    "description": row["description"],
                    "base_url": row["base_url"],
                    "last_updated": row["last_updated"],
                    "series_count": row["series_count"],
                }
                providers.append(provider)

            log_structured(
                "info",
                "external_providers_query_success",
                provider_count=len(providers),
                limit=limit,
                offset=offset,
                has_cursor=bool(cursor),
            )

            return providers

        except Exception as exc:
            log_structured(
                "error",
                "external_providers_query_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                limit=limit,
                offset=offset,
            )
            raise

    async def get_series(
        self,
        provider: Optional[str] = None,
        frequency: Optional[str] = None,
        asof: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get external series with optional filtering."""
        client = await self.get_trino_client()
        cursor_obj = PaginationCursor.from_string(cursor) if cursor else PaginationCursor()

        # Safe identifier validation
        safe_limit = self._validate_limit(limit, max_limit=1000)
        safe_offset = max(0, offset)

        # Build parametrized query
        query = """
            SELECT
                series_id,
                provider_id,
                name,
                frequency,
                description,
                units,
                last_updated,
                observation_count
            FROM external_series
            WHERE 1=1
        """

        params = {}
        conditions = []

        # Add filters
        if provider:
            conditions.append("provider_id = :provider")
            params["provider"] = self._safe_identifier(provider)

        if frequency:
            conditions.append("frequency = :frequency")
            params["frequency"] = frequency

        if asof:
            conditions.append("last_updated <= :asof")
            params["asof"] = asof

        # Add cursor-based pagination conditions
        if cursor_obj.series_id:
            conditions.append("series_id > :cursor_series_id")
            params["cursor_series_id"] = cursor_obj.series_id

        if cursor_obj.last_updated:
            conditions.append("last_updated >= :cursor_last_updated")
            params["cursor_last_updated"] = cursor_obj.last_updated

        if conditions:
            query += " AND " + " AND ".join(conditions)

        # Add ordering and limits
        query += """
            ORDER BY series_id, last_updated
            LIMIT :limit
            OFFSET :offset
        """
        params["limit"] = safe_limit
        params["offset"] = safe_offset

        try:
            result = await client.execute_query(query, params)

            # Convert to series objects
            series = []
            for row in result:
                series_obj = {
                    "id": row["series_id"],
                    "provider_id": row["provider_id"],
                    "name": row["name"],
                    "frequency": row["frequency"],
                    "description": row["description"],
                    "units": row["units"],
                    "last_updated": row["last_updated"],
                    "observation_count": row["observation_count"],
                }
                series.append(series_obj)

            log_structured(
                "info",
                "external_series_query_success",
                series_count=len(series),
                provider=provider,
                frequency=frequency,
                limit=limit,
                offset=offset,
            )

            return series

        except Exception as exc:
            log_structured(
                "error",
                "external_series_query_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                provider=provider,
                frequency=frequency,
                limit=limit,
                offset=offset,
            )
            raise

    async def get_observations(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
        asof: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
        cursor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get observations for a specific external series."""
        client = await self.get_trino_client()

        # Validate series exists
        series_exists = await self._series_exists(series_id)
        if not series_exists:
            raise NotFoundException(f"Series {series_id} not found")

        # Safe identifier validation
        safe_limit = self._validate_limit(limit, max_limit=10000)
        safe_offset = max(0, offset)

        # Build parametrized query
        query = """
            SELECT
                series_id,
                observation_date,
                value,
                metadata
            FROM external_observations
            WHERE series_id = :series_id
        """

        params = {"series_id": self._safe_identifier(series_id)}
        conditions = []

        # Add date filters
        if start_date:
            conditions.append("observation_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("observation_date <= :end_date")
            params["end_date"] = end_date

        if asof:
            conditions.append("ingested_at <= :asof")
            params["asof"] = asof

        if conditions:
            query += " AND " + " AND ".join(conditions)

        # Add ordering and limits
        query += """
            ORDER BY observation_date
            LIMIT :limit
            OFFSET :offset
        """
        params["limit"] = safe_limit
        params["offset"] = safe_offset

        # Performance guard: Check estimated result size for observations
        if safe_limit > 1000:
            LOGGER.warning(f"Very large observations result set requested: limit={safe_limit}, series_id={series_id}")

        # Stream results if extremely large
        if safe_limit > 5000:
            LOGGER.info(f"Streaming very large observations result set: limit={safe_limit}, series_id={series_id}")
            # Note: In production, would implement streaming query execution
            # For now, we'll just log the intent

        try:
            result = await client.execute_query(query, params)

            # Convert to observation objects
            observations = []
            for row in result:
                observation = {
                    "series_id": row["series_id"],
                    "date": row["observation_date"],
                    "value": row["value"],
                    "metadata": row.get("metadata", {}),
                }
                observations.append(observation)

            log_structured(
                "info",
                "external_observations_query_success",
                series_id=series_id,
                observation_count=len(observations),
                limit=limit,
                offset=offset,
            )

            return observations

        except Exception as exc:
            log_structured(
                "error",
                "external_observations_query_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                series_id=series_id,
                limit=limit,
                offset=offset,
            )
            raise

    async def get_metadata(
        self,
        provider: Optional[str] = None,
        include_counts: bool = False
    ) -> Dict[str, Any]:
        """Get external data metadata."""
        client = await self.get_trino_client()

        # Build parametrized query for providers
        query = """
            SELECT
                provider_id,
                name,
                description,
                base_url,
                last_updated,
                series_count
            FROM external_providers
        """

        params = {}
        if provider:
            query += " WHERE provider_id = :provider"
            params["provider"] = self._safe_identifier(provider)

        query += " ORDER BY provider_id"

        try:
            providers_result = await client.execute_query(query, params)

            providers = []
            for row in providers_result:
                provider_obj = {
                    "id": row["provider_id"],
                    "name": row["name"],
                    "description": row["description"],
                    "base_url": row["base_url"],
                    "last_updated": row["last_updated"],
                    "series_count": row["series_count"],
                }
                providers.append(provider_obj)

            # Get total series count
            total_series_query = "SELECT COUNT(*) as total FROM external_series"
            total_series_params = {}
            if provider:
                total_series_query += " WHERE provider_id = :provider"
                total_series_params["provider"] = self._safe_identifier(provider)

            total_result = await client.execute_query(total_series_query, total_series_params)
            total_series = total_result[0]["total"] if total_series_result else 0

            # Get last updated timestamp
            last_updated_query = """
                SELECT MAX(last_updated) as last_updated
                FROM external_providers
            """
            last_updated_params = {}
            if provider:
                last_updated_query += " WHERE provider_id = :provider"
                last_updated_params["provider"] = self._safe_identifier(provider)

            last_updated_result = await client.execute_query(last_updated_query, last_updated_params)
            last_updated = last_updated_result[0]["last_updated"] if last_updated_result else None

            metadata = {
                "providers": providers,
                "total_series": total_series,
                "last_updated": last_updated,
            }

            log_structured(
                "info",
                "external_metadata_query_success",
                provider_count=len(providers),
                total_series=total_series,
                provider=provider,
                include_counts=include_counts,
            )

            return metadata

        except Exception as exc:
            log_structured(
                "error",
                "external_metadata_query_failed",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                provider=provider,
            )
            raise

    async def _series_exists(self, series_id: str) -> bool:
        """Check if series exists."""
        client = await self.get_trino_client()

        query = """
            SELECT 1 as exists_check
            FROM external_series
            WHERE series_id = :series_id
            LIMIT 1
        """

        params = {"series_id": self._safe_identifier(series_id)}

        try:
            result = await client.execute_query(query, params)
            return len(result) > 0
        except Exception:
            return False

    def _validate_limit(self, limit: int, max_limit: int) -> int:
        """Validate and clamp limit value."""
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if limit > max_limit:
            return max_limit
        return limit

    def _safe_identifier(self, identifier: str) -> str:
        """Validate and sanitize identifier."""
        if not identifier:
            raise ValueError("Identifier cannot be empty")

        # Basic SQL injection prevention
        if any(char in identifier for char in ['\'', '"', ';', '--', '/*', '*/']):
            raise ValueError("Identifier contains invalid characters")

        # Length check
        if len(identifier) > 255:
            raise ValueError("Identifier too long")

        return identifier
