from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .storage.trino import TrinoAnalyticRepo
from .services.base_service import BaseTrinoService


class MetadataService(BaseTrinoService):
    """Domain service for read-oriented metadata surfaces (dimensions, locations, units, calendars)."""

    def __init__(self, trino: Optional[TrinoAnalyticRepo] = None):
        super().__init__(trino=trino)
        self._catalog = self._settings.database.trino_catalog
        self._schema = self._settings.database.trino_schema

    async def list_dimensions(self, *, asof: Optional[str], offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
        table = f"{self._catalog}.{self._schema}.metadata_dimensions"
        where = f"WHERE asof_date = DATE '{asof}'" if asof else ""
        query = (
            "SELECT dimension, values, coalesce(cast(asof_date as varchar), 'latest') as asof "
            f"FROM {table} {where} ORDER BY dimension LIMIT {limit} OFFSET {offset}"
        )
        rows = await self._trino.execute_query(query)
        # Normalize to expected shape
        out = [
            {
                "dimension": r.get("dimension"),
                "values": r.get("values") or [],
                "asof": r.get("asof", "latest"),
            }
            for r in rows
        ]
        # Unknown total without additional count query; return None to signal unknown
        return out, None

    async def list_locations(self, *, iso: str, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
        table = f"{self._catalog}.{self._schema}.iso_locations"
        query = (
            "SELECT iso, location_id, name, latitude, longitude "
            f"FROM {table} WHERE iso = '{iso}' ORDER BY location_id LIMIT {limit} OFFSET {offset}"
        )
        rows = await self._trino.execute_query(query)
        return rows, None

    async def list_units(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
        table = f"{self._catalog}.{self._schema}.units_canonical"
        query = f"SELECT * FROM {table} ORDER BY unit LIMIT {limit} OFFSET {offset}"
        rows = await self._trino.execute_query(query)
        return rows, None

    async def list_calendars(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
        table = f"{self._catalog}.{self._schema}.calendars"
        query = f"SELECT * FROM {table} ORDER BY calendar_id LIMIT {limit} OFFSET {offset}"
        rows = await self._trino.execute_query(query)
        return rows, None

