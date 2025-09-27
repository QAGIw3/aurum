"""Drought Data Access Object with Trino-backed queries."""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings

from ..config import TrinoConfig
from ..database.trino_client import get_trino_client
from ..state import configure as configure_state, get_settings


def _sanitize(value: str) -> str:
    """Return a safe literal for embedding into SQL strings."""

    return value.replace("'", "''")


class DroughtDao:
    """DAO encapsulating drought queries against the analytics warehouse."""

    def __init__(self, settings: Optional[AurumSettings] = None) -> None:
        try:
            self._settings = settings or get_settings()
        except RuntimeError:
            self._settings = AurumSettings.from_env()
            configure_state(self._settings)
        self._default_trino_cfg = TrinoConfig.from_settings(self._settings)

    def _resolve_config(self, trino_cfg: Optional[TrinoConfig]) -> TrinoConfig:
        return trino_cfg or self._default_trino_cfg

    async def query_indices(
        self,
        *,
        trino_cfg: Optional[TrinoConfig] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        cfg = self._resolve_config(trino_cfg)
        catalog = cfg.catalog

        conditions: List[str] = []
        if region_type:
            conditions.append(f"region_type = '{_sanitize(region_type)}'")
        if region_id:
            conditions.append(f"region_id = '{_sanitize(region_id)}'")
        if dataset:
            conditions.append(f"dataset = '{_sanitize(dataset)}'")
        if index_id:
            conditions.append(f"\"index\" = '{_sanitize(index_id)}'")
        if timescale:
            conditions.append(f"timescale = '{_sanitize(timescale)}'")
        if start_date:
            conditions.append(f"valid_date >= DATE '{_sanitize(start_date.isoformat())}'")
        if end_date:
            conditions.append(f"valid_date <= DATE '{_sanitize(end_date.isoformat())}'")
        where_clause = ""
        if conditions:
            where_clause = " AND " + " AND ".join(conditions)

        sql = f"""
            WITH ranked AS (
                SELECT
                    di.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY di.series_id, di.valid_date
                        ORDER BY di.ingest_ts DESC
                    ) AS rn
                FROM {catalog}.environment.drought_index AS di
                WHERE 1 = 1{where_clause}
            )
            SELECT
                series_id,
                dataset,
                \"index\" AS index,
                timescale,
                CAST(valid_date AS DATE) AS valid_date,
                as_of,
                value,
                unit,
                poc,
                region_type,
                region_id,
                g.region_name,
                g.parent_region_id,
                source_url,
                CAST(metadata AS JSON) AS metadata
            FROM ranked r
            LEFT JOIN {catalog}.ref.geographies g
                ON g.region_type = r.region_type
               AND g.region_id = r.region_id
            WHERE rn = 1
            ORDER BY valid_date DESC, series_id
            LIMIT {int(limit)}
        """

        client = get_trino_client(cfg)
        start = time.perf_counter()
        rows = await client.execute_query(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        for row in rows:
            metadata = row.get("metadata")
            if isinstance(metadata, str):
                try:
                    row["metadata"] = json.loads(metadata)
                except json.JSONDecodeError:
                    row["metadata"] = None

        return rows, elapsed_ms

    async def query_usdm(
        self,
        *,
        trino_cfg: Optional[TrinoConfig] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        cfg = self._resolve_config(trino_cfg)
        catalog = cfg.catalog

        conditions: List[str] = []
        if region_type:
            conditions.append(f"region_type = '{_sanitize(region_type)}'")
        if region_id:
            conditions.append(f"region_id = '{_sanitize(region_id)}'")
        if start_date:
            conditions.append(f"valid_date >= DATE '{_sanitize(start_date.isoformat())}'")
        if end_date:
            conditions.append(f"valid_date <= DATE '{_sanitize(end_date.isoformat())}'")
        where_sql = ""
        if conditions:
            where_sql = " AND " + " AND ".join(conditions)

        sql = f"""
            WITH ranked AS (
                SELECT
                    ua.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY ua.region_type, ua.region_id, ua.valid_date
                        ORDER BY ua.ingest_ts DESC
                    ) AS rn
                FROM {catalog}.environment.usdm_area ua
                WHERE 1 = 1{where_sql}
            )
            SELECT
                region_type,
                region_id,
                CAST(valid_date AS DATE) AS valid_date,
                as_of,
                d0_frac,
                d1_frac,
                d2_frac,
                d3_frac,
                d4_frac,
                source_url,
                CAST(metadata AS JSON) AS metadata,
                g.region_name,
                g.parent_region_id
            FROM ranked r
            LEFT JOIN {catalog}.ref.geographies g
                ON g.region_type = r.region_type
               AND g.region_id = r.region_id
            WHERE rn = 1
            ORDER BY valid_date DESC
            LIMIT {int(limit)}
        """

        client = get_trino_client(cfg)
        start = time.perf_counter()
        rows = await client.execute_query(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        for row in rows:
            metadata = row.get("metadata")
            if isinstance(metadata, str):
                try:
                    row["metadata"] = json.loads(metadata)
                except json.JSONDecodeError:
                    row["metadata"] = None

        return rows, elapsed_ms

    async def query_vector_events(
        self,
        *,
        trino_cfg: Optional[TrinoConfig] = None,
        layer: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        cfg = self._resolve_config(trino_cfg)
        catalog = cfg.catalog

        conditions: List[str] = []
        if layer:
            conditions.append(f"layer = '{_sanitize(layer)}'")
        if region_type:
            conditions.append(f"region_type = '{_sanitize(region_type)}'")
        if region_id:
            conditions.append(f"region_id = '{_sanitize(region_id)}'")
        if start_time:
            formatted = start_time.isoformat(sep=" ", timespec="seconds")
            conditions.append(f"valid_start >= TIMESTAMP '{_sanitize(formatted)}'")
        if end_time:
            formatted = end_time.isoformat(sep=" ", timespec="seconds")
            conditions.append(f"coalesce(valid_end, valid_start) <= TIMESTAMP '{_sanitize(formatted)}'")

        where_sql = ""
        if conditions:
            where_sql = " WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                ve.layer,
                ve.event_id,
                ve.region_type,
                ve.region_id,
                g.region_name,
                g.parent_region_id,
                ve.valid_start,
                ve.valid_end,
                ve.value,
                ve.unit,
                ve.category,
                ve.severity,
                ve.source_url,
                ve.geometry_wkt,
                CAST(ve.properties AS JSON) AS properties
            FROM {catalog}.environment.vector_events ve
            LEFT JOIN {catalog}.ref.geographies g
                ON g.region_type = ve.region_type
               AND g.region_id = ve.region_id
            {where_sql}
            ORDER BY coalesce(ve.valid_start, ve.ingest_ts) DESC
            LIMIT {int(limit)}
        """

        client = get_trino_client(cfg)
        start = time.perf_counter()
        rows = await client.execute_query(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        for row in rows:
            props = row.get("properties")
            if isinstance(props, str):
                try:
                    row["properties"] = json.loads(props)
                except json.JSONDecodeError:
                    row["properties"] = None

        return rows, elapsed_ms


__all__ = ["DroughtDao"]

