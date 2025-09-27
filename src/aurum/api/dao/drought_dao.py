"""Drought Data Access Object - handles drought indices and USDM data operations."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from ..config import TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class DroughtDao:
    """Data Access Object for drought data operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
    
    def _execute_trino_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a Trino query and return results."""
        from ..service import _execute_trino_query
        return _execute_trino_query(self._trino_config, sql)
    
    def _safe_literal(self, value: str) -> str:
        """Safely escape a literal value for SQL."""
        from ..service import _safe_literal
        return _safe_literal(value)
    
    def query_drought_indices(
        self,
        *,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query drought indices data."""
        catalog = self._trino_config.catalog
        base_conditions: List[str] = []
        
        if region_type:
            base_conditions.append(f"region_type = '{self._safe_literal(region_type)}'")
        if region_id:
            base_conditions.append(f"region_id = '{self._safe_literal(region_id)}'")
        if dataset:
            base_conditions.append(f"dataset = '{self._safe_literal(dataset)}'")
        if index_id:
            base_conditions.append(f"\"index\" = '{self._safe_literal(index_id)}'")
        if timescale:
            base_conditions.append(f"timescale = '{self._safe_literal(timescale)}'")
        if start_date:
            base_conditions.append(f"valid_date >= DATE '{self._safe_literal(start_date.isoformat())}'")
        if end_date:
            base_conditions.append(f"valid_date <= DATE '{self._safe_literal(end_date.isoformat())}'")
        
        where_clause = ""
        if base_conditions:
            where_clause = " AND " + " AND ".join(base_conditions)
        
        sql = f"""
            WITH ranked AS (
                SELECT
                    di.*, ROW_NUMBER() OVER (
                        PARTITION BY di.series_id, di.valid_date
                        ORDER BY di.ingest_ts DESC
                    ) AS rn
                FROM {catalog}.environment.drought_index AS di
                WHERE 1 = 1{where_clause}
            )
            SELECT
                series_id,
                dataset,
                "index" AS index,
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
        
        start_time = time.perf_counter()
        rows: List[Dict[str, Any]] = self._execute_trino_query(sql)
        
        # Process metadata JSON strings
        for row in rows:
            metadata = row.get("metadata")
            if isinstance(metadata, str):
                try:
                    row["metadata"] = json.loads(metadata)
                except json.JSONDecodeError:
                    row["metadata"] = None
        
        elapsed = (time.perf_counter() - start_time) * 1000.0
        return rows, elapsed
    
    def query_drought_usdm(
        self,
        *,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query USDM (US Drought Monitor) data."""
        catalog = self._trino_config.catalog
        conditions: List[str] = []
        
        if region_type:
            conditions.append(f"region_type = '{self._safe_literal(region_type)}'")
        if region_id:
            conditions.append(f"region_id = '{self._safe_literal(region_id)}'")
        if start_date:
            conditions.append(f"valid_date >= DATE '{self._safe_literal(start_date.isoformat())}'")
        if end_date:
            conditions.append(f"valid_date <= DATE '{self._safe_literal(end_date.isoformat())}'")
        
        where_sql = ""
        if conditions:
            where_sql = " AND " + " AND ".join(conditions)
        
        sql = f"""
            WITH ranked AS (
                SELECT
                    ua.*, ROW_NUMBER() OVER (
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
        
        start_time = time.perf_counter()
        rows: List[Dict[str, Any]] = self._execute_trino_query(sql)
        
        # Process metadata JSON strings
        for row in rows:
            metadata = row.get("metadata")
            if isinstance(metadata, str):
                try:
                    row["metadata"] = json.loads(metadata)
                except json.JSONDecodeError:
                    row["metadata"] = None
        
        elapsed = (time.perf_counter() - start_time) * 1000.0
        return rows, elapsed
    
    def query_drought_vector_events(
        self,
        *,
        layer: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query drought vector events data."""
        catalog = self._trino_config.catalog
        conditions: List[str] = []
        
        if layer:
            conditions.append(f"layer = '{self._safe_literal(layer)}'")
        if region_type:
            conditions.append(f"region_type = '{self._safe_literal(region_type)}'")
        if region_id:
            conditions.append(f"region_id = '{self._safe_literal(region_id)}'")
        if start_time:
            conditions.append(f"event_time >= TIMESTAMP '{self._safe_literal(start_time.isoformat())}'")
        if end_time:
            conditions.append(f"event_time <= TIMESTAMP '{self._safe_literal(end_time.isoformat())}'")
        
        where_sql = ""
        if conditions:
            where_sql = " WHERE " + " AND ".join(conditions)
        
        sql = f"""
            SELECT
                layer,
                event_time,
                region_type,
                region_id,
                g.region_name,
                g.parent_region_id,
                geometry_wkt,
                CAST(properties AS JSON) AS properties,
                CAST(metadata AS JSON) AS metadata
            FROM {catalog}.environment.drought_vector_events dve
            LEFT JOIN {catalog}.ref.geographies g
                ON g.region_type = dve.region_type
               AND g.region_id = dve.region_id
            {where_sql}
            ORDER BY event_time DESC
            LIMIT {int(limit)}
        """
        
        start_time = time.perf_counter()
        rows: List[Dict[str, Any]] = self._execute_trino_query(sql)
        
        # Process JSON strings
        for row in rows:
            for json_field in ["properties", "metadata"]:
                field_value = row.get(json_field)
                if isinstance(field_value, str):
                    try:
                        row[json_field] = json.loads(field_value)
                    except json.JSONDecodeError:
                        row[json_field] = None
        
        elapsed = (time.perf_counter() - start_time) * 1000.0
        return rows, elapsed