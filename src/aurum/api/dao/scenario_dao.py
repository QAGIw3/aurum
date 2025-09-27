"""Scenario Data Access Object - handles scenario data operations."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from ..config import CacheConfig, TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class ScenarioDao:
    """Data Access Object for scenario operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
        self._cache_config = CacheConfig.from_settings(self._settings)
    
    def _execute_trino_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a Trino query and return results."""
        from ..service import _execute_trino_query
        return _execute_trino_query(self._trino_config, sql)
    
    def _build_sql_scenario_outputs(
        self,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        offset: int = 0,
        limit: int = 1000,
        cursor_after: Optional[Dict[str, Any]] = None,
        cursor_before: Optional[Dict[str, Any]] = None,
        descending: bool = False,
    ) -> str:
        """Build SQL query for scenario outputs."""
        from ..service import _build_sql_scenario_outputs
        return _build_sql_scenario_outputs(
            self._trino_config,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            table_name=table_name,
            columns=columns,
            offset=offset,
            limit=limit,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    
    def query_scenario_outputs(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        offset: int = 0,
        limit: int = 1000,
        cursor_after: Optional[Dict[str, Any]] = None,
        cursor_before: Optional[Dict[str, Any]] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query scenario outputs data."""
        from ..service import query_scenario_outputs
        return query_scenario_outputs(
            self._trino_config,
            self._cache_config,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            table_name=table_name,
            columns=columns,
            offset=offset,
            limit=limit,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    
    def _build_sql_scenario_metrics_latest(
        self,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        metric_name: Optional[str] = None,
        limit: int = 1000,
    ) -> str:
        """Build SQL query for latest scenario metrics."""
        from ..service import _build_sql_scenario_metrics_latest
        return _build_sql_scenario_metrics_latest(
            self._trino_config,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            metric_name=metric_name,
            limit=limit,
        )
    
    def query_scenario_metrics_latest(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        metric_name: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query latest scenario metrics data."""
        from ..service import query_scenario_metrics_latest
        return query_scenario_metrics_latest(
            self._trino_config,
            self._cache_config,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            metric_name=metric_name,
            limit=limit,
        )
    
    def invalidate_scenario_outputs_cache(
        self,
        *,
        tenant_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Invalidate scenario outputs cache."""
        from ..service import invalidate_scenario_outputs_cache
        return invalidate_scenario_outputs_cache(
            self._cache_config,
            tenant_id=tenant_id,
            scenario_id=scenario_id,
        )
    
    def _scenario_cache_index_key(self, namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
        """Generate scenario cache index key."""
        from ..service import _scenario_cache_index_key
        return _scenario_cache_index_key(namespace, tenant_id, scenario_id)
    
    def _scenario_metrics_cache_index_key(self, namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
        """Generate scenario metrics cache index key."""
        from ..service import _scenario_metrics_cache_index_key
        return _scenario_metrics_cache_index_key(namespace, tenant_id, scenario_id)
    
    def _scenario_cache_version_key(self, namespace: str, tenant_id: Optional[str]) -> str:
        """Generate scenario cache version key."""
        from ..service import _scenario_cache_version_key
        return _scenario_cache_version_key(namespace, tenant_id)
    
    def _get_scenario_cache_version(self, client, namespace: str, tenant_id: Optional[str]) -> str:
        """Get scenario cache version."""
        from ..service import _get_scenario_cache_version
        return _get_scenario_cache_version(client, namespace, tenant_id)
    
    def _bump_scenario_cache_version(self, client, namespace: str, tenant_id: Optional[str]) -> None:
        """Bump scenario cache version."""
        from ..service import _bump_scenario_cache_version
        return _bump_scenario_cache_version(client, namespace, tenant_id)
    
    def _publish_cache_invalidation_event(self, tenant_id: Optional[str], scenario_id: str) -> None:
        """Publish cache invalidation event."""
        from ..service import _publish_cache_invalidation_event
        return _publish_cache_invalidation_event(tenant_id, scenario_id)