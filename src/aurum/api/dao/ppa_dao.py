"""Data access helpers for PPA valuations."""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

from aurum.core import AurumSettings

from ..config import TrinoConfig
from ..database.trino_client import get_trino_client
from ..state import configure as configure_state, get_settings


class PpaDao:
    """Execute PPA valuation queries using the configured analytics backend."""

    def __init__(self, settings: Optional[AurumSettings] = None) -> None:
        try:
            self._settings = settings or get_settings()
        except RuntimeError:
            self._settings = AurumSettings.from_env()
            configure_state(self._settings)

        self._default_trino_cfg = TrinoConfig.from_settings(self._settings)

    def _resolve_config(self, trino_cfg: Optional[TrinoConfig]) -> TrinoConfig:
        return trino_cfg or self._default_trino_cfg

    def execute(
        self,
        sql: str,
        *,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[List[Dict[str, object]], float]:
        cfg = self._resolve_config(trino_cfg)
        client = get_trino_client(cfg)

        start = time.perf_counter()
        rows = client.execute_query_sync(sql, use_cache=True)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return rows, elapsed_ms


__all__ = ["PpaDao"]

