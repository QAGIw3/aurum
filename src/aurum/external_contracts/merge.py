"""Utilities to materialize external contract topics into Iceberg via Trino."""
from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:  # pragma: no cover - dependency optional in some environments
    from trino import dbapi
except ImportError:  # pragma: no cover - allow importing module without trino installed
    dbapi = None  # type: ignore[assignment]


MERGE_ROOT = Path(__file__).resolve().parents[2] / "sql" / "merge"
CATALOG_MERGE_SQL = MERGE_ROOT / "catalog_merge.sql"
OBS_MERGE_SQL = MERGE_ROOT / "obs_merge.sql"


@dataclass
class MergeSummary:
    provider: str
    staging_table: str
    records_available: int
    merge_sql: Path


class TrinoExternalContractsConsumer:
    """Orchestrate Iceberg merges for canonical external contracts."""

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        catalog: str = "iceberg",
        schema: str = "external",
        staging_schema: str = "external_stage",
    ) -> None:
        self.host = host or os.getenv("AURUM_TRINO_HOST", "localhost")
        self.port = port or int(os.getenv("AURUM_TRINO_PORT", "8080"))
        self.user = user or os.getenv("AURUM_TRINO_USER", "aurum-airflow")
        self.password = password or os.getenv("AURUM_TRINO_PASSWORD")
        self.catalog = catalog
        self.schema = schema
        self.staging_schema = staging_schema

    def merge_catalog(self, provider: str, staging_table: Optional[str] = None) -> MergeSummary:
        table = staging_table or f"series_catalog_{provider.lower()}"
        return self._merge(
            provider=provider,
            staging_table=table,
            view_name="staging_external_series_catalog",
            merge_sql=CATALOG_MERGE_SQL,
        )

    def merge_observations(self, provider: str, staging_table: Optional[str] = None) -> MergeSummary:
        table = staging_table or f"timeseries_observation_{provider.lower()}"
        return self._merge(
            provider=provider,
            staging_table=table,
            view_name="staging_external_timeseries_observation",
            merge_sql=OBS_MERGE_SQL,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _merge(self, *, provider: str, staging_table: str, view_name: str, merge_sql: Path) -> MergeSummary:
        if dbapi is None:  # pragma: no cover - guarded import
            raise RuntimeError("trino package not installed; cannot execute merge")

        resolved_table = self._resolve_table(staging_table)
        logger.info(
            "Running external contract merge",
            extra={
                "provider": provider,
                "staging_table": resolved_table,
                "view": view_name,
                "merge_sql": str(merge_sql),
            },
        )

        with contextlib.closing(
            dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                password=self.password,
            )
        ) as conn:
            cursor = conn.cursor()
            records_available = self._count_rows(cursor, resolved_table)
            cursor.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {resolved_table}"
            )
            try:
                cursor.execute(merge_sql.read_text(encoding="utf-8"))
            finally:
                cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
        return MergeSummary(
            provider=provider,
            staging_table=resolved_table,
            records_available=records_available,
            merge_sql=merge_sql,
        )

    def _resolve_table(self, table: str) -> str:
        fragments = table.split(".")
        if len(fragments) == 1:
            return f"{self.catalog}.{self.staging_schema}.{table}"
        if len(fragments) == 2:
            return f"{self.catalog}.{table}"
        return table

    @staticmethod
    def _count_rows(cursor, table: str) -> int:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        result = cursor.fetchone()
        if not result:
            return 0
        value = result[0]
        return int(value) if value is not None else 0


__all__ = ["TrinoExternalContractsConsumer", "MergeSummary"]
