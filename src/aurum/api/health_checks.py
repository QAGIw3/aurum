"""Shared health-check helpers used by API endpoints and tooling."""

from __future__ import annotations

import time
from typing import Any, Dict

from .config import CacheConfig, TrinoConfig
from . import service


async def check_trino_ready(cfg: TrinoConfig) -> bool:
    """Return ``True`` when a lightweight Trino probe succeeds."""
    try:
        from .database.trino_client import get_trino_client

        client = get_trino_client(config=cfg)
        await client.execute_query("SELECT 1")
        return True
    except Exception:
        return False


def check_timescale_ready(dsn: str) -> bool:
    """Return ``True`` when a Timescale connection can be established."""
    if not dsn:
        return False

    try:  # pragma: no cover - import guard
        import psycopg  # type: ignore
    except ModuleNotFoundError:
        return False

    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


def check_redis_ready(cache_cfg: CacheConfig) -> bool:
    """Return ``True`` if Redis is reachable or explicitly disabled."""
    try:
        client = service._maybe_redis_client(cache_cfg)
    except Exception:
        return False

    if client is None:
        redis_configured = bool(
            cache_cfg.redis_url
            or cache_cfg.sentinel_endpoints
            or cache_cfg.cluster_nodes
        )
        if str(cache_cfg.mode).lower() == "disabled":
            redis_configured = False
        return not redis_configured

    return True


async def check_trino_deep_health(cfg: TrinoConfig) -> Dict[str, Any]:
    """Run a deeper Trino diagnostic including schema and perf sampling."""
    try:
        from .database.trino_client import get_trino_client

        client = get_trino_client(config=cfg)
        start_time = time.perf_counter()

        await client.execute_query("SELECT 1")

        schema_check_start = time.perf_counter()
        schema_query = (
            "SELECT table_name FROM iceberg.information_schema.tables "
            "WHERE table_schema = 'market' AND table_name IN "
            "('curve_observation','lmp_observation','series_observation')"
        )
        schema_rows = await client.execute_query(schema_query)
        schema_tables_found = len(schema_rows)
        schema_check_time = time.perf_counter() - schema_check_start

        perf_check_start = time.perf_counter()
        perf_query = (
            "SELECT COUNT(*) AS total_count FROM iceberg.market.curve_observation TABLESAMPLE SYSTEM (1)"
        )
        sample_rows = await client.execute_query(perf_query)
        perf_check_time = time.perf_counter() - perf_check_start
        sample_result = sample_rows[0].get("total_count") if sample_rows else 0

        total_time = time.perf_counter() - start_time

        return {
            "status": "healthy",
            "connectivity": "ok",
            "schema_tables_found": schema_tables_found,
            "schema_check_time_ms": round(schema_check_time * 1000, 2),
            "performance_check_time_ms": round(perf_check_time * 1000, 2),
            "total_check_time_ms": round(total_time * 1000, 2),
            "sample_query_result": int(sample_result or 0),
        }
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}


async def check_schema_registry_ready(schema_registry_url: str | None) -> Dict[str, Any]:
    """Check Schema Registry connectivity and health using httpx."""
    if not schema_registry_url:
        return {"status": "disabled", "error": "Schema Registry URL not configured"}

    try:
        import httpx
    except Exception as exc:  # pragma: no cover - optional dependency
        return {"status": "unavailable", "error": str(exc)}

    url = f"{schema_registry_url.rstrip('/')}/subjects"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                subjects = response.json()
                return {
                    "status": "healthy",
                    "subjects_count": len(subjects) if isinstance(subjects, list) else 0,
                }
            return {"status": "unavailable", "error": f"HTTP {response.status_code}"}
    except httpx.RequestError as exc:
        return {"status": "unavailable", "error": f"Connection failed: {exc}"}


__all__ = [
    "check_trino_ready",
    "check_timescale_ready",
    "check_redis_ready",
    "check_trino_deep_health",
    "check_schema_registry_ready",
]
