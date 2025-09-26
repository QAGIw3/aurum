from __future__ import annotations

"""Selector for pluggable data backends based on AurumSettings.

This provides a small bridge from core settings to the aurum.data abstraction
so routers/services can use a unified interface independent of the runtime
database choice (Trino, ClickHouse, Timescale).
"""

from typing import Optional

from aurum.core import AurumSettings
from aurum.data import ConnectionConfig, get_backend
from aurum.performance.connection_pool import PoolConfig


def get_data_backend(settings: Optional[AurumSettings] = None):
    settings = settings or AurumSettings.from_env()
    cfg = settings.data_backend
    pool_cfg = PoolConfig(
        min_size=cfg.connection_pool_min_size,
        max_size=cfg.connection_pool_max_size,
        max_idle_time_seconds=cfg.connection_pool_max_idle_time,
        acquire_timeout_seconds=cfg.connection_pool_acquire_timeout_seconds,
    )

    if cfg.backend_type.value == "trino":
        database = f"{cfg.trino_catalog}.{cfg.trino_database_schema}"
        conn = ConnectionConfig(
            host=cfg.trino_host,
            port=cfg.trino_port,
            database=database,
            username=cfg.trino_user,
            password=cfg.trino_password or "",
            ssl=True,
        )
        return get_backend("trino", conn, pool_cfg)

    if cfg.backend_type.value == "clickhouse":
        conn = ConnectionConfig(
            host=cfg.clickhouse_host,
            port=cfg.clickhouse_port,
            database=cfg.clickhouse_database,
            username=cfg.clickhouse_user,
            password=cfg.clickhouse_password or "",
            ssl=False,
        )
        return get_backend("clickhouse", conn, pool_cfg)

    if cfg.backend_type.value == "timescale":
        conn = ConnectionConfig(
            host=cfg.timescale_host,
            port=cfg.timescale_port,
            database=cfg.timescale_database,
            username=cfg.timescale_user,
            password=cfg.timescale_password or "",
            ssl=False,
        )
        return get_backend("timescale", conn, pool_cfg)

    raise ValueError(f"Unsupported backend: {cfg.backend_type}")


__all__ = ["get_data_backend"]

