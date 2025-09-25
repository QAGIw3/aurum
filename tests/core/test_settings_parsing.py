"""Unit tests for settings parsing: DataBackendConfig and cache TTLs."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Dict

import pytest


@contextmanager
def envvars(env: Dict[str, str]):
    """Temporarily set environment variables."""
    old = {k: os.environ.get(k) for k in env}
    try:
        os.environ.update(env)
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _enable_test_defaults():
    os.environ["AURUM_TEST_DEFAULT_SETTINGS"] = "1"


def test_data_backend_config_defaults_and_env_override():
    from aurum.core import AurumSettings

    _enable_test_defaults()

    # Defaults
    s = AurumSettings.from_env()
    assert s.data_backend.backend_type.value == "trino"
    assert s.data_backend.trino_host == "localhost"
    assert s.data_backend.trino_port == 8080
    assert s.data_backend.trino_catalog == "iceberg"
    assert s.data_backend.trino_database_schema == "market"

    # Backend selection via env
    with envvars({"AURUM_API_BACKEND": "clickhouse"}):
        s2 = AurumSettings.from_env()
        assert s2.data_backend.backend_type.value == "clickhouse"

    # Pool configuration overrides
    with envvars(
        {
            "AURUM_DATA_BACKEND_POOL_MIN_SIZE": "7",
            "AURUM_DATA_BACKEND_POOL_MAX_SIZE": "13",
            "AURUM_DATA_BACKEND_POOL_MAX_IDLE_TIME": "111",
            "AURUM_DATA_BACKEND_POOL_TIMEOUT_SECONDS": "12.5",
            "AURUM_DATA_BACKEND_POOL_ACQUIRE_TIMEOUT_SECONDS": "4.5",
        }
    ):
        s3 = AurumSettings.from_env()
        cfg = s3.data_backend
        assert cfg.connection_pool_min_size == 7
        assert cfg.connection_pool_max_size == 13
        assert cfg.connection_pool_max_idle_time == 111
        assert cfg.connection_pool_timeout_seconds == 12.5
        assert cfg.connection_pool_acquire_timeout_seconds == 4.5


def test_cache_ttl_defaults_and_overrides():
    from aurum.core import AurumSettings

    _enable_test_defaults()

    # Defaults
    s = AurumSettings.from_env()
    c = s.api.cache
    assert c.cache_ttl_high_frequency == 60
    assert c.cache_ttl_medium_frequency == 300
    assert c.cache_ttl_low_frequency == 3600
    assert c.cache_ttl_static == 7200
    # Derived defaults
    assert c.cache_ttl_curve_data == c.cache_ttl_medium_frequency
    assert c.cache_ttl_metadata == c.cache_ttl_static
    assert c.cache_ttl_scenario_data == c.cache_ttl_medium_frequency

    # Overrides
    with envvars(
        {
            "AURUM_API_CACHE_TTL_HIGH_FREQUENCY": "90",
            "AURUM_API_CACHE_TTL_MEDIUM_FREQUENCY": "301",
            "AURUM_API_CACHE_TTL_LOW_FREQUENCY": "1800",
            "AURUM_API_CACHE_TTL_STATIC": "9000",
            # Curve + metadata + scenario-specific
            "AURUM_API_CACHE_CURVE_TTL": "123",
            "AURUM_API_CACHE_METADATA_TTL": "321",
            "AURUM_API_CACHE_TTL_SCENARIO_DATA": "456",
        }
    ):
        s2 = AurumSettings.from_env()
        c2 = s2.api.cache
        assert c2.cache_ttl_high_frequency == 90
        assert c2.cache_ttl_medium_frequency == 301
        assert c2.cache_ttl_low_frequency == 1800
        assert c2.cache_ttl_static == 9000
        assert c2.cache_ttl_curve_data == 123
        assert c2.cache_ttl_metadata == 321
        assert c2.cache_ttl_scenario_data == 456

