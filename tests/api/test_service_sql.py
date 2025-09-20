from __future__ import annotations

import json
from datetime import date

from aurum.api import service


def test_build_sql_diff_without_filters_uses_where():
    sql = service._build_sql_diff(  # type: ignore[attr-defined]
        asof_a=date(2024, 1, 1),
        asof_b=date(2024, 1, 2),
        curve_key=None,
        asset_class=None,
        iso=None,
        location=None,
        market=None,
        product=None,
        block=None,
        tenor_type=None,
        limit=10,
        offset=0,
        cursor_after=None,
    )
    # Should not contain an AND without a WHERE
    assert " FROM iceberg.market.curve_observation AND " not in sql
    # Should include a WHERE clause with asof_date filter
    assert " FROM iceberg.market.curve_observation WHERE asof_date IN (DATE '2024-01-01', DATE '2024-01-02')" in sql


def test_build_sql_diff_with_filter_adds_and():
    sql = service._build_sql_diff(  # type: ignore[attr-defined]
        asof_a=date(2024, 1, 1),
        asof_b=date(2024, 1, 2),
        curve_key="FOO",
        asset_class=None,
        iso=None,
        location=None,
        market=None,
        product=None,
        block=None,
        tenor_type=None,
        limit=10,
        offset=0,
        cursor_after=None,
    )
    assert " WHERE curve_key = 'FOO' AND asof_date IN " in sql


def test_scenario_output_cache_invalidation(monkeypatch):
    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows
            self.description = [
                ("tenant_id",),
                ("scenario_id",),
                ("asof_date",),
                ("curve_key",),
                ("tenor_type",),
                ("contract_month",),
                ("tenor_label",),
                ("metric",),
                ("value",),
                ("band_lower",),
                ("band_upper",),
                ("attribution",),
                ("version_hash",),
            ]

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
            return False

        def cursor(self):
            return FakeCursor(self._rows)

    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}
            self.sets: dict[str, set[str]] = {}

        def get(self, key: str):
            return self.store.get(key)

        def setex(self, key: str, _ttl: int, value: str) -> None:
            self.store[key] = value

        def sadd(self, key: str, member: str) -> None:
            self.sets.setdefault(key, set()).add(member)

        def expire(self, key: str, _ttl: int) -> None:
            return None

        def smembers(self, key: str):
            members = self.sets.get(key, set())
            return {member.encode("utf-8") for member in members}

        def delete(self, *keys: str):
            for key in keys:
                self.store.pop(key, None)
                self.sets.pop(key, None)

    fake_rows = [
        (
            "tenant-1",
            "scn-1",
            date(2025, 1, 1),
            "curve-a",
            "MONTHLY",
            date(2025, 1, 1),
            "2025-01",
            "mid",
            42.0,
            None,
            None,
            json.dumps({"component": "policy", "delta": 0.1}),
            "hash",
        )
    ]

    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda _cfg: fake_redis)
    monkeypatch.setattr(service, "_require_trino", lambda: lambda **_kw: FakeConnection(fake_rows))

    cfg = service.CacheConfig(redis_url="redis://localhost", ttl_seconds=60)
    rows, _elapsed = service.query_scenario_outputs(
        service.TrinoConfig(),
        cfg,
        tenant_id="tenant-1",
        scenario_id="scn-1",
        curve_key=None,
        tenor_type=None,
        metric=None,
        limit=10,
    )
    assert rows
    index_key = "scenario-outputs:index:tenant-1:scn-1"
    assert index_key in fake_redis.sets
    cache_keys = list(fake_redis.store.keys())
    assert cache_keys

    service.invalidate_scenario_outputs_cache(cfg, tenant_id="tenant-1", scenario_id="scn-1")
    assert not fake_redis.store
    assert index_key not in fake_redis.sets
