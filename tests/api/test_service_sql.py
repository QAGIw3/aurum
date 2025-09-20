from __future__ import annotations

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
