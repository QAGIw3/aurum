from __future__ import annotations

from datetime import timezone

import pytest

from aurum.eia.periods import (
    ParsedPeriod,
    PeriodParseError,
    build_sql_period_expressions,
    parse_period_token,
)


def assert_period(period: ParsedPeriod, year: int, month: int, day: int, *, hours: int = 0, days: int = 0) -> None:
    start = period.start
    end = period.end
    assert start.tzinfo == timezone.utc
    assert end.tzinfo == timezone.utc
    assert start.year == year and start.month == month and start.day == day
    if hours:
        delta_hours = int((end - start).total_seconds() // 3600)
        assert delta_hours == hours
    if days:
        delta_days = (end - start).days
        assert delta_days == days


def test_parse_annual() -> None:
    period = parse_period_token("2024", "annual")
    assert_period(period, 2024, 1, 1, days=366)


def test_parse_quarter() -> None:
    period = parse_period_token("2024-Q3", "quarterly")
    assert_period(period, 2024, 7, 1, days=92)


def test_parse_quarter_fallback_format() -> None:
    period = parse_period_token("2024q2", "quarterly")
    assert_period(period, 2024, 4, 1)


def test_parse_monthly() -> None:
    period = parse_period_token("2024-03", "monthly")
    assert_period(period, 2024, 3, 1, days=31)


def test_parse_monthly_compact() -> None:
    period = parse_period_token("202403", "monthly")
    assert_period(period, 2024, 3, 1, days=31)


def test_parse_weekly() -> None:
    period = parse_period_token("2024-03-01", "weekly")
    assert_period(period, 2024, 3, 1, days=7)


def test_parse_daily() -> None:
    period = parse_period_token("2024-03-01", "daily")
    assert_period(period, 2024, 3, 1, days=1)


def test_parse_hourly_with_offset() -> None:
    period = parse_period_token("2024-03-01T05:00:00-05:00", "hourly")
    assert_period(period, 2024, 3, 1, hours=1)
    assert period.start.hour == 10  # converted to UTC


def test_invalid_frequency() -> None:
    with pytest.raises(PeriodParseError):
        parse_period_token("2024", "decade")


def test_invalid_token() -> None:
    with pytest.raises(PeriodParseError):
        parse_period_token("abc", "monthly")


def test_sql_expression_generation() -> None:
    start, end = build_sql_period_expressions("monthly")
    assert "UNIX_TIMESTAMP" in start
    assert "add_months" in end


def test_sql_expression_rejects_unknown_frequency() -> None:
    with pytest.raises(PeriodParseError):
        build_sql_period_expressions("invalid")
