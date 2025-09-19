from __future__ import annotations

from datetime import date

import pytest

from aurum.reference.calendars import expand_block_range, get_calendars, hours_for_block


def test_load_calendars_returns_known_calendar() -> None:
    calendars = get_calendars()
    assert "pjm" in calendars
    assert calendars["pjm"].timezone == "America/New_York"


def test_hours_for_block_on_peak_weekday() -> None:
    target = date(2024, 1, 2)  # Tuesday
    hours = hours_for_block("pjm", "ON_PEAK", target)
    assert len(hours) == 16
    assert hours[0].hour == 7
    assert hours[-1].hour == 22
    assert hours[0].tzinfo is not None


def test_hours_for_block_off_peak_weekend() -> None:
    target = date(2024, 1, 6)  # Saturday
    hours = hours_for_block("pjm", "OFF_PEAK", target)
    assert len(hours) == 24
    assert hours[0].hour == 0
    assert hours[-1].hour == 23


def test_unknown_calendar_raises() -> None:
    with pytest.raises(KeyError):
        hours_for_block("unknown", "ON_PEAK", date(2024, 1, 1))


def test_unknown_block_raises() -> None:
    with pytest.raises(KeyError):
        hours_for_block("pjm", "INVALID", date(2024, 1, 1))


def test_expand_block_range_skips_holidays() -> None:
    hours = expand_block_range(
        "pjm",
        "ON_PEAK",
        date(2024, 7, 3),
        date(2024, 7, 5),
        include_holidays=False,
    )
    holiday_hours = [dt for dt in hours if dt.date() == date(2024, 7, 4)]
    assert not holiday_hours  # July 4th excluded


def test_expand_block_range_including_holidays() -> None:
    hours = expand_block_range(
        "pjm",
        "ON_PEAK",
        date(2024, 7, 3),
        date(2024, 7, 4),
        include_holidays=True,
    )
    holiday_hours = [dt for dt in hours if dt.date() == date(2024, 7, 4)]
    assert holiday_hours
