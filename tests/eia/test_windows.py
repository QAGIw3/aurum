from __future__ import annotations

from aurum.eia.windows import WindowConfigError, compute_window_bounds


def test_compute_window_bounds_hourly() -> None:
    bounds = compute_window_bounds(
        window_end="2024-01-02T00:00:00Z",
        frequency="HOURLY",
        date_format='YYYY-MM-DD"T"HH24',
        hours="1",
    )
    assert bounds.start_token == "2024-01-01T23"
    assert bounds.end_token == "2024-01-01T23"


def test_compute_window_bounds_monthly_defaults() -> None:
    bounds = compute_window_bounds(
        window_end="2024-02-01T00:00:00Z",
        frequency="MONTHLY",
        date_format="YYYY-MM",
    )
    assert bounds.start_token == "2024-01"
    assert bounds.end_token == "2024-01"


def test_compute_window_bounds_invalid_end() -> None:
    try:
        compute_window_bounds(window_end=None, frequency="DAILY", date_format="YYYY-MM-DD")
    except WindowConfigError as exc:
        assert "EIA_WINDOW_END" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected WindowConfigError")
