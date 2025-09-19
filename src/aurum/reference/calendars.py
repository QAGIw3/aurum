"""Calendar and block utilities for ISO trading schedules."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List

from zoneinfo import ZoneInfo

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover
    yaml = None
import json

CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "calendars.yml"


@dataclass(frozen=True)
class BlockRule:
    weekdays: frozenset[int]
    hours: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class CalendarBlock:
    name: str
    rules: tuple[BlockRule, ...]


@dataclass(frozen=True)
class Calendar:
    name: str
    timezone: str
    holidays: tuple[date, ...]
    blocks: Dict[str, CalendarBlock]

    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


def _parse_hours(ranges: Iterable[str]) -> tuple[tuple[int, int], ...]:
    parsed: List[tuple[int, int]] = []
    for rng in ranges:
        start_str, end_str = rng.split("-")
        start = int(start_str)
        end = int(end_str)
        if start < 0 or end > 23 or start > end:
            raise ValueError(f"Invalid hour range '{rng}'")
        parsed.append((start, end))
    return tuple(parsed)


def _load_calendars(path: Path) -> Dict[str, Calendar]:
    with path.open(encoding="utf-8") as fh:
        if yaml is not None:
            raw = yaml.safe_load(fh) or {}
        else:
            raw = json.load(fh)
    calendars: Dict[str, Calendar] = {}
    for name, cfg in raw.items():
        blocks_cfg = cfg.get("blocks", {})
        blocks: Dict[str, CalendarBlock] = {}
        for block_name, block_val in blocks_cfg.items():
            rules_list = block_val.get("rules", [])
            rules: List[BlockRule] = []
            for rule in rules_list:
                weekdays = frozenset(int(day) for day in rule.get("weekdays", []))
                hours = _parse_hours(rule.get("hours", []))
                rules.append(BlockRule(weekdays=weekdays, hours=hours))
            blocks[block_name.upper()] = CalendarBlock(name=block_name.upper(), rules=tuple(rules))
        holiday_dates = tuple(date.fromisoformat(day) for day in cfg.get("holidays", []))
        calendars[name.lower()] = Calendar(
            name=name.lower(),
            timezone=cfg["timezone"],
            holidays=holiday_dates,
            blocks=blocks,
        )
    return calendars


@lru_cache(maxsize=1)
def get_calendars(config_path: Path | None = None) -> Dict[str, Calendar]:
    path = config_path or CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Calendar config not found: {path}")
    return _load_calendars(path)


def hours_for_block(calendar_name: str, block_name: str, target_date: date) -> List[datetime]:
    calendars = get_calendars()
    calendar = calendars.get(calendar_name.lower())
    if calendar is None:
        raise KeyError(f"Unknown calendar '{calendar_name}'")
    block = calendar.blocks.get(block_name.upper())
    if block is None:
        raise KeyError(f"Unknown block '{block_name}' for calendar '{calendar_name}'")

    weekday = target_date.isoweekday()
    tz = calendar.tz()
    selected_hours: List[int] = []
    for rule in block.rules:
        if weekday not in rule.weekdays:
            continue
        for start, end in rule.hours:
            selected_hours.extend(range(start, end + 1))
    if not selected_hours:
        return []
    unique_hours = sorted(set(selected_hours))
    return [datetime.combine(target_date, time(hour=h), tzinfo=tz) for h in unique_hours]


def expand_block_range(
    calendar_name: str,
    block_name: str,
    start_date: date,
    end_date: date,
    *,
    include_holidays: bool = False,
) -> List[datetime]:
    """Return timezone-aware datetimes covering the block between two dates.

    Args:
        calendar_name: Key referencing calendar definitions.
        block_name: Block identifier (e.g. ``ON_PEAK``).
        start_date: Inclusive start date.
        end_date: Inclusive end date.
        include_holidays: If ``False`` (default), skip dates listed as holidays.
    """

    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    calendars = get_calendars()
    calendar = calendars.get(calendar_name.lower())
    if calendar is None:
        raise KeyError(f"Unknown calendar '{calendar_name}'")

    results: List[datetime] = []
    current = start_date
    while current <= end_date:
        if include_holidays or current not in calendar.holidays:
            results.extend(hours_for_block(calendar_name, block_name, current))
        current += timedelta(days=1)
    return results


__all__ = [
    "Calendar",
    "CalendarBlock",
    "BlockRule",
    "get_calendars",
    "hours_for_block",
    "expand_block_range",
]
