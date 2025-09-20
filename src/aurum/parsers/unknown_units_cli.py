"""CLI to report unknown unit mappings from parsed vendor workbooks."""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from .runner import parse_files
from ..reference import map_units

LOG = logging.getLogger(__name__)


@dataclass
class UnknownUnitSummary:
    units_raw: str | None
    rows: int
    sample_iso: list[str]
    sample_market: list[str]
    sample_location: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "units_raw": self.units_raw,
            "rows": self.rows,
            "sample_iso": self.sample_iso,
            "sample_market": self.sample_market,
            "sample_location": self.sample_location,
        }


def _collect_unknown_units(df: pd.DataFrame) -> list[UnknownUnitSummary]:
    if df.empty or "units_raw" not in df.columns:
        return []

    summaries: list[UnknownUnitSummary] = []
    grouped = df.groupby("units_raw", dropna=False)
    for units_raw, group in grouped:
        raw_value = None if (units_raw is None or (isinstance(units_raw, float) and pd.isna(units_raw))) else units_raw
        try:
            currency, per_unit = map_units(raw_value)
        except Exception:  # pragma: no cover - unexpected mapper failure
            currency, per_unit = (None, None)
        if currency and per_unit:
            continue
        sample_iso = (
            group["iso"].dropna().astype(str).head(3).tolist()
            if "iso" in group.columns
            else []
        )
        sample_market = (
            group["market"].dropna().astype(str).head(3).tolist()
            if "market" in group.columns
            else []
        )
        sample_location = (
            group["location"].dropna().astype(str).head(3).tolist()
            if "location" in group.columns
            else []
        )
        summaries.append(
            UnknownUnitSummary(
                units_raw=None if (units_raw is None or pd.isna(units_raw)) else str(units_raw),
                rows=len(group),
                sample_iso=sample_iso,
                sample_market=sample_market,
                sample_location=sample_location,
            )
        )
    summaries.sort(key=lambda s: (s.units_raw or "", -s.rows))
    return summaries


def _iter_workbooks(paths: Iterable[Path]) -> list[Path]:
    workbook_paths: list[Path] = []
    for path in paths:
        if path.is_dir():
            workbook_paths.extend(sorted(path.glob("*.xlsx")))
            workbook_paths.extend(sorted(path.glob("*.xls")))
            continue
        if path.suffix.lower() not in {".xlsx", ".xls"}:
            LOG.warning("Skipping non-Excel file %s", path)
            continue
        workbook_paths.append(path)
    return workbook_paths


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report unknown unit mappings across vendor workbooks")
    parser.add_argument("paths", nargs="+", type=Path, help="Workbook paths or directories")
    parser.add_argument("--as-of", dest="as_of", help="Optional as-of date override (YYYY-MM-DD)")
    parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON instead of table output")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args(argv)


def _print_table(summaries: list[UnknownUnitSummary]) -> None:
    if not summaries:
        print("No unknown units detected")
        return
    col_widths = {
        "units_raw": max(len("Units Raw"), *(len(s.units_raw or "<blank>") for s in summaries)),
        "rows": max(len("Rows"), *(len(str(s.rows)) for s in summaries)),
        "sample_iso": max(len("Sample ISO"), *(len(", ".join(s.sample_iso)) for s in summaries)),
        "sample_market": max(len("Sample Market"), *(len(", ".join(s.sample_market)) for s in summaries)),
        "sample_location": max(len("Sample Location"), *(len(", ".join(s.sample_location)) for s in summaries)),
    }
    header = (
        f"{ 'Units Raw'.ljust(col_widths['units_raw']) }  "
        f"{ 'Rows'.rjust(col_widths['rows']) }  "
        f"{ 'Sample ISO'.ljust(col_widths['sample_iso']) }  "
        f"{ 'Sample Market'.ljust(col_widths['sample_market']) }  "
        f"{ 'Sample Location'.ljust(col_widths['sample_location']) }"
    )
    print(header)
    print("-" * len(header))
    for summary in summaries:
        units_text = summary.units_raw or "<blank>"
        print(
            f"{ units_text.ljust(col_widths['units_raw']) }  "
            f"{ str(summary.rows).rjust(col_widths['rows']) }  "
            f"{ ', '.join(summary.sample_iso).ljust(col_widths['sample_iso']) }  "
            f"{ ', '.join(summary.sample_market).ljust(col_widths['sample_market']) }  "
            f"{ ', '.join(summary.sample_location).ljust(col_widths['sample_location']) }"
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    as_of: date | None = None
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)

    workbooks = _iter_workbooks(args.paths)
    if not workbooks:
        LOG.error("No workbook inputs found")
        return 1

    df = parse_files(workbooks, as_of=as_of)
    summaries = _collect_unknown_units(df)

    if args.as_json:
        payload = [summary.to_dict() for summary in summaries]
        print(json.dumps(payload, indent=2))
    else:
        _print_table(summaries)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
