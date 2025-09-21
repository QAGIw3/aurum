#!/usr/bin/env python
"""Generate synthetic canonical curve data for performance and integration testing."""
from __future__ import annotations

import argparse
import math
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

CANONICAL_COLUMNS = [
    "asof_date",
    "source_file",
    "sheet_name",
    "asset_class",
    "region",
    "iso",
    "location",
    "market",
    "product",
    "block",
    "spark_location",
    "price_type",
    "units_raw",
    "currency",
    "per_unit",
    "tenor_type",
    "contract_month",
    "tenor_label",
    "value",
    "bid",
    "ask",
    "mid",
    "curve_key",
    "version_hash",
    "_ingest_ts",
]


def _random_curve_key(seed: int, idx: int) -> str:
    random.seed(seed * 997 + idx)
    return "".join(random.choices("0123456789abcdef", k=64))


def _synthetic_rows(
    asof: date,
    iso: str,
    tenors: Iterable[str],
    *,
    seed: int,
    base_price: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, tenor in enumerate(tenors):
        mid = base_price + math.sin(idx / 3) * 1.5
        bid = mid - 0.5
        ask = mid + 0.5
        rows.append(
            {
                "asof_date": asof,
                "source_file": "synthetic.xlsx",
                "sheet_name": "Synthetic",
                "asset_class": "power",
                "region": "US",
                "iso": iso,
                "location": f"{iso}-HUB",
                "market": "DA",
                "product": "power",
                "block": "ON_PEAK",
                "spark_location": None,
                "price_type": "MID",
                "units_raw": "USD/MWh",
                "currency": "USD",
                "per_unit": "MWh",
                "tenor_type": "MONTHLY" if tenor[0].isdigit() else "SEASON",
                "contract_month": pd.to_datetime(tenor, errors="coerce"),
                "tenor_label": tenor,
                "value": mid,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "curve_key": _random_curve_key(seed, idx),
                "version_hash": _random_curve_key(seed + 5, idx),
                "_ingest_ts": pd.Timestamp.utcnow(),
            }
        )
    return rows


def build_dataset(
    start_date: date,
    days: int,
    isos: list[str],
    tenors: list[str],
    *,
    seed: int,
    base_price: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for offset in range(days):
        asof = start_date + timedelta(days=offset)
        for iso in isos:
            rows.extend(_synthetic_rows(asof, iso, tenors, seed=seed + offset, base_price=base_price))
    frame = pd.DataFrame(rows, columns=CANONICAL_COLUMNS)
    frame["_ingest_ts"] = pd.Timestamp.utcnow()
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic curve observations")
    parser.add_argument("--start", type=lambda s: date.fromisoformat(s), default=date.today(), help="Start as-of date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=1, help="Number of sequential as-of dates to generate")
    parser.add_argument("--isos", default="PJM,ERCOT", help="Comma-separated ISO codes")
    parser.add_argument("--tenors", default="2024-01,2024-02,2024-03", help="Comma-separated tenor labels (YYYY-MM or CAL)" )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--base-price", type=float, default=45.0, help="Baseline price for the synthetic curves")
    parser.add_argument("--output", type=Path, required=True, help="Output file (.parquet or .csv)")
    args = parser.parse_args()

    isos = [entry.strip().upper() for entry in args.isos.split(",") if entry.strip()]
    tenors = [entry.strip() for entry in args.tenors.split(",") if entry.strip()]
    df = build_dataset(args.start, args.days, isos, tenors, seed=args.seed, base_price=args.base_price)

    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".parquet":
        df.to_parquet(output, index=False)
    elif output.suffix.lower() == ".csv":
        df.to_csv(output, index=False)
    else:
        raise SystemExit("Output must end with .parquet or .csv")
    print(f"Wrote synthetic dataset with {len(df)} rows to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
