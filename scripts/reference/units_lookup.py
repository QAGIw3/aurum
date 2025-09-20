#!/usr/bin/env python
"""CLI to inspect unit mappings and canonical lists.

Examples:
- List mapping rows filtered by prefix:
    python scripts/reference/units_lookup.py --mode mapping --prefix USD
- List canonical currencies and units:
    python scripts/reference/units_lookup.py --mode canonical
"""
from __future__ import annotations

import argparse
import csv
import sys
from typing import Iterable

from aurum.reference.units import UnitsMapper


def _write_mapping(mapper: UnitsMapper, prefix: str | None) -> None:
    mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
    writer = csv.DictWriter(sys.stdout, fieldnames=["units_raw", "currency", "per_unit"])  # type: ignore[arg-type]
    writer.writeheader()
    for rec in sorted(mapping.values(), key=lambda r: r.raw):
        if prefix and not rec.raw.lower().startswith(prefix.lower()):
            continue
        writer.writerow({"units_raw": rec.raw, "currency": rec.currency, "per_unit": rec.per_unit})


def _write_canonical(mapper: UnitsMapper, prefix: str | None) -> None:
    mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
    currencies = sorted({rec.currency for rec in mapping.values() if rec.currency})
    units = sorted({rec.per_unit for rec in mapping.values() if rec.per_unit})
    if prefix:
        pfx = prefix.lower()
        currencies = [c for c in currencies if c.lower().startswith(pfx)]
        units = [u for u in units if u.lower().startswith(pfx)]
    writer = csv.DictWriter(sys.stdout, fieldnames=["type", "value"])  # type: ignore[arg-type]
    writer.writeheader()
    for c in currencies:
        writer.writerow({"type": "currency", "value": c})
    for u in units:
        writer.writerow({"type": "unit", "value": u})


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Inspect unit mappings and canonical lists")
    p.add_argument("--mode", choices=["mapping", "canonical"], default="mapping")
    p.add_argument("--prefix", help="Optional startswith filter")
    args = p.parse_args(argv)

    mapper = UnitsMapper()
    if args.mode == "mapping":
        _write_mapping(mapper, args.prefix)
    else:
        _write_canonical(mapper, args.prefix)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

