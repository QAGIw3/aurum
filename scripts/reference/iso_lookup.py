#!/usr/bin/env python
"""CLI to query ISO location metadata from config/iso_nodes.csv.

Examples:
- List all CAISO locations:
    python scripts/reference/iso_lookup.py --iso CAISO
- Lookup a specific PJM location by id:
    python scripts/reference/iso_lookup.py --iso PJM --id AECO
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

from aurum.reference.iso_locations import IsoLocation, get_location, iter_locations


def _write_csv(rows: Iterable[IsoLocation], fh) -> None:
    fieldnames = [
        "iso",
        "location_id",
        "location_name",
        "location_type",
        "zone",
        "hub",
        "timezone",
    ]
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    writer.writeheader()
    for loc in rows:
        writer.writerow(asdict(loc))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Query ISO location registry")
    p.add_argument("--iso", help="Filter by ISO code (e.g. PJM, CAISO)")
    p.add_argument("--id", dest="location_id", help="Lookup a specific location id")
    p.add_argument("--name", help="Case-insensitive substring match on location name")
    args = p.parse_args(argv)

    if args.location_id:
        if not args.iso:
            print("--iso is required when using --id", file=sys.stderr)
            return 2
        loc = get_location(args.iso, args.location_id)
        if loc is None:
            print("No match", file=sys.stderr)
            return 1
        _write_csv([loc], sys.stdout)
        return 0

    # list mode
    locations = list(iter_locations(args.iso))
    if args.name:
        needle = args.name.lower()
        locations = [
            loc for loc in locations if loc.location_name and needle in loc.location_name.lower()
        ]
    _write_csv(locations, sys.stdout)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

