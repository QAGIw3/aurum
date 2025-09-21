#!/usr/bin/env python3
"""Register ingest sources (and optional watermarks) from the repo configs."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from aurum.db import register_ingest_source, update_ingest_watermark

REPO_ROOT = Path(__file__).resolve().parents[2]
EIA_SERIES_CONFIG = REPO_ROOT / "config" / "eia_ingest_datasets.json"
EIA_BULK_CONFIG = REPO_ROOT / "config" / "eia_bulk_datasets.json"


def _load_entries(path: Path, key: str) -> Iterable[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get(key, [])


def _register(entries: Iterable[dict], *, dsn: str | None, set_watermark: bool) -> None:
    for entry in entries:
        name = entry.get("source_name")
        if not name:
            continue
        description = entry.get("description")
        schedule = entry.get("schedule")
        target = entry.get("default_topic")
        register_ingest_source(
            name,
            description=description,
            schedule=schedule,
            target=target,
            dsn=dsn,
        )
        if set_watermark:
            watermark_ts = datetime.now(timezone.utc).replace(microsecond=0)
            update_ingest_watermark(name, "default", watermark_ts, dsn=dsn)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed ingest_source + ingest_watermark rows for dev")
    parser.add_argument("--dsn", help="Postgres DSN (defaults to env lookup)")
    parser.add_argument(
        "--with-watermarks",
        action="store_true",
        help="Also set the default watermark to 'now' for each source",
    )
    args = parser.parse_args()

    eia_series = list(_load_entries(EIA_SERIES_CONFIG, "datasets"))
    eia_bulk = list(_load_entries(EIA_BULK_CONFIG, "datasets"))
    if not eia_series and not eia_bulk:
        raise SystemExit("No dataset configs found; ensure config/eia_ingest_datasets.json exists")

    _register(eia_series, dsn=args.dsn, set_watermark=args.with_watermarks)
    _register(eia_bulk, dsn=args.dsn, set_watermark=args.with_watermarks)

    print(
        f"Seeded {len(eia_series)} EIA dataset sources and {len(eia_bulk)} bulk sources"
        + (" with watermarks" if args.with_watermarks else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
