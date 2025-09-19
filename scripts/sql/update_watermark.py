#!/usr/bin/env python
"""CLI to register ingest sources and update watermarks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable

from aurum.db import register_ingest_source, update_ingest_watermark


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update ingest watermarks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    register_parser = subparsers.add_parser("register", help="Register or update an ingest source")
    register_parser.add_argument("name", help="Unique ingest source name")
    register_parser.add_argument("--description", help="Human-readable description")
    register_parser.add_argument("--schedule", help="Schedule description (cron expression)")
    register_parser.add_argument("--target", help="Target dataset/table")

    watermark_parser = subparsers.add_parser("watermark", help="Update the watermark for a source")
    watermark_parser.add_argument("name", help="Ingest source name")
    watermark_parser.add_argument(
        "--key",
        default="default",
        help="Watermark key (default: default)",
    )
    watermark_parser.add_argument(
        "--timestamp",
        required=True,
        help="Watermark timestamp (ISO-8601, assumed UTC if no timezone provided)",
    )

    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "register":
        register_ingest_source(
            args.name,
            description=args.description,
            schedule=args.schedule,
            target=args.target,
        )
        print(f"Registered ingest source {args.name}")
        return 0

    if args.command == "watermark":
        ts = datetime.fromisoformat(args.timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        update_ingest_watermark(args.name, args.key, ts)
        print(f"Updated watermark for {args.name}/{args.key} -> {ts.isoformat()}")
        return 0

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
