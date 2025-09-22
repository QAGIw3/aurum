#!/usr/bin/env python3
"""Simple smoke test that hits CAISO OASIS for a tiny window and lands raw zips.

Usage:
  AURUM_KAFKA_BOOTSTRAP_SERVERS unset to skip Kafka. This script uses
  CaisoOasisCollector with kafka_cfg=None so it will only land raw zips.

Examples:
  python scripts/ingest/caiso_oasis_smoketest.py --minutes 30 --market RTPD
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from aurum.external.runner import _build_http_collector
from aurum.external.providers.caiso_collectors import CaisoOasisCollector


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--market", default="RTPD")
    parser.add_argument("--node", default=None)
    args = parser.parse_args()

    http = _build_http_collector("caiso-oasis", "https://oasis.caiso.com/oasisapi")
    collector = CaisoOasisCollector(http_collector=http, kafka_cfg=None)

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=max(5, args.minutes))
    end = now

    # LMP small window
    lmp_count = collector.ingest_prc_lmp(
        start_utc=start,
        end_utc=end,
        market_run_id=args.market,
        grp_all=bool(args.node is None),
        node=args.node,
    )
    # Also AS and Load/Forecast for the same window
    as_count = collector.ingest_as_results(start_utc=start, end_utc=end, market_run_id=args.market)
    load_count = collector.ingest_load_and_forecast(start_utc=start, end_utc=end, market_run_id=args.market)
    # And nodes crosswalk snapshot for today
    node_count = collector.ingest_nodes() + collector.ingest_apnodes()

    print(
        f"CAISO smoke test complete: LMP={lmp_count}, AS={as_count}, LOAD={load_count}, NODES={node_count}.\n"
        "Raw zip + manifest written under files/raw/caiso/oasis/."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

