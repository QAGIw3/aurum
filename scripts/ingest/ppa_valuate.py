#!/usr/bin/env python3
"""Trigger PPA valuation via the Aurum API."""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

import requests


def _build_session(token: str | None) -> requests.Session:
    session = requests.Session()
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
    session.headers.update({"User-Agent": "aurum-ppa-cli/0.1"})
    return session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Request a PPA valuation via the Aurum API")
    parser.add_argument("ppa_contract_id", help="PPA contract identifier")
    parser.add_argument("scenario_id", help="Scenario identifier")
    parser.add_argument("--base-url", default=os.getenv("AURUM_API_BASE_URL", "http://localhost:8095"))
    parser.add_argument("--tenant", default=os.getenv("AURUM_TENANT_ID"), help="Tenant identifier header")
    parser.add_argument("--token", default=os.getenv("AURUM_API_TOKEN"), help="Bearer token")
    parser.add_argument("--asof", help="Optional valuation as-of date (YYYY-MM-DD)")
    parser.add_argument("--ppa-price", type=float, help="Override PPA strike price")
    parser.add_argument("--volume", type=float, help="Energy volume per bucket (MWh)")
    parser.add_argument("--discount-rate", type=float, help="Annual discount rate")
    parser.add_argument("--upfront-cost", type=float, help="Upfront cost to include in NPV/IRR")
    parser.add_argument("--json", action="store_true", help="Print raw JSON response")

    args = parser.parse_args(argv)
    session = _build_session(args.token)

    headers: dict[str, str] = {}
    if args.tenant:
        headers["X-Aurum-Tenant"] = args.tenant

    payload: dict[str, Any] = {
        "ppa_contract_id": args.ppa_contract_id,
        "scenario_id": args.scenario_id,
    }
    if args.asof:
        payload["asof_date"] = args.asof
    options: dict[str, Any] = {}
    if args.ppa_price is not None:
        options["ppa_price"] = args.ppa_price
    if args.volume is not None:
        options["volume_mwh"] = args.volume
    if args.discount_rate is not None:
        options["discount_rate"] = args.discount_rate
    if args.upfront_cost is not None:
        options["upfront_cost"] = args.upfront_cost
    if options:
        payload["options"] = options

    url = f"{args.base_url.rstrip('/')}/v1/ppa/valuate"
    response = session.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code >= 400:
        raise SystemExit(f"Request failed: {response.status_code} {response.text}")
    data = response.json()

    if args.json:
        print(json.dumps(data, indent=2, sort_keys=True))
    else:
        rows = data.get("data", []) or []
        if not rows:
            print("No valuation rows returned.")
        for row in rows:
            metric = row.get("metric")
            value = row.get("value")
            period_start = row.get("period_start")
            period_end = row.get("period_end")
            print(f"{metric:<10} {value:>12} ({period_start} -> {period_end})")
        meta = data.get("meta") or {}
        if meta:
            print("Meta:", json.dumps(meta, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
