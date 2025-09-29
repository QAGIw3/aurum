from __future__ import annotations

"""Asynchronous generators backing GraphQL subscriptions."""

import asyncio
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from strawberry.types import Info

from ...telemetry.context import log_structured
from ..services.risk_compliance_service import get_risk_compliance_service
from .resolvers import EnergyMarketKey, get_iso_loader


async def energy_market_update_stream(
    info: Info,
    keys: List[EnergyMarketKey],
    *,
    poll_interval: float = 5.0,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Yield newest ISO market observations for the provided filters."""

    loader = get_iso_loader(info)
    last_seen: Dict[EnergyMarketKey, Optional[datetime]] = {key: None for key in keys}

    while True:
        for key in keys:
            try:
                rows, _ = await loader.load(key)
            except Exception as exc:  # pragma: no cover - defensive logging only
                log_structured(
                    "graphql_energy_stream_error",
                    error=str(exc),
                    iso_code=key.iso_code,
                    location_id=key.location_id,
                )
                continue

            if not rows:
                continue

            latest = rows[0]
            timestamp = latest.get("interval_start")
            if last_seen.get(key) == timestamp:
                continue

            last_seen[key] = timestamp
            yield {
                "filter": {
                    "iso_code": key.iso_code,
                    "market": key.market,
                    "location_id": key.location_id,
                    "granularity": key.granularity,
                    "limit": key.limit,
                    "start": key.start,
                    "end": key.end,
                },
                "point": latest,
            }

        await asyncio.sleep(max(1.0, poll_interval))


async def compliance_report_stream(
    portfolio_ids: List[str],
    *,
    poll_interval: float = 15.0,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Emit new compliance report artifacts as they are generated."""

    service = get_risk_compliance_service()
    seen: Dict[str, Optional[str]] = {pid: None for pid in portfolio_ids}

    while True:
        for portfolio_id in portfolio_ids:
            try:
                reports = await service.list_reports(portfolio_id, limit=1)
            except Exception as exc:  # pragma: no cover
                log_structured(
                    "graphql_compliance_stream_error",
                    error=str(exc),
                    portfolio_id=portfolio_id,
                )
                continue

            if not reports:
                continue

            latest = reports[0]
            filename = latest.get("filename")
            if seen.get(portfolio_id) == filename:
                continue

            seen[portfolio_id] = filename
            latest.setdefault("portfolio_id", portfolio_id)
            yield latest

        await asyncio.sleep(max(5.0, poll_interval))


__all__ = [
    "energy_market_update_stream",
    "compliance_report_stream",
]
