"""Shared helpers for PPA valuation endpoints."""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from .state import get_settings

LOGGER = logging.getLogger(__name__)


def _settings():
    """Return configured application settings."""
    return get_settings()


_PPA_DECIMAL_QUANTIZER = Decimal("0.000001")


def _scenario_outputs_enabled() -> bool:
    """Check whether scenario outputs APIs are enabled via settings."""
    return _settings().api.scenario_outputs_enabled


def _quantize_ppa_decimal(value: Any) -> Optional[Decimal]:
    """Normalize raw numeric inputs to a Decimal with fixed precision."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        try:
            return value.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None
    if isinstance(value, float) and value != value:  # NaN check
        return None
    try:
        as_decimal = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    try:
        return as_decimal.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def _persist_ppa_valuation_records(
    *,
    ppa_contract_id: str,
    scenario_id: str,
    tenant_id: Optional[str],
    asof_date: date,
    metrics: List[Dict[str, Any]],
) -> None:
    """Persist PPA valuation metrics when the feature flag is enabled."""
    settings = _settings()
    if not settings.api.ppa_write_enabled:
        return
    if not metrics:
        return

    try:
        import pandas as pd
    except ModuleNotFoundError:
        LOGGER.debug("Skipping PPA valuation persistence: pandas not available")
        return

    try:
        from aurum.parsers.iceberg_writer import write_ppa_valuation
    except ModuleNotFoundError:
        LOGGER.debug("Skipping PPA valuation persistence: pyiceberg writer not available")
        return

    records: List[Dict[str, Any]] = []
    now = datetime.utcnow().replace(microsecond=0)
    curve_hint = next((m.get("curve_key") for m in metrics if m.get("curve_key")), None)

    for metric in metrics:
        metric_name = str(metric.get("metric"))
        raw_value = metric.get("value")
        decimal_value = _quantize_ppa_decimal(raw_value)
        period_start = metric.get("period_start") or asof_date
        period_end = metric.get("period_end") or period_start
        curve_key = metric.get("curve_key") or curve_hint
        if curve_key:
            curve_hint = curve_key

        if isinstance(decimal_value, Decimal):
            value_for_hash = format(decimal_value, "f")
        elif raw_value is None:
            value_for_hash = ""
        else:
            value_for_hash = str(raw_value)

        record: Dict[str, Any] = {
            "asof_date": asof_date,
            "ppa_contract_id": ppa_contract_id,
            "scenario_id": scenario_id,
            "tenant_id": tenant_id,
            "curve_key": curve_key,
            "period_start": period_start,
            "period_end": period_end,
            "cashflow": None,
            "npv": None,
            "irr": None,
            "metric": metric_name,
            "value": decimal_value,
            "version_hash": hashlib.sha256(
                f"{ppa_contract_id}|{scenario_id}|{metric_name}|{period_start}|{period_end}|{value_for_hash}".encode("utf-8")
            ).hexdigest()[:16],
            "_ingest_ts": now,
        }

        metric_lower = metric_name.lower()
        if metric_lower == "cashflow":
            record["cashflow"] = decimal_value
        elif metric_lower == "npv":
            record["npv"] = decimal_value
        elif metric_lower == "irr":
            try:
                record["irr"] = float(raw_value) if raw_value is not None else None
            except (TypeError, ValueError):
                record["irr"] = float(decimal_value) if isinstance(decimal_value, Decimal) else None

        records.append(record)

    if not records:
        return

    try:
        frame = pd.DataFrame(records)
        write_ppa_valuation(frame)
    except Exception as exc:  # pragma: no cover - integration failure
        LOGGER.warning(
            "Failed to persist PPA valuation for %s/%s: %s",
            scenario_id,
            ppa_contract_id,
            exc,
        )


__all__ = [
    "_scenario_outputs_enabled",
    "_persist_ppa_valuation_records",
]
