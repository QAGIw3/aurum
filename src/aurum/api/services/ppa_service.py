"""Domain service for PPA contracts and valuations."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from ..config import TrinoConfig
from ..dao.ppa_dao import PpaDao
from ..query import build_filter_clause
from ..scenarios.scenario_service import STORE as ScenarioStore


def coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_date(value: Any, fallback: date) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return fallback
    return fallback


def month_end(day: date) -> date:
    last_day = monthrange(day.year, day.month)[1]
    return day.replace(day=last_day)


def month_offset(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def extract_currency(row: Dict[str, Any]) -> Optional[str]:
    currency = row.get("metric_currency")
    if isinstance(currency, str) and currency.strip():
        return currency.strip()
    unit = row.get("metric_unit")
    if isinstance(unit, str) and unit.strip():
        return unit.split("/", 1)[0].strip()
    return None


def compute_irr(
    cashflows: List[float], *, tolerance: float = 1e-6, max_iterations: int = 80
) -> Optional[float]:
    if not cashflows:
        return None
    if all(cf >= 0 for cf in cashflows) or all(cf <= 0 for cf in cashflows):
        return None

    def npv(rate: float) -> float:
        total = 0.0
        for idx, cf in enumerate(cashflows):
            total += cf / (1.0 + rate) ** idx
        return total

    low, high = -0.9999, 10.0
    f_low = npv(low)
    f_high = npv(high)
    if f_low == 0:
        return low
    if f_high == 0:
        return high
    if f_low * f_high > 0:
        return None

    mid = (low + high) / 2.0
    for _ in range(max_iterations):
        mid = (low + high) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < tolerance:
            return mid
        if f_low * f_mid < 0:
            high = mid
            f_high = f_mid
        else:
            low = mid
            f_low = f_mid
    return mid


class PpaService:
    """Encapsulates PPA contract listing and valuation logic."""

    def __init__(self, dao: Optional[PpaDao] = None) -> None:
        self._dao = dao or PpaDao()

    # ------------------------------------------------------------------
    async def list_contracts(
        self,
        *,
        tenant_id: Optional[str],
        offset: int,
        limit: int,
        counterparty_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        records = ScenarioStore.list_ppa_contracts(tenant_id, limit=offset + limit, offset=0)
        page = records[offset : offset + limit]

        items: List[Dict[str, Any]] = []
        for record in page:
            terms = record.terms if isinstance(record.terms, dict) else {}
            counterparty = terms.get("counterparty") or "unknown"
            if counterparty_filter and counterparty_filter.lower() not in str(counterparty).lower():
                continue
            capacity_mw = coerce_float(terms.get("capacity_mw"), 0.0)
            price_usd_mwh = coerce_float(terms.get("price_usd_mwh"), 0.0)
            start_date_val = str(terms.get("start_date") or "")
            end_date_val = str(terms.get("end_date") or "")
            items.append(
                {
                    "contract_id": record.id,
                    "name": str(record.instrument_id or record.id),
                    "counterparty": counterparty,
                    "capacity_mw": capacity_mw,
                    "price_usd_mwh": price_usd_mwh,
                    "start_date": start_date_val,
                    "end_date": end_date_val,
                }
            )
        return items

    # ------------------------------------------------------------------
    def list_contract_valuation_rows(
        self,
        *,
        contract_id: str,
        scenario_id: Optional[str],
        metric: Optional[str],
        tenant_id: Optional[str],
        limit: int,
        offset: int,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        filters: Dict[str, Optional[str]] = {
            "ppa_contract_id": contract_id,
            "tenant_id": tenant_id,
        }
        if scenario_id:
            filters["scenario_id"] = scenario_id
        if metric:
            filters["metric"] = metric
        where = build_filter_clause(filters)
        sql = (
            "SELECT cast(asof_date as date) as asof_date, "
            "cast(period_start as date) as period_start, "
            "cast(period_end as date) as period_end, "
            "scenario_id, tenant_id, curve_key, metric, value, cashflow, npv, irr, metric_currency, version_hash, _ingest_ts "
            "FROM iceberg.market.ppa_valuation"
            f"{where} "
            "ORDER BY asof_date DESC, scenario_id, metric, period_start DESC "
            f"LIMIT {int(limit)} OFFSET {int(offset)}"
        )

        rows, elapsed = self._dao.execute(sql, trino_cfg=trino_cfg)
        for record in rows:
            for numeric_key in ("value", "cashflow", "npv", "irr"):
                if record.get(numeric_key) is not None:
                    record[numeric_key] = float(record[numeric_key])
        return rows, elapsed

    # ------------------------------------------------------------------
    def calculate_valuation(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        asof_date: Optional[date] = None,
        metric: Optional[str] = "mid",
        options: Optional[Dict[str, Any]] = None,
        trino_cfg: Optional[TrinoConfig] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        filters: Dict[str, Optional[str]] = {"scenario_id": scenario_id}
        if tenant_id:
            filters["tenant_id"] = tenant_id
        target_metric = (metric or "mid").strip() or "mid"
        filters["metric"] = target_metric
        where = build_filter_clause(filters)
        if asof_date:
            clause = f"asof_date = DATE '{asof_date.isoformat()}'"
            where = where + (" AND " if where else " WHERE ") + clause

        sql = (
            "SELECT cast(contract_month as date) as contract_month, "
            "cast(asof_date as date) as asof_date, tenor_label, metric, value, "
            "metric_currency, metric_unit, metric_unit_denominator, curve_key, tenor_type, run_id "
            "FROM mart_scenario_output"
            f"{where} ORDER BY contract_month NULLS LAST, tenor_label"
        )

        rows, elapsed = self._dao.execute(sql, trino_cfg=trino_cfg)
        if not rows:
            return [], elapsed

        opts = options or {}
        ppa_price = coerce_float(opts.get("ppa_price"), 0.0)
        volume = coerce_float(opts.get("volume_mwh"), 1.0)
        discount_rate = max(coerce_float(opts.get("discount_rate"), 0.0), -0.9999)
        upfront_cost = coerce_float(opts.get("upfront_cost"), 0.0)
        monthly_rate = (1.0 + discount_rate) ** (1.0 / 12.0) - 1.0 if discount_rate else 0.0

        fallback_date = asof_date or date.today()
        base_period: Optional[date] = None
        latest_period: Optional[date] = None
        npv_total = -upfront_cost
        cashflows_by_offset: Dict[int, float] = {}
        metrics_out: List[Dict[str, Any]] = []
        currency_hint: Optional[str] = None
        curve_hint: Optional[str] = None
        run_hint: Optional[str] = None
        tenor_hint: Optional[str] = None

        for row in rows:
            if str(row.get("metric")) != target_metric:
                continue
            period_candidate = row.get("contract_month") or row.get("asof_date") or fallback_date
            period = coerce_date(period_candidate, fallback_date)
            base_period = base_period or period
            latest_period = period
            offset = month_offset(base_period, period) + 1
            price_value = coerce_float(row.get("value"), 0.0)
            currency = extract_currency(row)

            curve_hint = curve_hint or row.get("curve_key")
            run_hint = run_hint or row.get("run_id")
            tenor_hint = tenor_hint or row.get("tenor_type")
            currency_hint = currency_hint or currency

            cashflow = (price_value - ppa_price) * volume
            discount_factor = (1.0 + monthly_rate) ** offset if monthly_rate else 1.0
            discounted = cashflow / discount_factor
            npv_total += discounted
            cashflows_by_offset[offset] = cashflows_by_offset.get(offset, 0.0) + cashflow

            metrics_out.append(
                {
                    "period_start": period,
                    "period_end": month_end(period),
                    "metric": "cashflow",
                    "value": round(cashflow, 4),
                    "currency": currency,
                    "unit": currency,
                    "curve_key": row.get("curve_key"),
                    "run_id": row.get("run_id"),
                    "tenor_type": row.get("tenor_type"),
                }
            )

        if not metrics_out:
            return [], elapsed

        summary_start = base_period or fallback_date
        summary_end = month_end(latest_period) if latest_period else summary_start
        currency_summary = currency_hint or metrics_out[0].get("currency")
        metrics_out.append(
            {
                "period_start": summary_start,
                "period_end": summary_end,
                "metric": "NPV",
                "value": round(npv_total, 4),
                "currency": currency_summary,
                "unit": currency_summary,
                "curve_key": curve_hint,
                "run_id": run_hint,
                "tenor_type": tenor_hint,
            }
        )

        if cashflows_by_offset:
            max_offset = max(cashflows_by_offset)
            series = [-upfront_cost] + [cashflows_by_offset.get(idx, 0.0) for idx in range(1, max_offset + 1)]
            irr = compute_irr(series)
            if irr is not None:
                metrics_out.append(
                    {
                        "period_start": summary_start,
                        "period_end": summary_end,
                        "metric": "IRR",
                        "value": round(irr, 6),
                        "currency": None,
                        "unit": "ratio",
                        "curve_key": curve_hint,
                        "run_id": run_hint,
                        "tenor_type": tenor_hint,
                    }
                )

        return metrics_out, elapsed


__all__ = [
    "PpaService",
    "coerce_float",
    "coerce_date",
    "month_end",
    "month_offset",
    "extract_currency",
    "compute_irr",
]

