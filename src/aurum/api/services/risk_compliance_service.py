"""Risk compliance scheduling service: daily reports + retention.

This service schedules daily compliance reports (CFTC/FERC style) using the
`aurum.risk` engines and persists artifacts under `artifacts/risk/compliance`.
Retention policy is enforced per schedule by days and/or max reports.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
import contextlib
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from ...observability.telemetry_facade import get_telemetry_facade
from ...observability.telemetry_facade import MetricCategory
from ...observability.logging import get_logger

from ...telemetry.context import get_tenant_id

# Tenant context may be used for tagging; no hard dependency on tenancy enums

from ...core.settings import get_settings as _get_settings

from ...risk import (
    PortfolioInput,
    PositionInput,
    VaREngine,
    ComplianceReportConfig,
    ComplianceReportGenerator,
    RiskLimitsConfig,
)
from .risk_engine_service import get_risk_engine_service


ARTIFACT_ROOT = Path("artifacts/risk/compliance")


class ComplianceSchedule(BaseModel):
    schedule_id: str
    portfolio: Optional[PortfolioInput] = None
    portfolio_id: Optional[str] = None
    report_config: ComplianceReportConfig
    schedule_time_utc: str = Field("00:00", description="Daily time HH:MM UTC")
    enabled: bool = True
    retention_days: int = Field(30, ge=1)
    max_reports: Optional[int] = Field(100, ge=1)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None


def _parse_utc_time(spec: str) -> Tuple[int, int]:
    parts = (spec or "00:00").split(":", 1)
    try:
        hh = int(parts[0]); mm = int(parts[1]) if len(parts) > 1 else 0
        return (max(0, min(23, hh)), max(0, min(59, mm)))
    except Exception:
        return (0, 0)


def _compute_next_run(schedule_time_utc: str, now: Optional[datetime] = None) -> datetime:
    now = now or datetime.utcnow()
    hh, mm = _parse_utc_time(schedule_time_utc)
    candidate = datetime.combine(now.date(), dt_time(hour=hh, minute=mm))
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


class RiskComplianceService:
    def __init__(self) -> None:
        self._schedules: Dict[str, ComplianceSchedule] = {}
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._telemetry = None
        try:
            self._telemetry = get_telemetry_facade()
        except Exception:
            self._telemetry = None
        self._logger = get_logger(__name__)
        ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
        self._engine = VaREngine()
        self._generator = ComplianceReportGenerator(self._engine)

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
            self._task = None

    def create_schedule(
        self,
        portfolio: Optional[PortfolioInput] = None,
        report_config: ComplianceReportConfig = None,  # type: ignore[assignment]
        *,
        portfolio_id: Optional[str] = None,
        schedule_time_utc: str = "00:00",
        retention_days: int = 30,
        max_reports: Optional[int] = 100,
    ) -> str:
        from uuid import uuid4
        sid = str(uuid4())
        next_run = _compute_next_run(schedule_time_utc)
        sched = ComplianceSchedule(
            schedule_id=sid,
            portfolio=portfolio,
            portfolio_id=portfolio_id,
            report_config=report_config,
            schedule_time_utc=schedule_time_utc,
            retention_days=retention_days,
            max_reports=max_reports,
            next_run=next_run,
        )
        self._schedules[sid] = sched
        return sid

    async def list_schedules(self) -> List[ComplianceSchedule]:
        return sorted(self._schedules.values(), key=lambda s: (s.next_run or datetime.max))

    def delete_schedule(self, schedule_id: str) -> bool:
        return self._schedules.pop(schedule_id, None) is not None

    async def run_report_now(self, schedule_id: str) -> Optional[Path]:
        sched = self._schedules.get(schedule_id)
        if not sched:
            return None
        return await self._generate_and_persist(sched)

    async def run_report_for_portfolio(
        self, portfolio: PortfolioInput, cfg: ComplianceReportConfig
    ) -> Path:
        sched = ComplianceSchedule(
            schedule_id="on_demand",
            portfolio=portfolio,
            report_config=cfg,
            schedule_time_utc="00:00",
            retention_days=cfg.risk_limits.max_cvar and 30 or 30,
        )
        return await self._generate_and_persist(sched)

    async def list_reports(self, portfolio_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        root = ARTIFACT_ROOT / portfolio_id
        if not root.exists():
            return []
        items = []
        for p in sorted(root.glob("*.json"), reverse=True):
            try:
                items.append({
                    "path": str(p),
                    "filename": p.name,
                    "size": p.stat().st_size,
                    "modified": datetime.utcfromtimestamp(p.stat().st_mtime).isoformat() + "Z",
                })
            except Exception:
                continue
        return items[:limit]

    async def _scheduler_loop(self) -> None:
        try:
            while not self._shutdown.is_set():
                now = datetime.utcnow()
                for sched in list(self._schedules.values()):
                    if not sched.enabled or not sched.next_run:
                        continue
                    if now >= sched.next_run:
                        await self._generate_and_persist(sched)
                        # Compute next run
                        sched.last_run = now
                        sched.next_run = _compute_next_run(sched.schedule_time_utc, now)
                await asyncio.sleep(30)  # check twice a minute
        except asyncio.CancelledError:  # graceful exit
            return
        except Exception as exc:
            if self._telemetry:
                self._telemetry.error("risk_compliance.scheduler_error", error=str(exc))

    def _artifact_path(self, portfolio_id: str, as_of: datetime) -> Path:
        day = as_of.strftime("%Y-%m-%d")
        folder = ARTIFACT_ROOT / portfolio_id
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"compliance_{day}.json"

    async def _generate_and_persist(self, sched: ComplianceSchedule) -> Optional[Path]:
        try:
            # Resolve portfolio (prefer explicit, else fetch from risk engine service)
            portfolio = sched.portfolio
            if portfolio is None and sched.portfolio_id:
                portfolio = await _resolve_portfolio_from_service(sched.portfolio_id)
                if portfolio is None:
                    raise RuntimeError(f"No positions found for portfolio {sched.portfolio_id}")

            # Generate report
            report = self._generator.generate(portfolio, sched.report_config)

            # Alert on breaches if present
            breaches = []
            try:
                if report.ferc and isinstance(report.ferc.breaches, list):
                    breaches = report.ferc.breaches
            except Exception:
                breaches = []
            if breaches and self._telemetry and hasattr(self._telemetry, "create_alert"):
                for b in breaches:
                    try:
                        limit_name = b.get("limit_name") if isinstance(b, dict) else getattr(b, "limit_name", "")
                        value = b.get("value") if isinstance(b, dict) else getattr(b, "value", 0.0)
                        threshold = b.get("threshold") if isinstance(b, dict) else getattr(b, "threshold", 0.0)
                        severity = b.get("severity") if isinstance(b, dict) else getattr(b, "severity", "MEDIUM")
                        self._telemetry.create_alert(
                            title=f"Compliance breach: {limit_name}",
                            message=f"{sched.portfolio.portfolio_id}: {limit_name} {value:.4f} > {threshold:.4f}",
                            severity=str(severity),
                            portfolio_id=sched.portfolio.portfolio_id,
                            component="risk.compliance",
                        )
                    except Exception:
                        continue

            # Persist JSON
            as_of = datetime.utcnow()
            path = self._artifact_path(portfolio.portfolio_id, as_of)
            with path.open("w", encoding="utf-8") as f:
                json.dump(json.loads(report.model_dump_json()), f, default=str)

            # Retention
            await self._enforce_retention(sched)

            # Telemetry metric
            if self._telemetry:
                self._telemetry.record_counter(
                    "risk_compliance_reports",
                    1,
                    category=MetricCategory.BUSINESS,
                    portfolio_id=portfolio.portfolio_id,
                )

            return path
        except Exception as exc:
            if self._telemetry:
                self._telemetry.error("risk_compliance.generate_failed", error=str(exc))
            return None

    async def _enforce_retention(self, sched: ComplianceSchedule) -> None:
        root = ARTIFACT_ROOT / sched.portfolio.portfolio_id
        if not root.exists():
            return
        files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        # By count
        if sched.max_reports is not None and len(files) > sched.max_reports:
            for p in files[sched.max_reports:]:
                with contextlib.suppress(Exception):
                    p.unlink()
        # By age
        cutoff = datetime.utcnow() - timedelta(days=int(sched.retention_days or 30))
        for p in files:
            try:
                mtime = datetime.utcfromtimestamp(p.stat().st_mtime)
                if mtime < cutoff:
                    p.unlink()
            except Exception:
                continue


async def _resolve_portfolio_from_service(portfolio_id: str) -> Optional[PortfolioInput]:
    """Build PortfolioInput from positions stored in the risk engine service."""
    try:
        service = get_risk_engine_service()
        positions = await service.get_portfolio_positions(portfolio_id)  # type: ignore[attr-defined]
        if not positions:
            return None
        conv: List[PositionInput] = []
        for p in positions:
            conv.append(
                PositionInput(
                    asset_id=p.asset_id,
                    notional_value=float(p.notional_value),
                    position_type=p.position_type,
                    currency=p.currency,
                    risk_factors=dict(p.risk_factors or {}),
                    counterparty=None,
                    credit_rating=None,
                )
            )
        return PortfolioInput(portfolio_id=portfolio_id, positions=conv)
    except Exception:
        return None


_risk_compliance_service: Optional[RiskComplianceService] = None


def get_risk_compliance_service() -> RiskComplianceService:
    global _risk_compliance_service
    if _risk_compliance_service is None:
        _risk_compliance_service = RiskComplianceService()
    return _risk_compliance_service


# Lifecycle hook to start/stop scheduler with app
from ..lifespan_manager import LifecycleHook


class RiskComplianceLifecycleHook(LifecycleHook):
    def __init__(self) -> None:
        super().__init__("risk_compliance", priority=55)
        self._svc: Optional[RiskComplianceService] = None

    async def startup(self) -> None:
        self._svc = get_risk_compliance_service()
        await self._svc.start()

    async def shutdown(self) -> None:
        if self._svc:
            await self._svc.stop()
