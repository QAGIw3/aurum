"""v2 Risk Management API exposing VaR, stress testing, and compliance reports.

These endpoints are lightweight wrappers around `aurum.risk` modules and a
minimal compliance scheduler service.
"""

from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ...observability.telemetry_facade import get_telemetry_facade

from ...risk import (
    VaREngine,
    VaRConfig,
    VaRMethod,
    RiskLimitsConfig,
    PortfolioInput,
    PositionInput,
    VaRResult,
    StressTester,
    StressScenario,
    StressTestConfig,
    StressTestResult,
    ComplianceReportGenerator,
    ComplianceReportConfig,
    ComplianceReport,
)
from ..services.risk_engine_service import get_risk_engine_service

from ..services.risk_compliance_service import (
    get_risk_compliance_service,
    RiskComplianceLifecycleHook,  # exported for wiring into lifespan if needed
    ComplianceSchedule,
)


router = APIRouter(prefix="/v2/risk", tags=["risk"])


# -------- VaR


class VaRRequest(BaseModel):
    portfolio: Optional[PortfolioInput] = None
    portfolio_id: Optional[str] = None
    config: VaRConfig = Field(default_factory=VaRConfig)
    limits: Optional[RiskLimitsConfig] = None
    # Optional factor returns; if omitted, engine uses identity covariance or Monte Carlo
    factor_returns: Optional[Dict[str, List[float]]] = None


async def _resolve_portfolio(portfolio: Optional[PortfolioInput], portfolio_id: Optional[str]) -> PortfolioInput:
    if portfolio is not None:
        return portfolio
    if portfolio_id:
        service = get_risk_engine_service()
        positions = await service.get_portfolio_positions(portfolio_id)
        if not positions:
            raise HTTPException(status_code=404, detail=f"No positions found for portfolio {portfolio_id}")
        conv = [
            PositionInput(
                asset_id=p.asset_id,
                notional_value=float(p.notional_value),
                position_type=p.position_type,
                currency=p.currency,
                risk_factors=dict(p.risk_factors or {}),
            )
            for p in positions
        ]
        return PortfolioInput(portfolio_id=portfolio_id, positions=conv)
    raise HTTPException(status_code=400, detail="Provide either portfolio or portfolio_id")


@router.post("/var/calculate", response_model=VaRResult)
async def calculate_var(request: Request, payload: VaRRequest) -> VaRResult:
    start = time.perf_counter()
    try:
        engine = VaREngine()
        portfolio = await _resolve_portfolio(payload.portfolio, payload.portfolio_id)
        fr = None
        if payload.factor_returns:
            fr = {k: list(map(float, v)) for k, v in payload.factor_returns.items()}
        result = engine.calculate_var(portfolio, payload.config, factor_returns=fr, limits=payload.limits)
        get_telemetry_facade().record_success(
            operation="calculate_var_v2",
            query_time_ms=(time.perf_counter() - start) * 1000
        )
        return result
    except Exception as exc:
        get_telemetry_facade().record_error(operation="calculate_var_v2", error=exc, query_time_ms=(time.perf_counter() - start) * 1000)
        raise HTTPException(status_code=500, detail=f"Failed to calculate VaR: {exc}")


# -------- Stress testing


class StressRunRequest(BaseModel):
    portfolio: Optional[PortfolioInput] = None
    portfolio_id: Optional[str] = None
    scenarios: List[StressScenario]
    config: Optional[StressTestConfig] = None


@router.post("/stress/run", response_model=List[StressTestResult])
async def run_stress(request: Request, payload: StressRunRequest) -> List[StressTestResult]:
    start = time.perf_counter()
    try:
        engine = VaREngine()
        tester = StressTester(engine)
        portfolio = await _resolve_portfolio(payload.portfolio, payload.portfolio_id)
        results = tester.run(portfolio, payload.scenarios, config=payload.config)
        get_telemetry_facade().record_success(
            operation="run_stress_v2",
            query_time_ms=(time.perf_counter() - start) * 1000
        )
        return results
    except Exception as exc:
        get_telemetry_facade().record_error(operation="run_stress_v2", error=exc, query_time_ms=(time.perf_counter() - start) * 1000)
        raise HTTPException(status_code=500, detail=f"Failed to run stress test: {exc}")


# -------- Compliance


class ComplianceReportRequest(BaseModel):
    portfolio: Optional[PortfolioInput] = None
    portfolio_id: Optional[str] = None
    report: ComplianceReportConfig


@router.post("/compliance/report", response_model=ComplianceReport)
async def generate_compliance_report(request: Request, payload: ComplianceReportRequest) -> ComplianceReport:
    start = time.perf_counter()
    try:
        portfolio = await _resolve_portfolio(payload.portfolio, payload.portfolio_id)
        report = ComplianceReportGenerator().generate(portfolio, payload.report)
        get_telemetry_facade().record_success(
            operation="generate_compliance_report",
            query_time_ms=(time.perf_counter() - start) * 1000
        )
        return report
    except Exception as exc:
        get_telemetry_facade().record_error(operation="generate_compliance_report", error=exc, query_time_ms=(time.perf_counter() - start) * 1000)
        raise HTTPException(status_code=500, detail=f"Failed to generate compliance report: {exc}")


class CreateScheduleRequest(BaseModel):
    portfolio: Optional[PortfolioInput] = None
    portfolio_id: Optional[str] = None
    report: ComplianceReportConfig
    schedule_time_utc: str = Field("00:30", description="Daily UTC time HH:MM")
    retention_days: int = 30
    max_reports: Optional[int] = 100


class ScheduleResponse(BaseModel):
    schedule_id: str
    next_run: Optional[datetime]


@router.post("/compliance/schedules", response_model=ScheduleResponse)
async def create_compliance_schedule(request: Request, payload: CreateScheduleRequest) -> ScheduleResponse:
    svc = get_risk_compliance_service()
    sid = svc.create_schedule(
        portfolio=payload.portfolio,
        report_config=payload.report,
        portfolio_id=payload.portfolio_id,
        schedule_time_utc=payload.schedule_time_utc,
        retention_days=payload.retention_days,
        max_reports=payload.max_reports,
    )
    sched = (await svc.list_schedules())
    created = next((s for s in sched if s.schedule_id == sid), None)
    return ScheduleResponse(schedule_id=sid, next_run=created.next_run if created else None)


@router.get("/compliance/schedules", response_model=List[ComplianceSchedule])
async def list_compliance_schedules() -> List[ComplianceSchedule]:
    svc = get_risk_compliance_service()
    return await svc.list_schedules()


@router.delete("/compliance/schedules/{schedule_id}")
async def delete_compliance_schedule(schedule_id: str) -> Dict[str, Any]:
    ok = get_risk_compliance_service().delete_schedule(schedule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"deleted": True, "schedule_id": schedule_id}


@router.get("/compliance/reports/{portfolio_id}")
async def list_compliance_reports(portfolio_id: str, limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    svc = get_risk_compliance_service()
    items = await svc.list_reports(portfolio_id, limit=limit)
    return {"portfolio_id": portfolio_id, "reports": items}
