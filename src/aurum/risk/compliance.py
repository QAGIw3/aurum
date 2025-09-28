"""Regulatory compliance reporting (CFTC, FERC) and automated risk reports.

This module assembles risk analytics from var_engine and stress testing into
structured reports with audit metadata. It aims to provide opinionated defaults
that can be adapted to specific internal/external formats.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pydantic import BaseModel, Field

from .var_engine import (
    PortfolioInput,
    VaRConfig,
    VaRResult,
    VaREngine,
    RiskLimitsConfig,
)
from .stress_testing import StressScenario, StressTester, StressTestConfig, StressTestResult


class ComplianceReportConfig(BaseModel):
    firm_id: str
    reporting_period_start: date
    reporting_period_end: date
    contact_email: Optional[str] = None
    include_cftc: bool = True
    include_ferc: bool = True
    # Optional limits applied for alerting context in reports
    risk_limits: Optional[RiskLimitsConfig] = None


class CFTCReport(BaseModel):
    firm_id: str
    as_of: date
    portfolio_id: str
    net_positions: Dict[str, float]
    gross_notional: float
    var_95: float
    cvar_95: float
    top_concentrations: List[Dict[str, Any]]


class FERCReport(BaseModel):
    firm_id: str
    period_start: date
    period_end: date
    portfolio_id: str
    risk_overview: Dict[str, Any]
    stress_summary: List[Dict[str, Any]]
    breaches: List[Dict[str, Any]]


class ComplianceReport(BaseModel):
    generated_at: datetime
    firm_id: str
    portfolio_id: str
    cftc: Optional[CFTCReport] = None
    ferc: Optional[FERCReport] = None
    attachments: Dict[str, str] = Field(default_factory=dict, description="Inline CSV/JSON attachments")


class ComplianceReportGenerator:
    def __init__(self, engine: Optional[VaREngine] = None) -> None:
        self._engine = engine or VaREngine()
        self._stress = StressTester(self._engine)

    def _aggregate_positions(self, portfolio: PortfolioInput) -> Dict[str, float]:
        # Aggregate by asset_id for simplicity (can map to commodity group/product code externally)
        agg: Dict[str, float] = {}
        for p in portfolio.positions:
            sign = 1.0 if (p.position_type or "long").lower() == "long" else -1.0
            agg[p.asset_id] = agg.get(p.asset_id, 0.0) + sign * float(p.notional_value)
        return agg

    def _gross_notional(self, portfolio: PortfolioInput) -> float:
        return float(sum(abs(p.notional_value) for p in portfolio.positions))

    def generate_cftc(
        self,
        portfolio: PortfolioInput,
        var_result: VaRResult,
        firm_id: str,
        as_of: Optional[date] = None,
    ) -> CFTCReport:
        as_of = as_of or datetime.utcnow().date()
        net = self._aggregate_positions(portfolio)
        # Top concentrations by notional share
        total = self._gross_notional(portfolio) or 1.0
        concentrations = sorted(
            (
                {
                    "asset_id": p.asset_id,
                    "notional": abs(p.notional_value),
                    "weight": abs(p.notional_value) / total,
                    "position_type": p.position_type,
                }
                for p in portfolio.positions
            ),
            key=lambda x: x["notional"],
            reverse=True,
        )[:5]
        return CFTCReport(
            firm_id=firm_id,
            as_of=as_of,
            portfolio_id=portfolio.portfolio_id,
            net_positions=net,
            gross_notional=self._gross_notional(portfolio),
            var_95=float(var_result.var),
            cvar_95=float(var_result.cvar),
            top_concentrations=concentrations,
        )

    def generate_ferc(
        self,
        portfolio: PortfolioInput,
        var_result: VaRResult,
        stress_results: Iterable[StressTestResult],
        firm_id: str,
        period_start: date,
        period_end: date,
    ) -> FERCReport:
        overview = {
            "var": var_result.var,
            "cvar": var_result.cvar,
            "volatility": var_result.volatility,
            "max_drawdown": var_result.max_drawdown,
            "counterparty_top": [c.dict() for c in (var_result.counterparty_risk.top_counterparties if var_result.counterparty_risk else [])],
        }
        stress_summary = [
            {
                "scenario_id": r.scenario_id,
                "name": r.name,
                "pnl_impact": r.pnl_impact,
                "var_after": r.var_after,
                "cvar_after": r.cvar_after,
            }
            for r in stress_results
        ]
        breaches = [b.dict() for b in (var_result.breaches or [])]
        return FERCReport(
            firm_id=firm_id,
            period_start=period_start,
            period_end=period_end,
            portfolio_id=portfolio.portfolio_id,
            risk_overview=overview,
            stress_summary=stress_summary,
            breaches=breaches,
        )

    def _attachment_csv_positions(self, portfolio: PortfolioInput) -> str:
        lines = ["asset_id,position_type,notional,currency"]
        for p in portfolio.positions:
            lines.append(f"{p.asset_id},{p.position_type},{float(p.notional_value):.6f},{p.currency}")
        return "\n".join(lines)

    def generate(
        self,
        portfolio: PortfolioInput,
        cfg: ComplianceReportConfig,
        *,
        factor_returns: Optional[Dict[str, Any]] = None,
        scenarios: Optional[Iterable[StressScenario]] = None,
    ) -> ComplianceReport:
        # Baseline VaR
        var = self._engine.calculate_var(portfolio, VaRConfig(), factor_returns=factor_returns, limits=cfg.risk_limits)
        # Stress
        scen_list = list(scenarios or [])
        if not scen_list:
            from .stress_testing import default_scenarios as _default

            scen_list = _default()
        stress = self._stress.run(portfolio, scen_list, factor_returns=factor_returns, base_var=var)

        cftc: Optional[CFTCReport] = None
        ferc: Optional[FERCReport] = None
        if cfg.include_cftc:
            cftc = self.generate_cftc(portfolio, var, cfg.firm_id)
        if cfg.include_ferc:
            ferc = self.generate_ferc(portfolio, var, stress, cfg.firm_id, cfg.reporting_period_start, cfg.reporting_period_end)

        attachments = {
            "positions.csv": self._attachment_csv_positions(portfolio),
        }
        return ComplianceReport(
            generated_at=datetime.utcnow(),
            firm_id=cfg.firm_id,
            portfolio_id=portfolio.portfolio_id,
            cftc=cftc,
            ferc=ferc,
            attachments=attachments,
        )

