"""Risk management package: VaR engine, stress testing, and compliance.

This package provides reusable building blocks for risk analytics and reporting:
- var_engine: VaR/CVaR calculations (historical, parametric, Monte Carlo),
  portfolio aggregation, counterparty risk, risk limit checks, and dashboard helpers.
- stress_testing: scenario definitions and execution with portfolio impact analysis.
- compliance: report generators for CFTC/FERC style summaries and audit metadata.

The modules are designed to be framework-agnostic and can be used from API
services, batch jobs, or notebooks. All external provider access is optional and
guarded to keep these components functional in lightweight environments.
"""

from .var_engine import (
    VaRMethod,
    VaRConfig,
    PositionInput,
    PortfolioInput,
    VaRResult,
    RiskLimitsConfig,
    RiskLimitBreach,
    CounterpartyRiskResult,
    RiskDashboard,
    VaREngine,
    ExternalRiskDataClient,
)

from .stress_testing import (
    StressScenario,
    StressTestConfig,
    StressTestResult,
    StressTester,
    default_scenarios,
)

from .compliance import (
    ComplianceReportConfig,
    ComplianceReport,
    ComplianceReportGenerator,
)

__all__ = [
    # VaR engine
    "VaRMethod",
    "VaRConfig",
    "PositionInput",
    "PortfolioInput",
    "VaRResult",
    "RiskLimitsConfig",
    "RiskLimitBreach",
    "CounterpartyRiskResult",
    "RiskDashboard",
    "VaREngine",
    "ExternalRiskDataClient",
    # Stress testing
    "StressScenario",
    "StressTestConfig",
    "StressTestResult",
    "StressTester",
    "default_scenarios",
    # Compliance
    "ComplianceReportConfig",
    "ComplianceReport",
    "ComplianceReportGenerator",
]
