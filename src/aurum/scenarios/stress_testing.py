"""Policy shocks and outage scenarios with templated stress-tests and CLI.

This module provides:
- Templated stress test scenarios for policy shocks and outages
- Scenario definition and management system
- Automated scenario execution and impact analysis
- CLI interface for running stress tests
- Integration with portfolio rollup and risk analysis
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured


class StressTestConfig(BaseModel):
    """Configuration for stress test scenarios."""

    scenario_type: str = Field(..., description="Type of stress test scenario")
    severity: str = Field(default="medium", description="Severity level: low, medium, high, extreme")
    duration_hours: int = Field(default=24, description="Duration of the stress event in hours")
    affected_regions: List[str] = Field(default_factory=list, description="Geographic regions affected")
    probability: float = Field(default=0.01, ge=0.0, le=1.0, description="Probability of occurrence")
    impact_multiplier: float = Field(default=1.0, ge=0.0, description="Impact multiplier for the scenario")
    recovery_time_hours: int = Field(default=48, description="Time to recover from the event")


@dataclass
class StressTestTemplate:
    """Template for stress test scenarios."""

    template_id: str
    name: str
    description: str
    category: str  # 'policy', 'outage', 'weather', 'market', 'operational'
    parameters: Dict[str, Any]
    default_config: StressTestConfig
    created_by: str
    created_at: datetime
    version: str = "1.0"


@dataclass
class ScenarioImpact:
    """Impact assessment for a stress test scenario."""

    scenario_id: str
    affected_curves: List[str]
    price_impact: Dict[str, float]  # curve_key -> price_multiplier
    volume_impact: Dict[str, float]  # curve_key -> volume_multiplier
    confidence_level: float
    risk_metrics: Dict[str, Any]
    affected_positions: List[str]
    portfolio_impact: float
    recovery_timeline: List[Tuple[datetime, float]]  # timestamp -> recovery_percentage


class BaseStressTestScenario(ABC):
    """Abstract base class for stress test scenarios."""

    def __init__(self, template: StressTestTemplate, config: StressTestConfig):
        self.template = template
        self.config = config
        self.scenario_id = f"{template.template_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    @abstractmethod
    async def calculate_impact(
        self,
        as_of_date: datetime,
        market_data: Dict[str, Any]
    ) -> ScenarioImpact:
        """Calculate the impact of this stress scenario."""
        pass

    @abstractmethod
    def get_affected_curves(self) -> List[str]:
        """Get list of curves affected by this scenario."""
        pass

    def validate_config(self) -> bool:
        """Validate the scenario configuration."""
        return True


class PolicyShockScenario(BaseStressTestScenario):
    """Policy shock scenarios (e.g., carbon tax, renewable mandates)."""

    async def calculate_impact(
        self,
        as_of_date: datetime,
        market_data: Dict[str, Any]
    ) -> ScenarioImpact:

        affected_curves = self.get_affected_curves()

        # Calculate price impacts based on policy type
        price_impact = {}
        volume_impact = {}

        if self.template.template_id == "carbon_tax":
            # Carbon tax increases fossil fuel costs
            for curve_key in affected_curves:
                if "coal" in curve_key.lower() or "gas" in curve_key.lower():
                    price_impact[curve_key] = 1.15  # 15% price increase
                    volume_impact[curve_key] = 0.95  # 5% volume decrease
                elif "renewable" in curve_key.lower() or "wind" in curve_key.lower() or "solar" in curve_key.lower():
                    price_impact[curve_key] = 0.98  # 2% price decrease
                    volume_impact[curve_key] = 1.05  # 5% volume increase

        elif self.template.template_id == "renewable_mandate":
            # Renewable energy mandate
            for curve_key in affected_curves:
                if "renewable" in curve_key.lower():
                    price_impact[curve_key] = 0.95  # 5% price decrease
                    volume_impact[curve_key] = 1.10  # 10% volume increase

        elif self.template.template_id == "capacity_market_reform":
            # Capacity market pricing changes
            for curve_key in affected_curves:
                if "capacity" in curve_key.lower():
                    price_impact[curve_key] = 1.25  # 25% price increase

        # Apply severity multiplier
        severity_multipliers = {
            "low": 0.5,
            "medium": 1.0,
            "high": 1.5,
            "extreme": 2.0
        }

        severity_multiplier = severity_multipliers.get(self.config.severity, 1.0)

        for curve_key in price_impact:
            price_impact[curve_key] = 1.0 + (price_impact[curve_key] - 1.0) * severity_multiplier
            volume_impact[curve_key] = 1.0 + (volume_impact[curve_key] - 1.0) * severity_multiplier

        # Calculate risk metrics
        risk_metrics = {
            "var_95": abs(self.config.impact_multiplier * severity_multiplier * 100000),
            "var_99": abs(self.config.impact_multiplier * severity_multiplier * 150000),
            "max_drawdown": abs(self.config.impact_multiplier * severity_multiplier * 200000),
        }

        # Generate recovery timeline
        recovery_timeline = []
        for hour in range(self.config.recovery_time_hours + 1):
            timestamp = as_of_date + timedelta(hours=hour)
            # Exponential recovery curve
            recovery_pct = 1.0 - 0.5 * np.exp(-hour / (self.config.recovery_time_hours / 3))
            recovery_timeline.append((timestamp, recovery_pct))

        return ScenarioImpact(
            scenario_id=self.scenario_id,
            affected_curves=affected_curves,
            price_impact=price_impact,
            volume_impact=volume_impact,
            confidence_level=0.8,
            risk_metrics=risk_metrics,
            affected_positions=[],  # Would be populated from portfolio analysis
            portfolio_impact=self.config.impact_multiplier * severity_multiplier,
            recovery_timeline=recovery_timeline,
        )

    def get_affected_curves(self) -> List[str]:
        """Get curves affected by policy shocks."""
        if self.template.template_id == "carbon_tax":
            return ["coal_generation", "gas_generation", "wind_generation", "solar_generation"]
        elif self.template.template_id == "renewable_mandate":
            return ["wind_generation", "solar_generation", "hydro_generation"]
        elif self.template.template_id == "capacity_market_reform":
            return ["capacity_prices"]
        else:
            return []


class OutageScenario(BaseStressTestScenario):
    """Equipment outage scenarios."""

    async def calculate_impact(
        self,
        as_of_date: datetime,
        market_data: Dict[str, Any]
    ) -> ScenarioImpact:

        affected_curves = self.get_affected_curves()

        # Calculate outage impacts
        price_impact = {}
        volume_impact = {}

        if self.template.template_id == "generator_outage":
            # Major generator outage
            outage_capacity = self.config.parameters.get("outage_capacity_mw", 1000)

            for curve_key in affected_curves:
                if "generation" in curve_key.lower():
                    # Reduce generation capacity
                    volume_impact[curve_key] = 1.0 - (outage_capacity / 50000)  # Assume 50GW total capacity

                    # Increase prices due to supply shortage
                    price_impact[curve_key] = 1.0 + (outage_capacity / 50000) * 0.5

        elif self.template.template_id == "transmission_outage":
            # Transmission line outage
            for curve_key in affected_curves:
                if "lmp" in curve_key.lower():
                    # Congestion pricing impact
                    price_impact[curve_key] = 1.0 + 0.3  # 30% price increase due to congestion
                    volume_impact[curve_key] = 0.95  # Reduced flow

        elif self.template.template_id == "nuclear_plant_outage":
            # Nuclear plant specific outage
            for curve_key in affected_curves:
                if "nuclear" in curve_key.lower():
                    volume_impact[curve_key] = 0.0  # Complete outage
                    price_impact[curve_key] = 1.0  # Price impact through other generators

        # Apply severity and duration factors
        severity_multipliers = {
            "low": 0.7,
            "medium": 1.0,
            "high": 1.3,
            "extreme": 1.5
        }

        severity_multiplier = severity_multipliers.get(self.config.severity, 1.0)

        for curve_key in price_impact:
            price_impact[curve_key] = price_impact[curve_key] ** severity_multiplier
            if curve_key in volume_impact:
                volume_impact[curve_key] = volume_impact[curve_key] ** severity_multiplier

        # Risk metrics
        risk_metrics = {
            "var_95": abs(self.config.impact_multiplier * severity_multiplier * 50000),
            "var_99": abs(self.config.impact_multiplier * severity_multiplier * 75000),
            "max_drawdown": abs(self.config.impact_multiplier * severity_multiplier * 100000),
        }

        # Recovery timeline (outages typically have immediate full recovery)
        recovery_timeline = [
            (as_of_date, 0.0),  # Immediate impact
            (as_of_date + timedelta(hours=self.config.duration_hours), 1.0),  # Recovery
        ]

        return ScenarioImpact(
            scenario_id=self.scenario_id,
            affected_curves=affected_curves,
            price_impact=price_impact,
            volume_impact=volume_impact,
            confidence_level=0.9,
            risk_metrics=risk_metrics,
            affected_positions=[],
            portfolio_impact=self.config.impact_multiplier * severity_multiplier,
            recovery_timeline=recovery_timeline,
        )

    def get_affected_curves(self) -> List[str]:
        """Get curves affected by outages."""
        if self.template.template_id == "generator_outage":
            return ["total_generation", "lmp_prices"]
        elif self.template.template_id == "transmission_outage":
            return ["lmp_prices", "congestion_prices"]
        elif self.template.template_id == "nuclear_plant_outage":
            return ["nuclear_generation", "total_generation", "lmp_prices"]
        else:
            return []


class WeatherScenario(BaseStressTestScenario):
    """Weather event scenarios (heat waves, cold snaps, storms)."""

    async def calculate_impact(
        self,
        as_of_date: datetime,
        market_data: Dict[str, Any]
    ) -> ScenarioImpact:

        affected_curves = self.get_affected_curves()

        price_impact = {}
        volume_impact = {}

        if self.template.template_id == "heat_wave":
            # Heat wave increases demand and reduces efficiency
            for curve_key in affected_curves:
                if "load" in curve_key.lower() or "demand" in curve_key.lower():
                    price_impact[curve_key] = 1.0 + self.config.parameters.get("demand_increase", 0.15)
                    volume_impact[curve_key] = 1.0 + self.config.parameters.get("demand_increase", 0.15)
                elif "thermal" in curve_key.lower() or "gas" in curve_key.lower():
                    # Reduced efficiency
                    price_impact[curve_key] = 1.0 + 0.05
                    volume_impact[curve_key] = 0.95

        elif self.template.template_id == "cold_snap":
            # Cold snap increases heating demand
            for curve_key in affected_curves:
                if "load" in curve_key.lower():
                    price_impact[curve_key] = 1.0 + self.config.parameters.get("demand_increase", 0.20)
                    volume_impact[curve_key] = 1.0 + self.config.parameters.get("demand_increase", 0.20)
                elif "renewable" in curve_key.lower():
                    # Reduced solar/wind output
                    volume_impact[curve_key] = 0.90

        elif self.template.template_id == "storm":
            # Storm affects transmission and renewable generation
            for curve_key in affected_curves:
                if "renewable" in curve_key.lower():
                    volume_impact[curve_key] = 0.70  # Significant reduction
                elif "transmission" in curve_key.lower():
                    price_impact[curve_key] = 1.0 + 0.25

        # Apply duration and regional factors
        duration_factor = min(1.0, self.config.duration_hours / 24)  # Longer events have less marginal impact

        for curve_key in price_impact:
            price_impact[curve_key] = 1.0 + (price_impact[curve_key] - 1.0) * duration_factor

        for curve_key in volume_impact:
            volume_impact[curve_key] = 1.0 + (volume_impact[curve_key] - 1.0) * duration_factor

        # Risk metrics
        risk_metrics = {
            "var_95": abs(self.config.impact_multiplier * 75000),
            "var_99": abs(self.config.impact_multiplier * 100000),
            "max_drawdown": abs(self.config.impact_multiplier * 150000),
        }

        # Recovery timeline (weather events have gradual recovery)
        recovery_timeline = []
        for hour in range(self.config.recovery_time_hours + 1):
            timestamp = as_of_date + timedelta(hours=hour)
            # Sigmoid recovery curve
            recovery_pct = 1.0 / (1.0 + np.exp(-2 * (hour - self.config.duration_hours) / (self.config.duration_hours / 3)))
            recovery_timeline.append((timestamp, recovery_pct))

        return ScenarioImpact(
            scenario_id=self.scenario_id,
            affected_curves=affected_curves,
            price_impact=price_impact,
            volume_impact=volume_impact,
            confidence_level=0.7,  # Weather scenarios have lower predictability
            risk_metrics=risk_metrics,
            affected_positions=[],
            portfolio_impact=self.config.impact_multiplier,
            recovery_timeline=recovery_timeline,
        )

    def get_affected_curves(self) -> List[str]:
        """Get curves affected by weather events."""
        if self.template.template_id == "heat_wave":
            return ["load_demand", "thermal_generation", "gas_generation"]
        elif self.template.template_id == "cold_snap":
            return ["load_demand", "renewable_generation"]
        elif self.template.template_id == "storm":
            return ["renewable_generation", "transmission_prices"]
        else:
            return []


class StressTestEngine:
    """Main engine for running stress test scenarios."""

    def __init__(self):
        self.templates: Dict[str, StressTestTemplate] = {}
        self._initialize_templates()

    def _initialize_templates(self) -> None:
        """Initialize default stress test templates."""

        # Policy shock templates
        self.templates["carbon_tax"] = StressTestTemplate(
            template_id="carbon_tax",
            name="Carbon Tax Implementation",
            description="Implementation of carbon pricing mechanism",
            category="policy",
            parameters={"tax_rate_per_ton": 50.0},
            default_config=StressTestConfig(
                scenario_type="policy_shock",
                severity="medium",
                duration_hours=8760,  # 1 year
                impact_multiplier=1.0,
                probability=0.05,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        self.templates["renewable_mandate"] = StressTestTemplate(
            template_id="renewable_mandate",
            name="Renewable Portfolio Standard",
            description="Increased renewable energy requirements",
            category="policy",
            parameters={"renewable_target_percent": 30.0},
            default_config=StressTestConfig(
                scenario_type="policy_shock",
                severity="high",
                duration_hours=8760,
                impact_multiplier=1.2,
                probability=0.1,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        # Outage templates
        self.templates["generator_outage"] = StressTestTemplate(
            template_id="generator_outage",
            name="Major Generator Outage",
            description="Outage of a large generating unit",
            category="outage",
            parameters={"outage_capacity_mw": 1000},
            default_config=StressTestConfig(
                scenario_type="outage",
                severity="high",
                duration_hours=168,  # 1 week
                impact_multiplier=2.0,
                probability=0.02,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        self.templates["nuclear_plant_outage"] = StressTestTemplate(
            template_id="nuclear_plant_outage",
            name="Nuclear Plant Outage",
            description="Outage of nuclear generating facility",
            category="outage",
            parameters={"outage_capacity_mw": 1200},
            default_config=StressTestConfig(
                scenario_type="outage",
                severity="extreme",
                duration_hours=720,  # 1 month
                impact_multiplier=3.0,
                probability=0.01,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        # Weather templates
        self.templates["heat_wave"] = StressTestTemplate(
            template_id="heat_wave",
            name="Heat Wave Event",
            description="Extended period of high temperatures",
            category="weather",
            parameters={"temperature_increase": 10, "demand_increase": 0.15},
            default_config=StressTestConfig(
                scenario_type="weather",
                severity="medium",
                duration_hours=72,
                impact_multiplier=1.5,
                probability=0.1,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        self.templates["cold_snap"] = StressTestTemplate(
            template_id="cold_snap",
            name="Cold Snap Event",
            description="Extended period of low temperatures",
            category="weather",
            parameters={"temperature_decrease": 15, "demand_increase": 0.20},
            default_config=StressTestConfig(
                scenario_type="weather",
                severity="high",
                duration_hours=48,
                impact_multiplier=1.8,
                probability=0.05,
            ),
            created_by="system",
            created_at=datetime.now(),
        )

        log_structured("info", "stress_test_templates_initialized", num_templates=len(self.templates))

    async def run_stress_test(
        self,
        template_id: str,
        config: Optional[StressTestConfig] = None,
        as_of_date: Optional[datetime] = None,
        market_data: Optional[Dict[str, Any]] = None,
    ) -> ScenarioImpact:
        """Run a stress test scenario."""

        if template_id not in self.templates:
            raise ValueError(f"Unknown template: {template_id}")

        template = self.templates[template_id]

        if config is None:
            config = template.default_config

        if as_of_date is None:
            as_of_date = datetime.now()

        # Create scenario instance
        if template.category == "policy":
            scenario = PolicyShockScenario(template, config)
        elif template.category == "outage":
            scenario = OutageScenario(template, config)
        elif template.category == "weather":
            scenario = WeatherScenario(template, config)
        else:
            raise ValueError(f"Unsupported scenario category: {template.category}")

        # Calculate impact
        impact = await scenario.calculate_impact(as_of_date, market_data or {})

        log_structured(
            "info",
            "stress_test_completed",
            template_id=template_id,
            scenario_id=impact.scenario_id,
            portfolio_impact=impact.portfolio_impact,
            affected_curves=len(impact.affected_curves),
        )

        return impact

    async def run_multiple_scenarios(
        self,
        template_ids: List[str],
        config_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, ScenarioImpact]:
        """Run multiple stress test scenarios."""

        results = {}

        for template_id in template_ids:
            try:
                config = None
                if config_overrides and template_id in config_overrides:
                    config = StressTestConfig(**config_overrides[template_id])

                impact = await self.run_stress_test(template_id, config)
                results[template_id] = impact

            except Exception as exc:
                log_structured(
                    "error",
                    "stress_test_failed",
                    template_id=template_id,
                    error=str(exc),
                )
                results[template_id] = None

        return results

    def get_template(self, template_id: str) -> Optional[StressTestTemplate]:
        """Get a stress test template."""
        return self.templates.get(template_id)

    def list_templates(self, category: Optional[str] = None) -> List[StressTestTemplate]:
        """List available stress test templates."""
        templates = list(self.templates.values())

        if category:
            templates = [t for t in templates if t.category == category]

        return templates

    async def estimate_scenario_probability(
        self,
        template_id: str,
        historical_data: Dict[str, Any]
    ) -> float:
        """Estimate the probability of a scenario based on historical data."""

        template = self.templates[template_id]
        base_probability = template.default_config.probability

        # Adjust probability based on historical frequency
        # This would integrate with historical event data
        # For now, return base probability
        return base_probability

    async def generate_stress_test_report(
        self,
        scenario_impacts: Dict[str, ScenarioImpact]
    ) -> Dict[str, Any]:
        """Generate comprehensive stress test report."""

        total_portfolio_impact = sum(impact.portfolio_impact for impact in scenario_impacts.values() if impact)

        # Aggregate risk metrics
        aggregated_risk = {
            "var_95": max(impact.risk_metrics.get("var_95", 0) for impact in scenario_impacts.values() if impact),
            "var_99": max(impact.risk_metrics.get("var_99", 0) for impact in scenario_impacts.values() if impact),
            "max_drawdown": max(impact.risk_metrics.get("max_drawdown", 0) for impact in scenario_impacts.values() if impact),
        }

        # Find most affected curves
        all_affected_curves = {}
        for impact in scenario_impacts.values():
            if impact:
                for curve_key in impact.affected_curves:
                    if curve_key not in all_affected_curves:
                        all_affected_curves[curve_key] = 0
                    all_affected_curves[curve_key] += 1

        most_affected_curves = sorted(
            all_affected_curves.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        return {
            "report_date": datetime.now(),
            "scenarios_analyzed": len(scenario_impacts),
            "total_portfolio_impact": total_portfolio_impact,
            "aggregated_risk_metrics": aggregated_risk,
            "most_affected_curves": most_affected_curves,
            "scenario_details": {
                template_id: {
                    "impact": impact.portfolio_impact,
                    "affected_curves": len(impact.affected_curves),
                    "confidence_level": impact.confidence_level,
                }
                for template_id, impact in scenario_impacts.items()
                if impact
            }
        }


class StressTestCLI:
    """CLI interface for running stress tests."""

    def __init__(self, engine: StressTestEngine):
        self.engine = engine

    async def run_interactive_session(self) -> None:
        """Run interactive CLI session for stress testing."""

        print("=== Aurum Stress Test CLI ===")
        print("Available commands:")
        print("  list     - List available templates")
        print("  run      - Run a stress test scenario")
        print("  batch    - Run multiple scenarios")
        print("  report   - Generate stress test report")
        print("  help     - Show this help")
        print("  quit     - Exit")

        while True:
            try:
                command = input("\nCommand: ").strip().lower()

                if command == "quit" or command == "exit":
                    break
                elif command == "list":
                    await self._list_templates()
                elif command == "run":
                    await self._run_single_test()
                elif command == "batch":
                    await self._run_batch_tests()
                elif command == "report":
                    await self._generate_report()
                elif command == "help":
                    self._show_help()
                else:
                    print(f"Unknown command: {command}")

            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as exc:
                print(f"Error: {exc}")

    async def _list_templates(self) -> None:
        """List available templates."""
        templates = self.engine.list_templates()

        print(f"\nAvailable Templates ({len(templates)}):")
        print("-" * 50)

        for template in templates:
            print(f"ID: {template.template_id}")
            print(f"Name: {template.name}")
            print(f"Category: {template.category}")
            print(f"Description: {template.description}")
            print(f"Probability: {template.default_config.probability}")
            print()

    async def _run_single_test(self) -> None:
        """Run a single stress test."""
        template_id = input("Template ID: ").strip()

        template = self.engine.get_template(template_id)
        if not template:
            print(f"Template not found: {template_id}")
            return

        # Get configuration overrides
        severity = input(f"Severity (default: {template.default_config.severity}): ").strip()
        duration = input(f"Duration hours (default: {template.default_config.duration_hours}): ").strip()

        config = template.default_config.copy()
        if severity:
            config.severity = severity
        if duration:
            config.duration_hours = int(duration)

        print(f"Running stress test: {template.name}")
        print("Please wait...")

        impact = await self.engine.run_stress_test(template_id, config)

        if impact:
            print("
Results:")
            print(f"Scenario ID: {impact.scenario_id}")
            print(f"Portfolio Impact: {impact.portfolio_impact}")
            print(f"Affected Curves: {len(impact.affected_curves)}")
            print(f"Confidence Level: {impact.confidence_level}")
            print(f"VaR 95%: {impact.risk_metrics.get('var_95', 0)}")
        else:
            print("Stress test failed")

    async def _run_batch_tests(self) -> None:
        """Run multiple stress tests."""
        print("Available template categories:")
        print("  policy  - Policy shock scenarios")
        print("  outage  - Equipment outage scenarios")
        print("  weather - Weather event scenarios")
        print("  all     - All scenarios")

        category = input("Category (or 'all'): ").strip()

        templates = self.engine.list_templates()
        if category and category != "all":
            templates = [t for t in templates if t.category == category]

        if not templates:
            print("No templates found for the specified category")
            return

        print(f"\nRunning {len(templates)} stress tests...")
        print("Please wait...")

        results = await self.engine.run_multiple_scenarios([t.template_id for t in templates])

        successful = sum(1 for impact in results.values() if impact)
        print(f"\nCompleted: {successful}/{len(results)} scenarios")

        # Generate report
        report = await self.engine.generate_stress_test_report(results)
        print("
Summary:")
        print(f"Total Portfolio Impact: {report['total_portfolio_impact']}")
        print(f"VaR 95%: {report['aggregated_risk_metrics']['var_95']}")
        print(f"Most Affected Curve: {report['most_affected_curves'][0][0] if report['most_affected_curves'] else 'None'}")

    async def _generate_report(self) -> None:
        """Generate stress test report."""
        print("Generating comprehensive stress test report...")
        print("Please wait...")

        # Run all templates
        templates = self.engine.list_templates()
        results = await self.engine.run_multiple_scenarios([t.template_id for t in templates])

        report = await self.engine.generate_stress_test_report(results)

        print("
=== STRESS TEST REPORT ===")
        print(f"Report Date: {report['report_date']}")
        print(f"Scenarios Analyzed: {report['scenarios_analyzed']}")
        print(f"Total Portfolio Impact: {report['total_portfolio_impact']}")
        print("
Risk Metrics:")
        print(f"  VaR 95%: {report['aggregated_risk_metrics']['var_95']}")
        print(f"  VaR 99%: {report['aggregated_risk_metrics']['var_99']}")
        print(f"  Max Drawdown: {report['aggregated_risk_metrics']['max_drawdown']}")
        print("
Most Affected Curves:")
        for curve, count in report['most_affected_curves']:
            print(f"  {curve}: {count} scenarios")

    def _show_help(self) -> None:
        """Show help information."""
        print("\n=== Aurum Stress Test CLI Help ===")
        print("Commands:")
        print("  list     - List available stress test templates")
        print("  run      - Run a single stress test scenario")
        print("  batch    - Run multiple scenarios by category")
        print("  report   - Generate comprehensive stress test report")
        print("  help     - Show this help message")
        print("  quit     - Exit the CLI")
        print("\nTemplate Categories:")
        print("  policy   - Policy shocks (carbon tax, renewable mandates)")
        print("  outage   - Equipment outages (generator, transmission)")
        print("  weather  - Weather events (heat waves, cold snaps)")
        print("\nSeverity Levels:")
        print("  low      - Minimal impact")
        print("  medium   - Moderate impact")
        print("  high     - Significant impact")
        print("  extreme  - Maximum impact")


# Global stress test engine instance
_stress_test_engine: Optional[StressTestEngine] = None


def get_stress_test_engine() -> StressTestEngine:
    """Get the global stress test engine instance."""
    global _stress_test_engine
    if _stress_test_engine is None:
        _stress_test_engine = StressTestEngine()
    return _stress_test_engine


async def run_stress_test_cli() -> None:
    """Run the stress test CLI."""
    engine = get_stress_test_engine()
    cli = StressTestCLI(engine)
    await cli.run_interactive_session()
