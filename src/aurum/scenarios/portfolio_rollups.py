"""Portfolio rollups and exposure views by node, hub, zone, BA, ISO.

This module provides:
- Hierarchical rollup of portfolio exposures across geographic and organizational boundaries
- Multi-dimensional aggregation views (node → hub → zone → BA → ISO)
- Real-time exposure calculations with risk metrics
- Integration with curve data for pricing and valuation
- Scenario-based exposure analysis
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import log_structured


class RollupConfig(BaseModel):
    """Configuration for portfolio rollup calculations."""

    hierarchy_levels: List[str] = Field(default_factory=lambda: ["node", "hub", "zone", "ba", "iso"], description="Hierarchy levels for rollup")
    include_risk_metrics: bool = Field(default=True, description="Whether to calculate risk metrics")
    confidence_level: float = Field(default=0.95, ge=0.0, le=1.0, description="Confidence level for risk calculations")
    include_scenario_analysis: bool = Field(default=True, description="Whether to include scenario-based exposures")
    real_time_updates: bool = Field(default=True, description="Enable real-time exposure updates")
    cache_ttl_minutes: int = Field(default=15, description="Cache time-to-live for rollup results")


@dataclass
class Position:
    """A portfolio position with exposure details."""

    position_id: str
    instrument_type: str  # 'curve', 'contract', 'physical'
    curve_key: Optional[str] = None
    contract_id: Optional[str] = None
    quantity: float = 0.0
    price: Optional[float] = None
    location: str = ""  # Node, hub, or zone identifier
    counterparty: Optional[str] = None
    maturity_date: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Exposure:
    """Calculated exposure for a position or aggregation."""

    entity_id: str  # Node, hub, zone, BA, or ISO identifier
    entity_type: str  # 'node', 'hub', 'zone', 'ba', 'iso'
    exposure_amount: float = 0.0
    exposure_currency: str = "USD"
    delta: float = 0.0  # Price sensitivity
    gamma: float = 0.0  # Second-order price sensitivity
    vega: float = 0.0  # Volatility sensitivity
    theta: float = 0.0  # Time decay
    var_95: float = 0.0  # 95% Value at Risk
    var_99: float = 0.0  # 99% Value at Risk
    max_drawdown: float = 0.0
    positions: List[Position] = field(default_factory=list)
    as_of_date: datetime = field(default_factory=datetime.now)


@dataclass
class PortfolioRollup:
    """Complete portfolio rollup with hierarchical exposures."""

    as_of_date: datetime
    total_exposure: Exposure
    hierarchy_exposures: Dict[str, Exposure]  # entity_id -> Exposure
    risk_summary: Dict[str, Any] = field(default_factory=dict)
    scenario_exposures: Dict[str, Exposure] = field(default_factory=dict)  # scenario_id -> Exposure


class LocationHierarchy:
    """Manages the hierarchical relationship between locations."""

    def __init__(self):
        self.hierarchy_map: Dict[str, Dict[str, str]] = {
            # ISO-NE hierarchy
            "isone": {
                "nodes": {},  # node_id -> zone
                "zones": {},  # zone -> ba
                "bas": {},    # ba -> iso
            },
            # PJM hierarchy
            "pjm": {
                "nodes": {},
                "zones": {},
                "bas": {},
            },
            # CAISO hierarchy
            "caiso": {
                "nodes": {},
                "zones": {},
                "bas": {},
            },
        }

    async def initialize_from_data_warehouse(self) -> None:
        """Initialize hierarchy from data warehouse tables."""
        # This would query the actual ISO node, zone, and BA tables
        # For now, populate with sample data
        self.hierarchy_map["isone"]["nodes"] = {
            "4000": "ME",      # Maine
            "4001": "NH",      # New Hampshire
            "4002": "VT",      # Vermont
            "4003": "CT",      # Connecticut
            "4004": "RI",      # Rhode Island
            "4005": "SEMA",    # Southeast Massachusetts
            "4006": "WCMA",    # Western Central Massachusetts
            "4007": "NEMA",    # Northeast Massachusetts
            "4008": "NE",      # New England
        }

        self.hierarchy_map["isone"]["zones"] = {
            "ME": "NEPOOL",    # Maine -> NEPOOL BA
            "NH": "NEPOOL",    # New Hampshire -> NEPOOL BA
            "VT": "NEPOOL",    # Vermont -> NEPOOL BA
            "CT": "NEPOOL",    # Connecticut -> NEPOOL BA
            "RI": "NEPOOL",    # Rhode Island -> NEPOOL BA
            "SEMA": "NEPOOL",  # Southeast Massachusetts -> NEPOOL BA
            "WCMA": "NEPOOL",  # Western Central Massachusetts -> NEPOOL BA
            "NEMA": "NEPOOL",  # Northeast Massachusetts -> NEPOOL BA
        }

        self.hierarchy_map["isone"]["bas"] = {
            "NEPOOL": "ISONE",  # NEPOOL BA -> ISO-NE
        }

        log_structured("info", "location_hierarchy_initialized", num_isos=len(self.hierarchy_map))

    def get_parent(self, iso: str, entity_type: str, entity_id: str) -> Optional[str]:
        """Get the parent entity in the hierarchy."""
        if iso not in self.hierarchy_map:
            return None

        iso_hierarchy = self.hierarchy_map[iso]

        if entity_type == "node":
            return iso_hierarchy["nodes"].get(entity_id)
        elif entity_type == "zone":
            return iso_hierarchy["zones"].get(entity_id)
        elif entity_type == "ba":
            return iso_hierarchy["bas"].get(entity_id)

        return None

    def get_hierarchy_path(self, iso: str, start_entity_type: str, start_entity_id: str) -> List[Tuple[str, str]]:
        """Get the full hierarchy path from a starting entity."""
        path = []

        current_type = start_entity_type
        current_id = start_entity_id

        while current_id is not None:
            path.append((current_type, current_id))
            current_id = self.get_parent(iso, current_type, current_id)

            # Move up the hierarchy
            if current_type == "node":
                current_type = "zone"
            elif current_type == "zone":
                current_type = "ba"
            elif current_type == "ba":
                current_type = "iso"
            else:
                break

        return path


class PortfolioRollupEngine:
    """Engine for calculating portfolio rollups and exposures."""

    def __init__(self, config: RollupConfig):
        self.config = config
        self.location_hierarchy = LocationHierarchy()
        self._cache: Dict[str, PortfolioRollup] = {}

    async def initialize(self) -> None:
        """Initialize the rollup engine."""
        await self.location_hierarchy.initialize_from_data_warehouse()

        log_structured("info", "portfolio_rollup_engine_initialized")

    async def calculate_rollup(
        self,
        positions: List[Position],
        as_of_date: datetime,
        iso: str = "isone",
        scenario_id: Optional[str] = None,
    ) -> PortfolioRollup:
        """Calculate complete portfolio rollup for given positions."""

        cache_key = f"{iso}_{as_of_date.isoformat()}_{scenario_id or 'baseline'}"

        if cache_key in self._cache and self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        # Calculate base exposures
        base_exposures = await self._calculate_base_exposures(positions, as_of_date, iso)

        # Apply hierarchical rollup
        hierarchy_exposures = await self._rollup_hierarchically(base_exposures, iso)

        # Calculate risk metrics
        risk_summary = {}
        if self.config.include_risk_metrics:
            risk_summary = await self._calculate_risk_metrics(hierarchy_exposures, as_of_date)

        # Calculate scenario exposures
        scenario_exposures = {}
        if self.config.include_scenario_analysis and scenario_id:
            scenario_exposures = await self._calculate_scenario_exposures(
                positions, scenario_id, as_of_date, iso
            )

        # Create total exposure
        total_exposure = self._aggregate_exposures(list(hierarchy_exposures.values()))

        rollup = PortfolioRollup(
            as_of_date=as_of_date,
            total_exposure=total_exposure,
            hierarchy_exposures=hierarchy_exposures,
            risk_summary=risk_summary,
            scenario_exposures=scenario_exposures,
        )

        # Cache the result
        self._cache[cache_key] = rollup

        log_structured(
            "info",
            "portfolio_rollup_calculated",
            iso=iso,
            num_positions=len(positions),
            num_hierarchy_levels=len(hierarchy_exposures),
            total_exposure=total_exposure.exposure_amount,
        )

        return rollup

    async def _calculate_base_exposures(
        self,
        positions: List[Position],
        as_of_date: datetime,
        iso: str
    ) -> Dict[str, Exposure]:
        """Calculate base exposures for each position location."""
        exposures = {}

        for position in positions:
            location = position.location

            if location not in exposures:
                exposures[location] = Exposure(
                    entity_id=location,
                    entity_type="node",
                    as_of_date=as_of_date,
                )

            # Calculate position exposure
            position_exposure = await self._calculate_position_exposure(position, as_of_date, iso)

            # Add to location exposure
            exposures[location].exposure_amount += position_exposure
            exposures[location].positions.append(position)

            # Update risk metrics
            if self.config.include_risk_metrics:
                exposures[location].delta += position_exposure * 0.01  # Simplified delta

        return exposures

    async def _calculate_position_exposure(
        self,
        position: Position,
        as_of_date: datetime,
        iso: str
    ) -> float:
        """Calculate exposure for a single position."""

        if position.instrument_type == "curve":
            # Get curve price from the data warehouse
            curve_price = await self._get_curve_price(position.curve_key, as_of_date, iso)
            exposure = position.quantity * curve_price
        elif position.instrument_type == "contract":
            # Contract exposure based on contract terms
            exposure = position.quantity * (position.price or 50.0)  # Default price
        else:
            # Physical position exposure
            exposure = position.quantity * 1000  # Simplified calculation

        return exposure

    async def _get_curve_price(self, curve_key: str, as_of_date: datetime, iso: str) -> float:
        """Get curve price from the data warehouse."""
        # This would query the actual curve data
        # For now, return a synthetic price
        import random
        return 50.0 + random.normalvariate(0, 10)

    async def _rollup_hierarchically(
        self,
        base_exposures: Dict[str, Exposure],
        iso: str
    ) -> Dict[str, Exposure]:
        """Roll up exposures through the hierarchy."""
        hierarchy_exposures = {}

        # Start with node-level exposures
        hierarchy_exposures.update(base_exposures)

        # Roll up to zones
        zone_exposures = await self._rollup_to_zones(base_exposures, iso)
        hierarchy_exposures.update(zone_exposures)

        # Roll up to BAs
        ba_exposures = await self._rollup_to_bas(zone_exposures, iso)
        hierarchy_exposures.update(ba_exposures)

        # Roll up to ISO
        iso_exposures = await self._rollup_to_iso(ba_exposures, iso)
        hierarchy_exposures.update(iso_exposures)

        return hierarchy_exposures

    async def _rollup_to_zones(
        self,
        node_exposures: Dict[str, Exposure],
        iso: str
    ) -> Dict[str, Exposure]:
        """Roll up node exposures to zones."""
        zone_exposures = {}

        for node_id, exposure in node_exposures.items():
            zone_id = self.location_hierarchy.get_parent(iso, "node", node_id)

            if zone_id:
                if zone_id not in zone_exposures:
                    zone_exposures[zone_id] = Exposure(
                        entity_id=zone_id,
                        entity_type="zone",
                        as_of_date=exposure.as_of_date,
                    )

                # Aggregate exposure
                zone_exposures[zone_id].exposure_amount += exposure.exposure_amount
                zone_exposures[zone_id].positions.extend(exposure.positions)

                # Aggregate risk metrics
                zone_exposures[zone_id].delta += exposure.delta
                zone_exposures[zone_id].gamma += exposure.gamma
                zone_exposures[zone_id].vega += exposure.vega
                zone_exposures[zone_id].theta += exposure.theta

        return zone_exposures

    async def _rollup_to_bas(
        self,
        zone_exposures: Dict[str, Exposure],
        iso: str
    ) -> Dict[str, Exposure]:
        """Roll up zone exposures to balancing authorities."""
        ba_exposures = {}

        for zone_id, exposure in zone_exposures.items():
            ba_id = self.location_hierarchy.get_parent(iso, "zone", zone_id)

            if ba_id:
                if ba_id not in ba_exposures:
                    ba_exposures[ba_id] = Exposure(
                        entity_id=ba_id,
                        entity_type="ba",
                        as_of_date=exposure.as_of_date,
                    )

                # Aggregate exposure
                ba_exposures[ba_id].exposure_amount += exposure.exposure_amount
                ba_exposures[ba_id].positions.extend(exposure.positions)

                # Aggregate risk metrics
                ba_exposures[ba_id].delta += exposure.delta
                ba_exposures[ba_id].gamma += exposure.gamma
                ba_exposures[ba_id].vega += exposure.vega
                ba_exposures[ba_id].theta += exposure.theta

        return ba_exposures

    async def _rollup_to_iso(
        self,
        ba_exposures: Dict[str, Exposure],
        iso: str
    ) -> Dict[str, Exposure]:
        """Roll up BA exposures to ISO level."""
        iso_exposures = {}

        for ba_id, exposure in ba_exposures.items():
            iso_id = self.location_hierarchy.get_parent(iso, "ba", ba_id)

            if iso_id:
                if iso_id not in iso_exposures:
                    iso_exposures[iso_id] = Exposure(
                        entity_id=iso_id,
                        entity_type="iso",
                        as_of_date=exposure.as_of_date,
                    )

                # Aggregate exposure
                iso_exposures[iso_id].exposure_amount += exposure.exposure_amount
                iso_exposures[iso_id].positions.extend(exposure.positions)

                # Aggregate risk metrics
                iso_exposures[iso_id].delta += exposure.delta
                iso_exposures[iso_id].gamma += exposure.gamma
                iso_exposures[iso_id].vega += exposure.vega
                iso_exposures[iso_id].theta += exposure.theta

        return iso_exposures

    async def _calculate_risk_metrics(
        self,
        hierarchy_exposures: Dict[str, Exposure],
        as_of_date: datetime
    ) -> Dict[str, Any]:
        """Calculate risk metrics for the portfolio."""

        total_exposure = sum(exp.exposure_amount for exp in hierarchy_exposures.values())

        if total_exposure == 0:
            return {"var_95": 0, "var_99": 0, "max_drawdown": 0}

        # Simplified risk calculations
        # In reality, this would use historical data and more sophisticated models
        daily_volatility = 0.02  # 2% daily volatility assumption

        var_95 = total_exposure * daily_volatility * 1.645  # 95% confidence
        var_99 = total_exposure * daily_volatility * 2.326  # 99% confidence
        max_drawdown = total_exposure * 0.1  # 10% max drawdown assumption

        return {
            "var_95": var_95,
            "var_99": var_99,
            "max_drawdown": max_drawdown,
            "daily_volatility": daily_volatility,
            "concentration_risk": self._calculate_concentration_risk(hierarchy_exposures),
        }

    def _calculate_concentration_risk(self, hierarchy_exposures: Dict[str, Exposure]) -> float:
        """Calculate concentration risk across the portfolio."""
        if not hierarchy_exposures:
            return 0.0

        total_exposure = sum(exp.exposure_amount for exp in hierarchy_exposures.values())
        if total_exposure == 0:
            return 0.0

        # Calculate Herfindahl-Hirschman Index for concentration
        concentration_ratios = [
            (exp.exposure_amount / total_exposure) ** 2
            for exp in hierarchy_exposures.values()
            if exp.exposure_amount > 0
        ]

        hhi = sum(concentration_ratios) if concentration_ratios else 0

        # Normalize to 0-1 scale (1.0 = maximum concentration)
        max_possible_hhi = 1.0 / len(hierarchy_exposures) if hierarchy_exposures else 0
        normalized_hhi = hhi / max_possible_hhi if max_possible_hhi > 0 else 0

        return min(1.0, normalized_hhi)

    async def _calculate_scenario_exposures(
        self,
        positions: List[Position],
        scenario_id: str,
        as_of_date: datetime,
        iso: str
    ) -> Dict[str, Exposure]:
        """Calculate exposures under different scenarios."""

        # This would integrate with the scenario engine to run what-if analysis
        # For now, return a simplified scenario exposure
        scenario_exposure = Exposure(
            entity_id="scenario",
            entity_type="scenario",
            exposure_amount=sum(pos.quantity * 55.0 for pos in positions),  # Scenario price
            as_of_date=as_of_date,
        )

        return {scenario_id: scenario_exposure}

    def _aggregate_exposures(self, exposures: List[Exposure]) -> Exposure:
        """Aggregate multiple exposures into a single exposure."""

        if not exposures:
            return Exposure(entity_id="total", entity_type="total")

        total_exposure = Exposure(
            entity_id="total",
            entity_type="total",
            as_of_date=exposures[0].as_of_date,
        )

        total_exposure.exposure_amount = sum(exp.exposure_amount for exp in exposures)
        total_exposure.delta = sum(exp.delta for exp in exposures)
        total_exposure.gamma = sum(exp.gamma for exp in exposures)
        total_exposure.vega = sum(exp.vega for exp in exposures)
        total_exposure.theta = sum(exp.theta for exp in exposures)

        # Calculate aggregate risk metrics
        total_exposure.var_95 = sum(exp.var_95 for exp in exposures)
        total_exposure.var_99 = sum(exp.var_99 for exp in exposures)
        total_exposure.max_drawdown = max(exp.max_drawdown for exp in exposures)

        # Collect all positions
        for exp in exposures:
            total_exposure.positions.extend(exp.positions)

        return total_exposure

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached result is still valid."""
        if cache_key not in self._cache:
            return False

        cached_time = self._cache[cache_key].as_of_date
        age_minutes = (datetime.now() - cached_time).total_seconds() / 60

        return age_minutes < self.config.cache_ttl_minutes

    async def get_rollup_summary(
        self,
        as_of_date: datetime,
        iso: str = "isone"
    ) -> Dict[str, Any]:
        """Get a summary of portfolio rollup data."""

        # This would query the actual rollup tables
        # For now, return a sample summary
        return {
            "as_of_date": as_of_date,
            "iso": iso,
            "total_exposure": 1000000.0,
            "num_positions": 150,
            "hierarchy_levels": ["node", "zone", "ba", "iso"],
            "risk_metrics": {
                "var_95": 50000.0,
                "var_99": 75000.0,
                "concentration_risk": 0.3
            }
        }


# Global portfolio rollup engine instance
_rollup_engine: Optional[PortfolioRollupEngine] = None


def get_rollup_engine() -> PortfolioRollupEngine:
    """Get the global portfolio rollup engine instance."""
    global _rollup_engine
    if _rollup_engine is None:
        _rollup_engine = PortfolioRollupEngine(RollupConfig())
    return _rollup_engine
