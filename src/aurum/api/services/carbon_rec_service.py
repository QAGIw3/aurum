"""Carbon and REC Data Integration Service.

This service provides:
- Unified carbon instruments schema for credits, allowances, and RECs
- Asset pricing APIs including carbon cost calculations
- Portfolio scenario integration with carbon pricing
- Carbon market data ingestion and analysis
- Regulatory compliance tracking for carbon instruments
- Integration with forecasting and risk management systems
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4
from enum import Enum

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..daos.base_dao import TrinoDAO


class CarbonInstrumentType(str, Enum):
    """Types of carbon instruments."""
    CARBON_CREDIT = "carbon_credit"
    CARBON_ALLOWANCE = "carbon_allowance"
    RENEWABLE_ENERGY_CERTIFICATE = "rec"
    CARBON_OFFSET = "carbon_offset"
    CARBON_FUTURE = "carbon_future"


class CarbonMarket(str, Enum):
    """Carbon markets and registries."""
    EU_ETS = "eu_ets"
    CALIFORNIA_CAP_AND_TRADE = "california_cat"
    RGGI = "rggi"
    AUSTRALIAN_ETS = "australian_ets"
    KOREA_ETS = "korea_ets"
    VOLUNTARY_MARKETS = "voluntary"
    REC_MARKETS = "rec_markets"


class CarbonInstrument(BaseModel):
    """Carbon instrument definition."""

    instrument_id: str
    instrument_type: CarbonInstrumentType
    market: CarbonMarket
    vintage_year: int
    expiry_date: Optional[datetime]
    quantity_tons: float
    status: str = "active"  # "active", "retired", "cancelled", "expired"
    registry_id: str
    project_id: Optional[str] = None
    project_type: Optional[str] = None
    location: str
    methodology: str
    verification_standard: str
    verification_body: str
    issuance_date: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class CarbonPricing(BaseModel):
    """Carbon pricing data for assets."""

    asset_id: str
    timestamp: datetime
    geography: str
    carbon_intensity: float  # tons CO2 per MWh
    carbon_price: float  # $/ton CO2
    carbon_cost: float  # $/MWh
    carbon_cost_percent: float  # percentage of total cost
    pricing_method: str  # "market_price", "shadow_price", "compliance_cost"
    market_source: str
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class PortfolioCarbonExposure(BaseModel):
    """Carbon exposure analysis for portfolios."""

    portfolio_id: str
    analysis_date: datetime
    total_emissions_tons: float
    total_carbon_cost: float
    carbon_intensity: float  # tons CO2 per MWh
    carbon_cost_per_mwh: float
    compliance_obligations: Dict[str, float]  # Market -> tons required
    carbon_credits_held: Dict[str, int]  # Market -> credits count
    net_carbon_position: float
    risk_metrics: Dict[str, float]
    scenario_impact: Dict[str, float]  # Scenario -> impact percentage


class RECTrading(BaseModel):
    """Renewable Energy Certificate trading data."""

    rec_id: str
    vintage_year: int
    generation_date: datetime
    generation_source: str  # "solar", "wind", "hydro", etc.
    generation_location: str
    quantity_mwh: float
    status: str = "issued"  # "issued", "traded", "retired", "expired"
    current_owner: str
    trading_history: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CarbonRecService:
    """Carbon and REC data integration service."""

    def __init__(self):
        """Initialize carbon REC service."""
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Carbon market data cache
        self._carbon_markets: Dict[str, Dict[str, Any]] = {}
        self._carbon_instruments: Dict[str, CarbonInstrument] = {}
        self._carbon_pricing: Dict[str, List[CarbonPricing]] = defaultdict(list)
        self._portfolio_exposures: Dict[str, PortfolioCarbonExposure] = {}

        # Initialize carbon market data
        self._initialize_carbon_markets()

    def _initialize_carbon_markets(self) -> None:
        """Initialize carbon market configurations."""
        self._carbon_markets = {
            "eu_ets": {
                "name": "EU Emissions Trading System",
                "currency": "EUR",
                "price_range": (20, 120),  # €/ton CO2
                "compliance_period": "2021-2030",
                "sectors": ["power", "industry", "aviation"]
            },
            "california_cat": {
                "name": "California Cap-and-Trade",
                "currency": "USD",
                "price_range": (15, 100),  # $/ton CO2
                "compliance_period": "2021-2030",
                "sectors": ["power", "industry", "transportation"]
            },
            "rggi": {
                "name": "Regional Greenhouse Gas Initiative",
                "currency": "USD",
                "price_range": (5, 15),  # $/ton CO2
                "compliance_period": "2021-2025",
                "sectors": ["power"]
            }
        }

    async def register_carbon_instrument(self, instrument: CarbonInstrument) -> bool:
        """Register a new carbon instrument."""
        try:
            self._carbon_instruments[instrument.instrument_id] = instrument

            # Store in database (mock implementation)
            await self._store_carbon_instrument(instrument)

            self.telemetry.info("Carbon instrument registered", instrument_id=instrument.instrument_id)
            return True
        except Exception as e:
            self.telemetry.error("Failed to register carbon instrument", instrument_id=instrument.instrument_id, error=str(e))
            return False

    async def _store_carbon_instrument(self, instrument: CarbonInstrument) -> None:
        """Store carbon instrument in database."""
        # Mock implementation - would insert into carbon_instruments table
        pass

    async def get_carbon_pricing(
        self,
        asset_id: str,
        geography: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[CarbonPricing]:
        """Get carbon pricing for an asset over time period."""
        # Mock implementation
        pricing_data = []

        for day in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=day)

            # Simulate carbon intensity and pricing
            base_intensity = 0.4  # tons CO2/MWh
            intensity_variation = np.random.normal(0, 0.1)
            carbon_intensity = max(0.1, base_intensity + intensity_variation)

            # Get market price based on geography
            market_price = self._get_market_carbon_price(geography, date)

            carbon_cost = carbon_intensity * market_price
            carbon_cost_percent = carbon_cost / 100.0  # Assume $100/MWh base price

            pricing = CarbonPricing(
                asset_id=asset_id,
                timestamp=date,
                geography=geography,
                carbon_intensity=carbon_intensity,
                carbon_price=market_price,
                carbon_cost=carbon_cost,
                carbon_cost_percent=carbon_cost_percent,
                pricing_method="market_price",
                market_source=self._get_market_source(geography),
                confidence=0.85
            )

            pricing_data.append(pricing)

        return pricing_data

    def _get_market_carbon_price(self, geography: str, date: datetime) -> float:
        """Get carbon price for geography and date."""
        if geography.lower() in ["eu", "europe"]:
            market = self._carbon_markets.get("eu_ets", {})
        elif geography.lower() in ["california", "ca"]:
            market = self._carbon_markets.get("california_cat", {})
        elif geography.lower() in ["northeast", "rggi"]:
            market = self._carbon_markets.get("rggi", {})
        else:
            market = self._carbon_markets.get("eu_ets", {})  # Default

        price_range = market.get("price_range", (20, 80))
        base_price = (price_range[0] + price_range[1]) / 2

        # Add some temporal variation
        volatility = 0.1
        price_variation = np.random.normal(0, base_price * volatility)
        return max(price_range[0], min(price_range[1], base_price + price_variation))

    def _get_market_source(self, geography: str) -> str:
        """Get carbon market source name."""
        if geography.lower() in ["eu", "europe"]:
            return "EU ETS"
        elif geography.lower() in ["california", "ca"]:
            return "California Cap-and-Trade"
        elif geography.lower() in ["northeast", "rggi"]:
            return "RGGI"
        else:
            return "International Carbon Markets"

    async def calculate_portfolio_carbon_exposure(
        self,
        portfolio_id: str,
        scenario_id: str,
        analysis_date: datetime
    ) -> PortfolioCarbonExposure:
        """Calculate carbon exposure for a portfolio under a scenario."""

        # Mock implementation
        total_emissions = 1000000.0  # tons CO2
        total_carbon_cost = 50000000.0  # $50M

        exposure = PortfolioCarbonExposure(
            portfolio_id=portfolio_id,
            analysis_date=analysis_date,
            total_emissions_tons=total_emissions,
            total_carbon_cost=total_carbon_cost,
            carbon_intensity=0.45,  # tons CO2/MWh
            carbon_cost_per_mwh=20.0,  # $/MWh
            compliance_obligations={
                "eu_ets": 800000,
                "california_cat": 200000
            },
            carbon_credits_held={
                "eu_ets": 500000,
                "california_cat": 150000
            },
            net_carbon_position=total_emissions - sum([
                self._carbon_credits_held.get(market, 0) for market in ["eu_ets", "california_cat"]
            ]),
            risk_metrics={
                "carbon_price_volatility": 0.25,
                "compliance_risk": 0.15,
                "market_risk": 0.20
            },
            scenario_impact={
                "baseline": 0.0,
                "high_carbon_price": 0.30,
                "low_carbon_price": -0.20
            }
        )

        self._portfolio_exposures[portfolio_id] = exposure
        return exposure

    async def get_rec_trading_data(
        self,
        geography: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[RECTrading]:
        """Get REC trading data for geography and time period."""
        # Mock implementation
        recs = []

        for day in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=day)

            rec = RECTrading(
                rec_id=str(uuid4()),
                vintage_year=date.year,
                generation_date=date,
                generation_source="solar",
                generation_location=geography,
                quantity_mwh=1000.0 + np.random.normal(0, 100),
                status="issued",
                current_owner="portfolio_123",
                trading_history=[
                    {
                        "timestamp": date,
                        "action": "issued",
                        "price": 15.0,
                        "quantity": 1000.0
                    }
                ]
            )

            recs.append(rec)

        return recs

    async def calculate_asset_carbon_cost(
        self,
        asset_id: str,
        geography: str,
        generation_mwh: float,
        carbon_intensity: float,
        pricing_date: datetime
    ) -> Dict[str, float]:
        """Calculate carbon cost for asset generation."""

        # Get carbon pricing
        pricing = await self.get_carbon_pricing(
            asset_id, geography, pricing_date, pricing_date
        )

        if pricing:
            carbon_price = pricing[0].carbon_price
            carbon_cost = generation_mwh * carbon_intensity * carbon_price

            return {
                "carbon_cost_usd": carbon_cost,
                "carbon_cost_per_mwh": carbon_intensity * carbon_price,
                "carbon_intensity": carbon_intensity,
                "carbon_price": carbon_price,
                "generation_mwh": generation_mwh
            }

        return {}

    async def get_carbon_market_data(
        self,
        market: CarbonMarket,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get carbon market price and volume data."""
        market_info = self._carbon_markets.get(market.value, {})

        # Mock market data
        data_points = []

        for day in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=day)

            price_range = market_info.get("price_range", (20, 80))
            base_price = (price_range[0] + price_range[1]) / 2
            price = base_price + np.random.normal(0, base_price * 0.05)

            data_points.append({
                "date": date,
                "price": max(price_range[0], min(price_range[1], price)),
                "volume": int(np.random.normal(1000000, 200000)),
                "currency": market_info.get("currency", "USD")
            })

        return {
            "market": market.value,
            "market_name": market_info.get("name", "Unknown"),
            "data": data_points,
            "summary": {
                "avg_price": np.mean([p["price"] for p in data_points]),
                "price_volatility": np.std([p["price"] for p in data_points]),
                "total_volume": sum([p["volume"] for p in data_points])
            }
        }

    async def get_portfolio_carbon_exposure(self, portfolio_id: str) -> Optional[PortfolioCarbonExposure]:
        """Get cached carbon exposure analysis."""
        return self._portfolio_exposures.get(portfolio_id)

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "carbon_instruments": len(self._carbon_instruments),
            "carbon_markets": len(self._carbon_markets),
            "portfolio_exposures": len(self._portfolio_exposures),
            "last_update": datetime.utcnow()
        }


def get_carbon_rec_service() -> CarbonRecService:
    """Get the global carbon REC service instance."""
    return CarbonRecService()


async def calculate_asset_carbon_impact(
    asset_id: str,
    generation_profile: List[Dict[str, Any]],
    carbon_intensity: float,
    geography: str
) -> Dict[str, Any]:
    """Calculate comprehensive carbon impact for asset generation profile."""
    service = get_carbon_rec_service()

    total_carbon_cost = 0.0
    total_emissions = 0.0
    carbon_cost_breakdown = []

    for period in generation_profile:
        generation_mwh = period.get("generation_mwh", 0)
        timestamp = period.get("timestamp", datetime.utcnow())

        emissions = generation_mwh * carbon_intensity
        total_emissions += emissions

        # Get carbon pricing for this period
        pricing = await service.get_carbon_pricing(
            asset_id, geography, timestamp, timestamp
        )

        if pricing:
            carbon_cost = emissions * pricing[0].carbon_price
            total_carbon_cost += carbon_cost

            carbon_cost_breakdown.append({
                "timestamp": timestamp,
                "generation_mwh": generation_mwh,
                "emissions_tons": emissions,
                "carbon_price": pricing[0].carbon_price,
                "carbon_cost": carbon_cost
            })

    return {
        "asset_id": asset_id,
        "total_emissions_tons": total_emissions,
        "total_carbon_cost_usd": total_carbon_cost,
        "carbon_cost_per_mwh": total_carbon_cost / sum(p["generation_mwh"] for p in generation_profile) if generation_profile else 0,
        "carbon_intensity": carbon_intensity,
        "period_breakdown": carbon_cost_breakdown
    }


async def analyze_portfolio_carbon_risk(
    portfolio_id: str,
    scenario_id: str,
    risk_horizon_days: int = 365
) -> Dict[str, Any]:
    """Analyze carbon risk for portfolio under scenario."""
    service = get_carbon_rec_service()

    # Get current exposure
    current_exposure = await service.calculate_portfolio_carbon_exposure(
        portfolio_id, scenario_id, datetime.utcnow()
    )

    # Simulate risk scenarios
    risk_scenarios = {
        "carbon_price_shock": 0.5,  # 50% price increase
        "regulatory_change": 0.3,   # 30% cost increase
        "market_disruption": 0.2    # 20% volatility increase
    }

    scenario_impacts = {}
    for scenario, impact_factor in risk_scenarios.items():
        impacted_cost = current_exposure.total_carbon_cost * (1 + impact_factor)
        scenario_impacts[scenario] = {
            "current_cost": current_exposure.total_carbon_cost,
            "impacted_cost": impacted_cost,
            "additional_cost": impacted_cost - current_exposure.total_carbon_cost,
            "risk_level": "high" if impact_factor > 0.3 else "medium"
        }

    return {
        "portfolio_id": portfolio_id,
        "scenario_id": scenario_id,
        "current_exposure": current_exposure,
        "risk_scenarios": scenario_impacts,
        "recommendations": [
            "Consider carbon credit purchases for compliance",
            "Evaluate carbon capture investments",
            "Monitor regulatory developments closely"
        ]
    }
