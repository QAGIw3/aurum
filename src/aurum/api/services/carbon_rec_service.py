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

import hashlib
import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple
from pydantic import BaseModel, Field

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
        self._asset_profiles: Dict[str, Dict[str, Any]] = {}
        self._portfolio_holdings: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._carbon_credits_held: Dict[str, Dict[str, float]] = defaultdict(dict)
        self._market_shocks: Dict[str, List[Dict[str, Any]]] = {}
        self._market_trends: Dict[str, float] = {}
        self._rec_registry: Dict[str, List[RECTrading]] = defaultdict(list)
        self._rec_market_profiles: Dict[str, Dict[str, Any]] = {}

        # Initialize carbon market data
        self._initialize_carbon_markets()
        self._initialize_market_signals()
        self._initialize_asset_profiles()
        self._initialize_rec_market_profiles()

    def _initialize_carbon_markets(self) -> None:
        """Initialize carbon market configurations."""
        self._carbon_markets = {
            "eu_ets": {
                "name": "EU Emissions Trading System",
                "currency": "EUR",
                "price_range": (20, 120),  # €/ton CO2
                "avg_daily_volume": 2_500_000,
                "compliance_period": "2021-2030",
                "sectors": ["power", "industry", "aviation"]
            },
            "california_cat": {
                "name": "California Cap-and-Trade",
                "currency": "USD",
                "price_range": (15, 100),  # $/ton CO2
                "avg_daily_volume": 1_200_000,
                "compliance_period": "2021-2030",
                "sectors": ["power", "industry", "transportation"]
            },
            "rggi": {
                "name": "Regional Greenhouse Gas Initiative",
                "currency": "USD",
                "price_range": (5, 15),  # $/ton CO2
                "avg_daily_volume": 550_000,
                "compliance_period": "2021-2025",
                "sectors": ["power"]
            }
        }

    def _initialize_market_signals(self) -> None:
        """Initialize deterministic market trend and shock data."""
        self._market_trends = {
            "eu_ets": 0.018,          # 1.8% annual upward drift from policy tightening
            "california_cat": 0.014,  # 1.4% annual upward drift from allowance scarcity
            "rggi": 0.010,            # 1.0% annual upward drift driven by demand
            "voluntary": 0.012,
            "rec_markets": 0.011
        }

        baseline_year = datetime.utcnow().year
        self._market_shocks = {
            "eu_ets": [
                {
                    "name": "fit_for_55_supply_reduction",
                    "start": datetime(baseline_year, 2, 1),
                    "end": datetime(baseline_year, 4, 30),
                    "impact": 0.06  # +6% premium
                },
                {
                    "name": "industrial_demand_softening",
                    "start": datetime(baseline_year, 9, 1),
                    "end": datetime(baseline_year, 10, 15),
                    "impact": -0.035  # -3.5% discount
                }
            ],
            "california_cat": [
                {
                    "name": "auction_withdrawal",
                    "start": datetime(baseline_year, 5, 15),
                    "end": datetime(baseline_year, 6, 10),
                    "impact": 0.045
                }
            ],
            "rggi": [
                {
                    "name": "mild_winter",
                    "start": datetime(baseline_year, 1, 1),
                    "end": datetime(baseline_year, 2, 15),
                    "impact": -0.025
                }
            ],
            "voluntary": [],
            "rec_markets": []
        }

    def _initialize_asset_profiles(self) -> None:
        """Initialize representative asset carbon profiles and holdings."""
        self._asset_profiles = {
            "wind_farm_001": {
                "asset_type": "wind",
                "baseline_intensity": 0.015,
                "abatement_factor": 0.40,
                "avg_energy_price": 52.0,
                "geography": "eu",
                "capacity_mw": 150,
                "avg_utilization": 0.48
            },
            "solar_site_007": {
                "asset_type": "solar",
                "baseline_intensity": 0.020,
                "abatement_factor": 0.55,
                "avg_energy_price": 45.0,
                "geography": "california",
                "capacity_mw": 90,
                "avg_utilization": 0.32
            },
            "combined_cycle_305": {
                "asset_type": "gas",
                "baseline_intensity": 0.38,
                "abatement_factor": 0.12,
                "avg_energy_price": 78.0,
                "geography": "northeast",
                "capacity_mw": 420,
                "avg_utilization": 0.62
            },
            "refinery_cogen_112": {
                "asset_type": "cogen",
                "baseline_intensity": 0.42,
                "abatement_factor": 0.08,
                "avg_energy_price": 92.0,
                "geography": "california",
                "capacity_mw": 160,
                "avg_utilization": 0.74
            }
        }

        # Illustrative holdings for common portfolios to support analytics demos
        self._portfolio_holdings.update(
            {
                "portfolio_123": [
                    {"asset_id": "wind_farm_001", "geography": "eu", "generation_mwh": 320_000},
                    {"asset_id": "combined_cycle_305", "geography": "northeast", "generation_mwh": 610_000}
                ],
                "portfolio_transition": [
                    {"asset_id": "solar_site_007", "geography": "california", "generation_mwh": 210_000},
                    {"asset_id": "combined_cycle_305", "geography": "northeast", "generation_mwh": 480_000},
                    {"asset_id": "refinery_cogen_112", "geography": "california", "generation_mwh": 300_000}
                ]
            }
        )

        # Seed illustrative compliance positions
        self._carbon_credits_held.update(
            {
                "portfolio_123": {"eu_ets": 410_000, "rggi": 140_000},
                "portfolio_transition": {"california_cat": 185_000, "eu_ets": 95_000}
            }
        )

    async def _get_portfolio_holdings(self, portfolio_id: str) -> List[Dict[str, Any]]:
        """Retrieve holdings for a portfolio with deterministic synthesis fallback."""
        if portfolio_id in self._portfolio_holdings:
            return self._portfolio_holdings[portfolio_id]

        cache_key = f"portfolio_holdings:{portfolio_id}"
        cached_holdings = await self.cache_manager.get(cache_key)
        if isinstance(cached_holdings, list) and cached_holdings:
            self._portfolio_holdings[portfolio_id] = cached_holdings
            return cached_holdings

        synthesized = self._generate_synthetic_holdings(portfolio_id)
        if synthesized:
            self._portfolio_holdings[portfolio_id] = synthesized
            await self.cache_manager.set(cache_key, synthesized, ttl_seconds=3600)

        return synthesized

    def _initialize_rec_market_profiles(self) -> None:
        """Initialize REC market profiles with deterministic parameters."""
        self._rec_market_profiles = {
            "california": {
                "region": "CAISO",
                "base_price": 18.5,
                "base_quantity": 780.0,
                "preferred_sources": ["solar", "geothermal"],
                "compliance_months": [3, 6, 9, 12]
            },
            "texas": {
                "region": "ERCOT",
                "base_price": 10.2,
                "base_quantity": 910.0,
                "preferred_sources": ["wind", "solar"],
                "compliance_months": [2, 5, 8, 11]
            },
            "northeast": {
                "region": "PJM",
                "base_price": 13.1,
                "base_quantity": 650.0,
                "preferred_sources": ["wind", "hydro"],
                "compliance_months": [4, 7, 10, 12]
            },
            "midwest": {
                "region": "MISO",
                "base_price": 7.8,
                "base_quantity": 720.0,
                "preferred_sources": ["wind", "biomass"],
                "compliance_months": [3, 6, 9, 12]
            },
            "rec_default": {
                "region": "North America",
                "base_price": 9.5,
                "base_quantity": 680.0,
                "preferred_sources": ["wind", "solar"],
                "compliance_months": [3, 6, 9, 12]
            }
        }

    def _generate_synthetic_holdings(self, portfolio_id: str) -> List[Dict[str, Any]]:
        """Generate deterministic holdings when no explicit data is available."""
        if not self._asset_profiles:
            return []

        asset_ids = sorted(self._asset_profiles.keys())
        digest = hashlib.sha1(portfolio_id.encode("utf-8")).hexdigest()
        seed_value = int(digest[:8], 16)

        synthesized: List[Dict[str, Any]] = []
        for idx, asset_id in enumerate(asset_ids):
            selector = (seed_value + idx * 97) % 3
            if selector == 0:
                continue

            profile = self._asset_profiles[asset_id]
            capacity_mw = profile.get("capacity_mw", 100)
            utilization = profile.get("avg_utilization", 0.5)
            annual_generation = capacity_mw * 8760 * utilization * (0.25 + (selector * 0.1))

            synthesized.append(
                {
                    "asset_id": asset_id,
                    "geography": profile.get("geography", "eu"),
                    "generation_mwh": max(50_000, round(annual_generation))
                }
            )

        if not synthesized:
            # Ensure at least one holding exists
            fallback_asset = asset_ids[seed_value % len(asset_ids)]
            profile = self._asset_profiles[fallback_asset]
            fallback_generation = profile.get("capacity_mw", 100) * 8760 * profile.get("avg_utilization", 0.5) * 0.3
            synthesized.append(
                {
                    "asset_id": fallback_asset,
                    "geography": profile.get("geography", "eu"),
                    "generation_mwh": max(50_000, round(fallback_generation))
                }
            )

        return synthesized

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
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        market_key = self._resolve_market_key(geography)
        cache_key = (
            f"carbon_pricing:{asset_id}:{market_key}"
            f":{start_date.strftime('%Y%m%d')}:{end_date.strftime('%Y%m%d')}"
        )

        cached_series = await self.cache_manager.get(cache_key)
        if cached_series:
            pricing_objects = [
                CarbonPricing(**entry) if isinstance(entry, dict) else entry
                for entry in cached_series
            ]
            self._carbon_pricing[asset_id] = pricing_objects
            return pricing_objects

        asset_profile = self._asset_profiles.get(asset_id, {
            "asset_type": "generic",
            "baseline_intensity": 0.32,
            "abatement_factor": 0.05,
            "avg_energy_price": 100.0,
            "geography": geography,
            "capacity_mw": 100,
            "avg_utilization": 0.55
        })

        results: List[CarbonPricing] = []
        intensity_history = {}
        pricing_history = {}

        for day in range((end_date - start_date).days + 1):
            observation_date = start_date + timedelta(days=day)
            market_price, price_metadata = await self._get_market_carbon_price(
                market_key, observation_date
            )

            carbon_intensity = self._calculate_asset_carbon_intensity(
                asset_profile, market_key, observation_date
            )

            carbon_cost_per_mwh = carbon_intensity * market_price
            energy_price = asset_profile.get("avg_energy_price", 100.0)
            carbon_cost_percent = (
                carbon_cost_per_mwh / energy_price if energy_price else 0.0
            )

            pricing_point = CarbonPricing(
                asset_id=asset_id,
                timestamp=observation_date,
                geography=geography,
                carbon_intensity=carbon_intensity,
                carbon_price=market_price,
                carbon_cost=carbon_cost_per_mwh,
                carbon_cost_percent=carbon_cost_percent,
                pricing_method="market_price_live",
                market_source=self._get_market_source(market_key),
                confidence=max(0.6, 0.95 - abs(price_metadata.get("shock_adjustment", 0.0))),
                metadata={
                    "asset_type": asset_profile.get("asset_type", "generic"),
                    "market_key": market_key,
                    "seasonal_component": price_metadata.get("seasonal_component"),
                    "weekly_component": price_metadata.get("weekly_component"),
                    "shock_adjustment": price_metadata.get("shock_adjustment"),
                    "regulatory_drift": price_metadata.get("regulatory_drift"),
                    "currency": price_metadata.get("currency", "USD")
                }
            )

            results.append(pricing_point)
            intensity_history[observation_date] = carbon_intensity
            pricing_history[observation_date] = market_price

        await self.cache_manager.set(
            cache_key,
            [pricing.dict() for pricing in results],
            ttl_seconds=900
        )

        existing_history = {
            entry.timestamp: entry for entry in self._carbon_pricing.get(asset_id, [])
        }
        existing_history.update({point.timestamp: point for point in results})
        ordered_history = [
            existing_history[timestamp]
            for timestamp in sorted(existing_history.keys())
        ]
        self._carbon_pricing[asset_id] = ordered_history

        self.telemetry.info(
            "Carbon pricing series generated",
            asset_id=asset_id,
            geography=geography,
            market_key=market_key,
            periods=len(results),
            avg_price=statistics.mean(pricing_history.values()),
            avg_intensity=statistics.mean(intensity_history.values())
            if intensity_history else None
        )

        if results:
            self.telemetry.record_gauge(
                "carbon_price_usd_per_ton",
                value=results[-1].carbon_price,
                category=MetricCategory.BUSINESS,
                market=market_key
            )

        return results

    def _resolve_market_key(self, geography: str) -> str:
        """Resolve internal market key from geography or market alias."""
        geography_key = geography.lower()

        if geography_key in {"eu", "europe", "germany", "france", "uk"}:
            return "eu_ets"
        if geography_key in {"california", "ca", "us_ca", "western_usa"}:
            return "california_cat"
        if geography_key in {"northeast", "rggi", "new_york", "massachusetts"}:
            return "rggi"
        if geography_key in {"voluntary", "offset", "nature_based"}:
            return "voluntary"
        if geography_key in {"rec", "renewable", "ercot", "pjm"}:
            return "rec_markets"

        return "eu_ets"

    async def _get_market_carbon_price(
        self,
        market_key: str,
        date: datetime
    ) -> Tuple[float, Dict[str, Any]]:
        """Retrieve (and cache) deterministic live carbon price for a market."""
        cache_key = f"live_carbon_price:{market_key}:{date.strftime('%Y%m%d')}"
        cached_entry = await self.cache_manager.get(cache_key)
        if isinstance(cached_entry, dict) and "price" in cached_entry:
            price_value = float(cached_entry["price"])
            metadata = cached_entry.get("metadata", {})
            metadata.setdefault("currency", self._carbon_markets.get(market_key, {}).get("currency", "USD"))
            return price_value, metadata

        price_value, metadata = self._compute_market_price(market_key, date)

        await self.cache_manager.set(
            cache_key,
            {"price": price_value, "metadata": metadata},
            ttl_seconds=600
        )

        return price_value, metadata

    def _compute_market_price(
        self,
        market_key: str,
        date: datetime
    ) -> Tuple[float, Dict[str, Any]]:
        """Deterministically compute market price using seasonal and policy signals."""
        market_info = self._carbon_markets.get(market_key, {})
        price_min_local, price_max_local = market_info.get("price_range", (20.0, 90.0))
        currency = market_info.get("currency", "USD")
        fx_rate = 1.07 if currency == "EUR" else 1.0

        base_price_local = (price_min_local + price_max_local) / 2

        day_of_year = date.timetuple().tm_yday
        seasonal_component = 0.07 * math.sin(2 * math.pi * day_of_year / 365)
        weekly_component = 0.025 * math.sin(2 * math.pi * day_of_year / 52)

        drift_rate = self._market_trends.get(market_key, 0.0)
        year_progress = (date - datetime(date.year, 1, 1)).days / 365
        regulatory_drift = drift_rate * year_progress

        shock_adjustment = 0.0
        for event in self._market_shocks.get(market_key, []):
            if event["start"] <= date <= event["end"]:
                shock_adjustment += event.get("impact", 0.0)

        derived_price_local = base_price_local * (
            1 + seasonal_component + weekly_component + regulatory_drift + shock_adjustment
        )
        bounded_local = max(price_min_local, min(price_max_local, derived_price_local))
        price_usd = bounded_local * fx_rate

        metadata = {
            "seasonal_component": seasonal_component,
            "weekly_component": weekly_component,
            "regulatory_drift": regulatory_drift,
            "shock_adjustment": shock_adjustment,
            "currency": currency,
            "fx_rate_to_usd": fx_rate,
            "price_local": bounded_local,
            "source_market": market_key
        }

        return price_usd, metadata

    def _calculate_asset_carbon_intensity(
        self,
        asset_profile: Dict[str, Any],
        market_key: str,
        date: datetime
    ) -> float:
        """Calculate adjusted carbon intensity for an asset."""
        baseline_intensity = asset_profile.get("baseline_intensity", 0.35)
        abatement_factor = asset_profile.get("abatement_factor", 0.0)
        asset_type = asset_profile.get("asset_type", "generic")

        day_of_year = date.timetuple().tm_yday

        if asset_type == "wind":
            seasonal_adjustment = 0.08 * math.sin(2 * math.pi * (day_of_year + 30) / 365)
        elif asset_type == "solar":
            seasonal_adjustment = 0.10 * math.cos(2 * math.pi * (day_of_year - 172) / 365)
        elif asset_type == "gas":
            seasonal_adjustment = 0.05 * math.sin(2 * math.pi * day_of_year / 365)
        else:
            seasonal_adjustment = 0.04 * math.sin(2 * math.pi * (day_of_year - 45) / 365)

        regulatory_relief = max(0.75, 1 - self._market_trends.get(market_key, 0.0) * (day_of_year / 365))
        technology_factor = max(0.2, 1 - abatement_factor)
        seasonal_factor = 1 + seasonal_adjustment

        adjusted_intensity = baseline_intensity * technology_factor * seasonal_factor * regulatory_relief
        return max(0.01, round(adjusted_intensity, 4))

    def _estimate_market_volume(
        self,
        market_key: str,
        date: datetime,
        price_shock: float = 0.0
    ) -> int:
        """Estimate market trading volume using deterministic seasonal patterns."""
        market_info = self._carbon_markets.get(market_key, {})
        base_volume = market_info.get("avg_daily_volume", 750_000)

        day_of_year = date.timetuple().tm_yday
        seasonal_component = 0.12 * math.cos(2 * math.pi * day_of_year / 365)
        weekly_component = 0.05 * math.sin(2 * math.pi * day_of_year / 7)
        shock_multiplier = 1 + (price_shock * 0.6)

        derived_volume = base_volume * (1 + seasonal_component + weekly_component) * shock_multiplier
        return int(max(50_000, round(derived_volume)))

    def _build_scenario_impacts(
        self,
        scenario_id: str,
        price_samples: Sequence[float]
    ) -> Dict[str, float]:
        """Create scenario impact mapping based on observed price levels."""
        scenario_impacts: Dict[str, float] = {"baseline": 0.0}

        if not price_samples:
            scenario_impacts[scenario_id] = 0.0
            scenario_impacts["policy_shock"] = 0.18
            scenario_impacts["technology_breakthrough"] = -0.12
            return scenario_impacts

        avg_price = statistics.mean(price_samples)
        volatility = statistics.pstdev(price_samples) if len(price_samples) > 1 else avg_price * 0.05
        normalized_vol = volatility / max(avg_price, 1.0)

        scenario_key = scenario_id.lower()
        if any(keyword in scenario_key for keyword in ("high", "stress", "tight")):
            impact = min(0.55, 0.22 + normalized_vol * 2.6)
        elif any(keyword in scenario_key for keyword in ("low", "glut", "bear")):
            impact = -min(0.35, 0.16 + normalized_vol * 1.4)
        elif any(keyword in scenario_key for keyword in ("net", "transition", "policy")):
            impact = min(0.45, 0.28 + normalized_vol * 1.8)
        else:
            impact = min(0.30, 0.18 + normalized_vol * 1.2) if avg_price else 0.0

        scenario_impacts[scenario_id] = round(impact, 3)

        policy_shock = min(0.48, 0.20 + normalized_vol * 2.1)
        tech_breakthrough = -min(0.20, 0.10 + normalized_vol)
        demand_shock = min(0.32, 0.14 + normalized_vol * 1.7)

        scenario_impacts.setdefault("policy_shock", round(policy_shock, 3))
        scenario_impacts.setdefault("technology_breakthrough", round(tech_breakthrough, 3))
        scenario_impacts.setdefault("demand_shock", round(demand_shock, 3))

        return scenario_impacts

    def _normalize_rec_geography(self, geography: str) -> str:
        """Normalize geography identifiers to REC market keys."""
        key = geography.lower().replace(" ", "_")
        mapping = {
            "ca": "california",
            "caiso": "california",
            "california": "california",
            "tx": "texas",
            "ercot": "texas",
            "texas": "texas",
            "pjm": "northeast",
            "new_jersey": "northeast",
            "northeast": "northeast",
            "miso": "midwest",
            "midwest": "midwest",
            "illinois": "midwest",
            "rec_markets": "rec_default"
        }
        return mapping.get(key, key if key in self._rec_market_profiles else "rec_default")

    def _generate_rec_trading_record(
        self,
        region_key: str,
        date: datetime
    ) -> RECTrading:
        """Generate deterministic REC trading record for a given day."""
        profile = self._rec_market_profiles.get(region_key, self._rec_market_profiles["rec_default"])
        day_of_year = date.timetuple().tm_yday

        seasonal_component = 0.18 * math.sin(2 * math.pi * (day_of_year - 45) / 365)
        weekly_component = 0.06 * math.cos(2 * math.pi * day_of_year / 30)
        compliance_boost = 0.0
        if date.month in profile.get("compliance_months", []) and date.day >= 20:
            compliance_boost = 0.12

        clearing_price = profile["base_price"] * (
            1 + seasonal_component + weekly_component + compliance_boost
        )
        quantity_mwh = profile["base_quantity"] * (
            1 + 0.25 * math.cos(2 * math.pi * day_of_year / 180)
        )

        sources = profile.get("preferred_sources", ["solar"])
        generation_source = sources[day_of_year % len(sources)]

        owner_cycle = [
            "portfolio_123",
            "portfolio_transition",
            "utility_green_supply",
            "corporate_ppas"
        ]
        current_owner = owner_cycle[(day_of_year + len(region_key)) % len(owner_cycle)]

        if compliance_boost > 0.0:
            status = "retired"
        elif day_of_year % 7 == 0:
            status = "traded"
        else:
            status = "issued"

        rec_hash = hashlib.sha1(f"{region_key}:{date.strftime('%Y%m%d')}".encode("utf-8")).hexdigest()[:12]
        rec_id = f"rec_{rec_hash}"

        trading_history: List[Dict[str, Any]] = [
            {
                "timestamp": date,
                "action": "issued",
                "price": round(clearing_price, 2),
                "quantity": round(quantity_mwh, 2)
            }
        ]

        trade_timestamp = date + timedelta(days=2)
        trade_price = clearing_price * (1 + 0.04 * math.sin(day_of_year / 12))
        if status in {"traded", "retired"}:
            trading_history.append(
                {
                    "timestamp": trade_timestamp,
                    "action": "traded",
                    "price": round(trade_price, 2),
                    "quantity": round(quantity_mwh * 0.85, 2)
                }
            )

        if status == "retired":
            trading_history.append(
                {
                    "timestamp": trade_timestamp + timedelta(days=5),
                    "action": "retired",
                    "price": round(trade_price * 1.02, 2),
                    "quantity": round(quantity_mwh * 0.6, 2)
                }
            )

        metadata = {
            "region": profile.get("region"),
            "seasonal_component": seasonal_component,
            "weekly_component": weekly_component,
            "compliance_boost": compliance_boost,
            "clearing_price_usd": round(clearing_price, 2)
        }

        return RECTrading(
            rec_id=rec_id,
            vintage_year=date.year,
            generation_date=date,
            generation_source=generation_source,
            generation_location=profile.get("region", region_key.upper()),
            quantity_mwh=round(quantity_mwh, 2),
            status=status,
            current_owner=current_owner,
            trading_history=trading_history,
            metadata=metadata
        )

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
        holdings = await self._get_portfolio_holdings(portfolio_id)
        if not holdings:
            self.telemetry.warning(
                "No holdings available for portfolio; returning neutral exposure",
                portfolio_id=portfolio_id
            )
            exposure = PortfolioCarbonExposure(
                portfolio_id=portfolio_id,
                analysis_date=analysis_date,
                total_emissions_tons=0.0,
                total_carbon_cost=0.0,
                carbon_intensity=0.0,
                carbon_cost_per_mwh=0.0,
                compliance_obligations={},
                carbon_credits_held={},
                net_carbon_position=0.0,
                risk_metrics={},
                scenario_impact={"baseline": 0.0}
            )
            self._portfolio_exposures[portfolio_id] = exposure
            return exposure

        total_emissions = 0.0
        total_carbon_cost = 0.0
        total_generation = 0.0

        compliance_obligations: Dict[str, float] = defaultdict(float)
        market_price_samples: Dict[str, List[float]] = defaultdict(list)
        price_samples: List[float] = []
        intensity_samples: List[float] = []

        low_carbon_generation = 0.0

        for holding in holdings:
            asset_id = holding.get("asset_id")
            geography = holding.get("geography")
            generation_mwh = float(holding.get("generation_mwh", 0.0))

            if not asset_id or generation_mwh <= 0:
                continue

            asset_profile = self._asset_profiles.get(asset_id, {})
            if not geography:
                geography = asset_profile.get("geography", "eu")

            pricing_series = await self.get_carbon_pricing(
                asset_id, geography, analysis_date, analysis_date
            )

            if not pricing_series:
                continue

            pricing_point = pricing_series[0]
            market_key = self._resolve_market_key(geography)

            asset_emissions = generation_mwh * pricing_point.carbon_intensity
            asset_carbon_cost = generation_mwh * pricing_point.carbon_cost

            total_emissions += asset_emissions
            total_carbon_cost += asset_carbon_cost
            total_generation += generation_mwh

            compliance_obligations[market_key] += asset_emissions
            market_price_samples[market_key].append(pricing_point.carbon_price)
            price_samples.append(pricing_point.carbon_price)
            intensity_samples.append(pricing_point.carbon_intensity)

            if pricing_point.carbon_intensity <= 0.05:
                low_carbon_generation += generation_mwh

        carbon_credits = self._carbon_credits_held.get(portfolio_id, {})
        carbon_credits_rounded = {
            market: int(round(value)) for market, value in carbon_credits.items()
        }

        for market in carbon_credits.keys():
            compliance_obligations.setdefault(market, 0.0)

        net_positions: Dict[str, float] = {}
        for market, obligation in compliance_obligations.items():
            net_positions[market] = obligation - carbon_credits.get(market, 0.0)

        for market, credits in carbon_credits.items():
            if market not in net_positions:
                net_positions[market] = -credits

        net_carbon_position = sum(net_positions.values())

        carbon_intensity = (
            total_emissions / total_generation if total_generation else 0.0
        )
        carbon_cost_per_mwh = (
            total_carbon_cost / total_generation if total_generation else 0.0
        )

        avg_price = statistics.mean(price_samples) if price_samples else 0.0
        price_volatility = (
            statistics.pstdev(price_samples) if len(price_samples) > 1 else 0.0
        )

        high_price_obligations = 0.0
        for market, obligation in compliance_obligations.items():
            price_list = market_price_samples.get(market, [])
            if price_list and statistics.mean(price_list) > 60:
                high_price_obligations += obligation

        high_price_share = (
            high_price_obligations / total_emissions if total_emissions else 0.0
        )

        intensity_volatility = (
            statistics.pstdev(intensity_samples) if len(intensity_samples) > 1 else 0.0
        )

        renewable_share = (
            low_carbon_generation / total_generation if total_generation else 0.0
        )

        risk_metrics = {
            "carbon_price_volatility": round(price_volatility, 4),
            "average_market_price": round(avg_price, 2),
            "compliance_gap_tons": round(net_carbon_position, 2),
            "high_price_market_share": round(high_price_share, 4),
            "intensity_volatility": round(intensity_volatility, 5),
            "renewable_generation_share": round(renewable_share, 4)
        }

        scenario_impact = self._build_scenario_impacts(scenario_id, price_samples)

        exposure = PortfolioCarbonExposure(
            portfolio_id=portfolio_id,
            analysis_date=analysis_date,
            total_emissions_tons=round(total_emissions, 2),
            total_carbon_cost=round(total_carbon_cost, 2),
            carbon_intensity=round(carbon_intensity, 5),
            carbon_cost_per_mwh=round(carbon_cost_per_mwh, 4),
            compliance_obligations={
                market: round(value, 2) for market, value in compliance_obligations.items()
            },
            carbon_credits_held=carbon_credits_rounded,
            net_carbon_position=round(net_carbon_position, 2),
            risk_metrics=risk_metrics,
            scenario_impact=scenario_impact
        )

        self._portfolio_exposures[portfolio_id] = exposure

        cache_key = (
            f"portfolio_exposure:{portfolio_id}:{scenario_id}:{analysis_date.strftime('%Y%m%d')}"
        )
        await self.cache_manager.set(cache_key, exposure.dict(), ttl_seconds=900)

        self.telemetry.info(
            "Portfolio carbon exposure calculated",
            portfolio_id=portfolio_id,
            scenario_id=scenario_id,
            analysis_date=analysis_date.isoformat(),
            total_emissions=exposure.total_emissions_tons,
            net_position=exposure.net_carbon_position
        )

        return exposure

    async def get_rec_trading_data(
        self,
        geography: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[RECTrading]:
        """Get REC trading data for geography and time period."""
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        region_key = self._normalize_rec_geography(geography)
        cache_key = (
            f"rec_trading:{region_key}:{start_date.strftime('%Y%m%d')}:{end_date.strftime('%Y%m%d')}"
        )

        cached_records = await self.cache_manager.get(cache_key)
        if isinstance(cached_records, list) and cached_records:
            return [
                RECTrading(**record) if isinstance(record, dict) else record
                for record in cached_records
            ]

        trading_series: List[RECTrading] = []

        for day in range((end_date - start_date).days + 1):
            observation_date = start_date + timedelta(days=day)
            trading_record = self._generate_rec_trading_record(region_key, observation_date)
            trading_series.append(trading_record)

        existing_ids = {rec.rec_id for rec in self._rec_registry[region_key]}
        for record in trading_series:
            if record.rec_id not in existing_ids:
                self._rec_registry[region_key].append(record)

        await self.cache_manager.set(
            cache_key,
            [record.dict() for record in trading_series],
            ttl_seconds=900
        )

        total_volume = sum(rec.quantity_mwh for rec in trading_series)
        avg_price = statistics.mean(
            rec.metadata.get("clearing_price_usd", 0.0) for rec in trading_series
        ) if trading_series else 0.0

        self.telemetry.info(
            "REC trading series generated",
            geography=geography,
            region_key=region_key,
            records=len(trading_series),
            total_volume=round(total_volume, 2),
            average_price=round(avg_price, 2)
        )

        self.telemetry.record_gauge(
            "rec_trading_volume_mwh",
            value=total_volume,
            category=MetricCategory.BUSINESS,
            market=region_key
        )

        return trading_series

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
        volumes: List[int] = []

        for day in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=day)
            price_usd, price_metadata = await self._get_market_carbon_price(market.value, date)
            volume = self._estimate_market_volume(market.value, date, price_metadata.get("shock_adjustment", 0.0))

            data_points.append({
                "date": date,
                "price_usd": price_usd,
                "price_local": price_metadata.get("price_local"),
                "currency": price_metadata.get("currency", market_info.get("currency", "USD")),
                "volume": volume,
                "metadata": {
                    "seasonal_component": price_metadata.get("seasonal_component"),
                    "weekly_component": price_metadata.get("weekly_component"),
                    "regulatory_drift": price_metadata.get("regulatory_drift"),
                    "shock_adjustment": price_metadata.get("shock_adjustment"),
                    "fx_rate_to_usd": price_metadata.get("fx_rate_to_usd")
                }
            })
            volumes.append(volume)

        return {
            "market": market.value,
            "market_name": market_info.get("name", "Unknown"),
            "data": data_points,
            "summary": {
                "avg_price_usd": statistics.mean([p["price_usd"] for p in data_points]) if data_points else 0,
                "price_volatility": statistics.pstdev([p["price_usd"] for p in data_points]) if len(data_points) > 1 else 0,
                "total_volume": sum(volumes),
                "avg_volume": statistics.mean(volumes) if volumes else 0
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
