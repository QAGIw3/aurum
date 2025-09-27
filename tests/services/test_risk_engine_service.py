import importlib
import math
from types import SimpleNamespace

import numpy as np
import pytest
import pytest_asyncio

metrics_module = importlib.import_module("aurum.observability.metrics")

if not hasattr(metrics_module, "get_metrics_client"):
    def _noop_metrics_client(*args, **kwargs):
        return SimpleNamespace(
            emit=lambda *a, **k: None,
            record=lambda *a, **k: None,
            gauge=lambda *a, **k: None,
        )

    metrics_module.get_metrics_client = _noop_metrics_client  # type: ignore[attr-defined]


from aurum.api.services.risk_engine_service import (
    MonteCarloConfig,
    PortfolioPosition,
    RiskEngineService,
    RiskScenario,
)


PORTFOLIO_ID = "test-portfolio"


@pytest_asyncio.fixture
async def populated_service() -> RiskEngineService:
    service = RiskEngineService()
    service.set_random_seed(1234)

    positions = [
        PortfolioPosition(
            asset_id="asset_alpha",
            position_type="long",
            notional_value=1_000_000,
            risk_factors={"energy_price": 0.8, "demand": 0.3},
            metadata={},
        ),
        PortfolioPosition(
            asset_id="asset_beta",
            position_type="short",
            notional_value=600_000,
            risk_factors={"energy_price": 0.4, "weather": 0.5},
            metadata={},
        ),
        PortfolioPosition(
            asset_id="asset_gamma",
            position_type="long",
            notional_value=750_000,
            risk_factors={"demand": 0.7, "weather": 0.2},
            metadata={},
        ),
    ]

    for position in positions:
        await service.add_portfolio_position(PORTFOLIO_ID, position)

    return service


@pytest.mark.asyncio
async def test_calculate_portfolio_var_produces_rich_metrics(populated_service: RiskEngineService) -> None:
    config = MonteCarloConfig(num_simulations=1500, random_seed=42)

    result = await populated_service.calculate_portfolio_var(
        PORTFOLIO_ID,
        confidence_level=0.95,
        time_horizon_days=5,
        num_simulations=1500,
        monte_carlo_config=config,
    )

    risk_metrics = result.risk_metrics

    assert risk_metrics["var"] >= 0
    assert risk_metrics["cvar"] >= risk_metrics["var"]
    assert risk_metrics["volatility"] > 0
    assert "factor_contributions" in result.parameters
    assert any(abs(v) > 1e-6 for v in result.parameters["factor_contributions"].values())
    assert isinstance(result.confidence_intervals["var_95"], tuple)
    assert len(result.confidence_intervals["var_95"]) == 2


@pytest.mark.asyncio
async def test_scenario_analysis_increases_tail_risk(populated_service: RiskEngineService) -> None:
    config = MonteCarloConfig(num_simulations=1200, random_seed=77)

    base_result = await populated_service.calculate_portfolio_var(
        PORTFOLIO_ID,
        confidence_level=0.95,
        time_horizon_days=3,
        num_simulations=1200,
        monte_carlo_config=config,
    )

    stress_scenario = RiskScenario(
        scenario_id="stress_energy",
        scenario_name="Energy Shock",
        description="Energy price spike with high correlation",
        probability=0.3,
        risk_factors={"energy_price": 1.8, "demand": -0.2},
        correlation_shocks={"energy_price": 0.5},
        duration_days=7,
        market_impact="high",
    )

    scenario_results = await populated_service.run_risk_scenario_analysis(
        PORTFOLIO_ID,
        scenarios=[stress_scenario],
        num_simulations=1200,
        monte_carlo_config=config,
    )

    scenario_metrics = scenario_results["scenario_results"]["stress_energy"]

    assert scenario_metrics["var_95"] >= base_result.risk_metrics["var"]
    assert scenario_metrics["cvar_95"] >= base_result.risk_metrics["cvar"]
    assert math.isclose(scenario_metrics["probability"], 0.3)


@pytest.mark.asyncio
async def test_aggregation_and_correlations_use_simulation_cache(populated_service: RiskEngineService) -> None:
    config = MonteCarloConfig(num_simulations=1000, random_seed=17)

    var_result = await populated_service.calculate_portfolio_var(
        PORTFOLIO_ID,
        confidence_level=0.95,
        time_horizon_days=2,
        num_simulations=1000,
        monte_carlo_config=config,
    )

    aggregation = await populated_service.calculate_portfolio_aggregation(PORTFOLIO_ID)

    assert aggregation.total_var == pytest.approx(var_result.risk_metrics["var"], rel=0.2)
    assert aggregation.total_cvar >= aggregation.total_var
    assert aggregation.diversification_benefit >= 0
    assert len(aggregation.asset_breakdown) == 3

    correlations = await populated_service.calculate_portfolio_correlations(PORTFOLIO_ID)

    assert correlations["factor_correlation_matrix"]
    if correlations["position_correlation_matrix"] is not None:
        matrix = correlations["position_correlation_matrix"]
        assert len(matrix) == len(aggregation.asset_breakdown)


@pytest.mark.asyncio
async def test_scenario_comparison_metrics_capture_weighting(populated_service: RiskEngineService) -> None:
    config = MonteCarloConfig(num_simulations=900, random_seed=5)

    mild = RiskScenario(
        scenario_id="mild",
        scenario_name="Mild Demand Dip",
        description="Slight demand softness",
        probability=0.7,
        risk_factors={"demand": -0.15},
        correlation_shocks={"demand": -0.2},
        duration_days=3,
        market_impact="moderate",
    )

    severe = RiskScenario(
        scenario_id="severe",
        scenario_name="Severe Energy Spike",
        description="Energy supply disruption",
        probability=0.3,
        risk_factors={"energy_price": 2.0, "weather": 0.5},
        correlation_shocks={"energy_price": 0.6},
        duration_days=10,
        market_impact="extreme",
    )

    results = await populated_service.run_risk_scenario_analysis(
        PORTFOLIO_ID,
        scenarios=[mild, severe],
        num_simulations=900,
        monte_carlo_config=config,
    )

    comparison = results["comparison_metrics"]
    assert comparison["scenario_count"] == 2

    mild_var = results["scenario_results"]["mild"]["var_95"]
    severe_var = results["scenario_results"]["severe"]["var_95"]

    weighted_var = comparison["weighted_var_95"]
    assert min(mild_var, severe_var) <= weighted_var <= max(mild_var, severe_var)
    assert comparison["worst_case_loss"] >= comparison["weighted_var_95"]


@pytest.mark.asyncio
async def test_correlation_matrix_is_positive_definite(populated_service: RiskEngineService) -> None:
    config = MonteCarloConfig(num_simulations=800, random_seed=10)
    await populated_service.calculate_portfolio_var(
        PORTFOLIO_ID,
        confidence_level=0.95,
        time_horizon_days=4,
        num_simulations=800,
        monte_carlo_config=config,
    )

    correlations = await populated_service.calculate_portfolio_correlations(PORTFOLIO_ID)
    factor_corr = np.array(correlations["factor_correlation_matrix"])

    assert factor_corr.shape[0] == factor_corr.shape[1] > 0
    assert np.allclose(factor_corr, factor_corr.T, atol=1e-10)
    assert np.allclose(np.diag(factor_corr), np.ones(factor_corr.shape[0]), atol=1e-8)

    eigenvalues = np.linalg.eigvalsh(factor_corr)
    assert np.all(eigenvalues >= -1e-6)


@pytest.mark.asyncio
async def test_aggregation_without_prior_var_runs_simulation() -> None:
    service = RiskEngineService()
    service.set_random_seed(321)

    positions = [
        PortfolioPosition(
            asset_id="asset_alpha",
            position_type="long",
            notional_value=900_000,
            risk_factors={"energy_price": 0.6, "demand": 0.2},
            metadata={},
        ),
        PortfolioPosition(
            asset_id="asset_delta",
            position_type="short",
            notional_value=450_000,
            risk_factors={"weather": 0.4},
            metadata={},
        ),
    ]

    for position in positions:
        await service.add_portfolio_position(PORTFOLIO_ID, position)

    aggregation = await service.calculate_portfolio_aggregation(PORTFOLIO_ID)

    assert aggregation.total_var >= 0
    assert aggregation.total_cvar >= aggregation.total_var
    assert aggregation.total_volatility >= 0
    assert set(aggregation.asset_breakdown.keys()) == {"asset_alpha", "asset_delta"}
    assert aggregation.asset_breakdown["asset_delta"]["position_direction"] == -1.0
