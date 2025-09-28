import pytest

from aurum.risk import (
    VaREngine,
    VaRConfig,
    VaRMethod,
    PortfolioInput,
    PositionInput,
    StressTester,
    StressScenario,
    default_scenarios,
)


def _portfolio() -> PortfolioInput:
    return PortfolioInput(
        portfolio_id="P",
        positions=[
            PositionInput(asset_id="ELEC", notional_value=1_500_000, position_type="long", risk_factors={"market": 1.0, "rate": -0.2}),
            PositionInput(asset_id="NG", notional_value=800_000, position_type="short", risk_factors={"market": 0.5}),
        ],
    )


def test_scenario_pnl_direction():
    engine = VaREngine()
    pf = _portfolio()
    # Positive market shock should hurt a short position and help a long; overall sign depends on net exposure
    pnl_up = engine.scenario_pnl(pf, {"market": 0.02})
    pnl_down = engine.scenario_pnl(pf, {"market": -0.02})
    assert pnl_up == pytest.approx(-pnl_down, rel=1e-6)


def test_stress_volatility_increase_increases_var():
    engine = VaREngine()
    tester = StressTester(engine)
    pf = _portfolio()
    base = engine.calculate_var(pf, VaRConfig(method=VaRMethod.MONTE_CARLO, num_simulations=4000))
    scen = StressScenario(
        scenario_id="vol_up",
        name="Vol Up",
        factor_shocks={},
        volatility_multipliers={"market": 2.0},
        correlation_shift=0.0,
    )
    res = tester.run(pf, [scen], base_var=base)
    assert len(res) == 1
    assert res[0].var_after >= base.var

