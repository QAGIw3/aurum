import pytest

from aurum.risk import (
    VaREngine,
    VaRConfig,
    VaRMethod,
    PortfolioInput,
    PositionInput,
)


def _sample_portfolio() -> PortfolioInput:
    return PortfolioInput(
        portfolio_id="T",
        positions=[
            PositionInput(asset_id="A", notional_value=1_000_000, position_type="long", risk_factors={"market": 1.0, "rate": -0.3}),
            PositionInput(asset_id="B", notional_value=750_000, position_type="short", risk_factors={"market": 0.6}),
        ],
    )


def test_var_monotonic_confidence():
    engine = VaREngine()
    pf = _sample_portfolio()
    res95 = engine.calculate_var(pf, VaRConfig(method=VaRMethod.MONTE_CARLO, confidence_level=0.95, num_simulations=5000))
    res99 = engine.calculate_var(pf, VaRConfig(method=VaRMethod.MONTE_CARLO, confidence_level=0.99, num_simulations=5000))
    assert res95.var >= 0
    assert res99.var >= res95.var
    assert res99.cvar >= res99.var


def test_var_parametric_sensible():
    engine = VaREngine()
    pf = _sample_portfolio()
    res = engine.calculate_var(pf, VaRConfig(method=VaRMethod.PARAMETRIC))
    assert res.var >= 0
    assert res.cvar >= res.var
    # Reduce exposure and expect lower VaR
    pf_low = PortfolioInput(
        portfolio_id="T",
        positions=[
            PositionInput(asset_id="A", notional_value=500_000, position_type="long", risk_factors={"market": 0.5, "rate": -0.15}),
            PositionInput(asset_id="B", notional_value=375_000, position_type="short", risk_factors={"market": 0.3}),
        ],
    )
    res_low = engine.calculate_var(pf_low, VaRConfig(method=VaRMethod.PARAMETRIC))
    assert res_low.var <= res.var

