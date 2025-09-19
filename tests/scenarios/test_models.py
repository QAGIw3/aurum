import pytest

from aurum.scenarios import DriverType, ScenarioAssumption


def test_scenario_assumption_valid_policy():
    assumption = ScenarioAssumption(
        driver_type=DriverType.POLICY,
        payload={"policy_name": "RPS", "start_year": 2026, "description": "New mandate"},
    )
    assert assumption.driver_type == DriverType.POLICY


def test_scenario_assumption_invalid_payload_raises():
    with pytest.raises(ValueError):
        ScenarioAssumption(driver_type=DriverType.FLEET_CHANGE, payload={"fleet_id": "x"})
