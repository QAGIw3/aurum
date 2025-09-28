import pandas as pd

from aurum.ml import ABTestingService, run_ab_test


def test_run_ab_test_compares_models():
    y = pd.Series([10, 11, 12, 13, 14])
    a = pd.Series([10.1, 10.9, 12.1, 13.1, 13.9])
    b = pd.Series([9.5, 11.2, 12.5, 13.6, 14.7])
    result = run_ab_test(y, a, b)
    assert result.winner in {"A", "B", "tie"}
    assert any(k.endswith("rmse") for k in result.metrics.keys())


def test_ab_testing_service_record_metrics():
    service = ABTestingService()
    champion_metrics = {"rmse": 1.2, "mape": 0.05}
    challenger_metrics = {"rmse": 1.0, "mape": 0.04}
    experiment = service.record_metrics(
        "exp1",
        champion_name="model_a",
        challenger_name="model_b",
        champion_metrics=champion_metrics,
        challenger_metrics=challenger_metrics,
    )
    assert experiment.winner == "model_b"
    leaderboard = service.leaderboard()
    assert not leaderboard.empty
    assert "model_a_rmse" in leaderboard.columns

