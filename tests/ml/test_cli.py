import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from aurum.ml import cli


def _write_series_csv(tmp_path: Path, filename: str) -> Path:
    idx = pd.date_range("2024-01-01", periods=48, freq="h")
    df = pd.DataFrame({"timestamp": idx, "price": 50 + np.sin(np.linspace(0, 4 * np.pi, len(idx)))})
    path = tmp_path / filename
    df.to_csv(path, index=False)
    return path


def test_cli_train_forecast_and_anomalies(tmp_path, capsys):
    csv_path = _write_series_csv(tmp_path, "train.csv")
    outcome = SimpleNamespace(
        registered_name="test_model",
        registered_version="v1",
        metrics={"rmse": 1.23},
        candidate_name="BestModel",
        timestamp="2024-01-01T00:00:00Z",
        evaluation_report=[{"model": "BestModel", "rmse": 1.23}],
        ab_tests=None,
    )
    with patch("aurum.ml.cli.retrain_best_forecaster", return_value=outcome):
        args = SimpleNamespace(
            csv=str(csv_path),
            column="price",
            time_index="timestamp",
            name="test_model",
            version=None,
            horizon=6,
            initial=24,
            step=6,
            freq="h",
            feature_windows="6,12",
            feature_lags="1,2",
            no_feature_calendar=False,
            no_feature_volatility=False,
            no_ab_test=False,
        )
        cli.cmd_train_forecast(args)
        captured = json.loads(capsys.readouterr().out)
        assert captured["candidate_name"] == "BestModel"

    args_anom = SimpleNamespace(
        csv=str(csv_path),
        column="price",
        time_index="timestamp",
        window=12,
        z=2.5,
        mad_threshold=None,
        cooldown=0,
        mode="adaptive",
    )
    cli.cmd_anomalies(args_anom)
    output = capsys.readouterr().out.strip()
    assert "z_score" in output


def test_cli_forecast_shape_and_volatility(tmp_path, capsys):
    csv_path = _write_series_csv(tmp_path, "forecast.csv")

    class DummyModel:
        def forecast(self, steps: int):
            idx = pd.date_range("2024-03-01", periods=steps, freq="h")
            preds = pd.Series(np.full(steps, 42.0), index=idx)
            return SimpleNamespace(predictions=preds, lower=None, upper=None)

    class DummyRegistry:
        def __init__(self):
            self._model = DummyModel()

        def latest(self, name: str):
            return ("latest", {})

        def load(self, name: str, version: str):
            return self._model

    with patch("aurum.ml.cli.ModelRegistry", DummyRegistry):
        args_forecast = SimpleNamespace(name="foo", version="latest", steps=4)
        cli.cmd_forecast(args_forecast)
        csv_output = capsys.readouterr().out.strip()
        assert "prediction" in csv_output.splitlines()[0]

    args_shape = SimpleNamespace(
        csv=str(csv_path),
        column="price",
        time_index="timestamp",
        window=12,
        horizon=4,
    )
    cli.cmd_shape_forecast(args_shape)
    json_output = json.loads(capsys.readouterr().out)
    assert "predictions" in json_output

    args_vol = SimpleNamespace(csv=str(csv_path), column="price", time_index="timestamp", window=12)
    cli.cmd_volatility(args_vol)
    vol_output = json.loads(capsys.readouterr().out)
    assert "summary" in vol_output
