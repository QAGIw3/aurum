import json
from pathlib import Path

import numpy as np
import pandas as pd

from aurum.ml import FeaturePipelineConfig, ModelRegistry, retrain_best_forecaster


def test_retrain_best_forecaster_registers_model(tmp_path):
    idx = pd.date_range("2024-01-01", periods=120, freq="H")
    values = 30 + 3 * np.sin(np.linspace(0, 6 * np.pi, len(idx)))
    series = pd.Series(values, index=idx)

    registry_path = tmp_path / "models"
    registry = ModelRegistry(base_dir=registry_path)

    outcome = retrain_best_forecaster(
        lambda: series,
        registry=registry,
        model_name="test_energy_forecaster",
        horizon=6,
        initial_train_size=48,
        step=6,
        freq_hint="H",
        feature_config=FeaturePipelineConfig(rolling_windows=(6, 12), lags=(1, 24), include_calendar=False),
        enable_ab_testing=True,
    )

    model_dir = registry_path / outcome.registered_name / outcome.registered_version
    metadata_path = model_dir / "metadata.json"
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text())
    assert metadata["metrics"]
    assert metadata["extra"]["evaluation"]
    if outcome.ab_tests:
        assert metadata["extra"].get("ab_tests")

