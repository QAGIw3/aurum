from pathlib import Path

import pandas as pd

from aurum.ml import ModelRegistry, NaiveLastValueForecaster


def test_model_registry_roundtrip(tmp_path):
    registry_path = tmp_path / "models"
    registry = ModelRegistry(base_dir=registry_path)
    series = pd.Series([1, 2, 3, 4, 5])
    model = NaiveLastValueForecaster()
    model.fit(series)
    location = registry.save("demo_model", "v1", model, metrics={"rmse": 1.0}, notes="test")
    assert location.exists()
    loaded = registry.load("demo_model", "v1")
    assert isinstance(loaded, NaiveLastValueForecaster)
    metadata = registry.metadata("demo_model", "v1")
    assert metadata.metrics["rmse"] == 1.0
    versions = registry.list_versions("demo_model")
    assert "v1" in versions
    latest = registry.latest("demo_model")
    assert latest is not None and latest[0] == "v1"
