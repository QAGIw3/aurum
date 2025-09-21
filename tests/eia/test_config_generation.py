from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
SCRIPTS_ROOT = REPO_ROOT / "scripts" / "eia"

sys.path.insert(0, str(SRC_ROOT))

from aurum.reference import eia_catalog  # noqa: E402


@pytest.fixture(scope="module")
def generator_module():
    spec = importlib.util.spec_from_file_location(
        "aurum_generate_ingest_config",
        SCRIPTS_ROOT / "generate_ingest_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def test_generated_config_matches_checked_in(generator_module) -> None:
    datasets = list(eia_catalog.iter_datasets())
    overrides = json.loads((REPO_ROOT / "config" / "eia_ingest_overrides.json").read_text())
    generated = generator_module.generate_config(
        datasets,
        default_schedule="15 6 * * *",
        overrides=overrides,
    )
    current = json.loads((REPO_ROOT / "config" / "eia_ingest_datasets.json").read_text())
    assert current["datasets"] == generated
