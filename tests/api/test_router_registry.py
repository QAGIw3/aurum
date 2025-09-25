from __future__ import annotations

import os

import pytest

from aurum.api.router_registry import get_v1_router_specs
from aurum.core import AurumSettings


@pytest.fixture
def reset_split_flags(monkeypatch):
    flags = [
        "AURUM_API_V1_SPLIT_EIA",
        "AURUM_API_V1_SPLIT_ISO",
        "AURUM_API_V1_SPLIT_PPA",
        "AURUM_API_V1_SPLIT_DROUGHT",
        "AURUM_API_V1_SPLIT_ADMIN",
        "AURUM_API_V1_SPLIT_METADATA",
    ]
    for flag in flags:
        monkeypatch.delenv(flag, raising=False)
    yield
    for flag in flags:
        monkeypatch.delenv(flag, raising=False)


def test_v1_router_specs_deduplicates_split_modules(monkeypatch, reset_split_flags):
    monkeypatch.setenv("AURUM_API_V1_SPLIT_PPA", "1")
    settings = AurumSettings()

    specs = get_v1_router_specs(settings)
    ppa_specs = [spec for spec in specs if spec.name == "aurum.api.v1.ppa"]

    assert len(ppa_specs) == 1


def test_v1_router_specs_unique_when_all_flags_enabled(monkeypatch, reset_split_flags):
    for flag in [
        "AURUM_API_V1_SPLIT_EIA",
        "AURUM_API_V1_SPLIT_ISO",
        "AURUM_API_V1_SPLIT_PPA",
        "AURUM_API_V1_SPLIT_DROUGHT",
        "AURUM_API_V1_SPLIT_ADMIN",
        "AURUM_API_V1_SPLIT_METADATA",
    ]:
        monkeypatch.setenv(flag, "1")

    settings = AurumSettings()
    specs = get_v1_router_specs(settings)

    names = [spec.name for spec in specs if spec.name is not None]

    assert len(names) == len(set(names))
