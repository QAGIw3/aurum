"""Tests for migration feature flag environment handling."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

import aurum.core.settings as settings


@pytest.fixture(autouse=True)
def _reset_metrics(monkeypatch: pytest.MonkeyPatch):
    """Ensure tests do not persist global metrics state across runs."""

    monkeypatch.setattr(settings, "_migration_metrics", None)
    yield
    monkeypatch.setattr(settings, "_migration_metrics", None)


def _unset_flag(monkeypatch: pytest.MonkeyPatch, flag: str) -> None:
    monkeypatch.delenv(flag, raising=False)
    monkeypatch.delenv(flag.lower(), raising=False)


def test_is_feature_enabled_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    flag = settings.FEATURE_FLAGS["USE_SIMPLIFIED_SETTINGS"]
    _unset_flag(monkeypatch, flag)
    monkeypatch.setenv(flag, "true")

    assert settings.is_feature_enabled(flag) is True


def test_is_feature_enabled_lowercase_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    flag = settings.FEATURE_FLAGS["USE_SIMPLIFIED_SETTINGS"]
    _unset_flag(monkeypatch, flag)
    monkeypatch.setenv(flag.lower(), "true")

    assert settings.is_feature_enabled(flag) is True


def test_get_migration_phase_lowercase_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    flag = settings.FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"]
    _unset_flag(monkeypatch, flag)
    monkeypatch.setenv(flag.lower(), "hybrid")

    stub_metrics = SimpleNamespace(set_migration_phase=lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "_migration_metrics", stub_metrics)

    assert settings.get_migration_phase() == "hybrid"


def test_advance_migration_phase_sets_upper_and_lower(monkeypatch: pytest.MonkeyPatch) -> None:
    flag = settings.FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"]
    _unset_flag(monkeypatch, flag)

    stub_metrics = SimpleNamespace(set_migration_phase=lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "_migration_metrics", stub_metrics)

    settings.advance_migration_phase(phase="simplified")

    assert os.getenv(flag) == "simplified"
    assert os.getenv(flag.lower()) == "simplified"
