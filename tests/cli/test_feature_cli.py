from __future__ import annotations

import io
from typing import Any, Dict

import pytest

from aurum.cli import feature


@pytest.fixture(autouse=True)
def reset_defaults(monkeypatch):
    """Reset module-level defaults between tests."""
    monkeypatch.delenv("AURUM_API_BASE_URL", raising=False)
    monkeypatch.delenv("AURUM_API_TOKEN", raising=False)


def test_main_uses_env_base_url(monkeypatch, capsys):
    monkeypatch.setenv("AURUM_API_BASE_URL", "https://api.example.com")

    def stub_request(_session, *, base_url: str, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        assert base_url == "https://api.example.com"
        assert method == "GET"
        assert path == "/v1/admin/features"
        return {"items": []}

    monkeypatch.setattr(feature, "_request", stub_request)
    monkeypatch.setattr(feature, "_build_session", lambda token: feature.requests.Session())

    exit_code = feature.main(["list"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "items" in captured.out


def test_main_respects_cli_base_url(monkeypatch):
    called = {}

    def stub_request(_session, *, base_url: str, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        called["base_url"] = base_url
        return {"items": []}

    monkeypatch.setattr(feature, "_request", stub_request)
    monkeypatch.setattr(feature, "_build_session", lambda token: feature.requests.Session())

    exit_code = feature.main(["list", "--base-url", "https://override.example.com"])
    assert exit_code == 0
    assert called["base_url"] == "https://override.example.com"


def test_main_returns_error_code_on_failure(monkeypatch, capsys):
    def stub_request(*args, **kwargs):
        raise feature.FeatureCLIError("boom")

    monkeypatch.setattr(feature, "_request", stub_request)
    monkeypatch.setattr(feature, "_build_session", lambda token: feature.requests.Session())

    exit_code = feature.main(["list"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "boom" in captured.err
