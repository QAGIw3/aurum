from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.secrets.push_vault_env import main as push_main


class DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


def test_push_vault_env_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured = []

    def fake_post(url: str, headers: dict[str, str], json: dict, timeout: int):  # type: ignore[override]
        captured.append((url, headers, json))
        return DummyResponse()

    monkeypatch.setattr("scripts.secrets.push_vault_env.requests.post", fake_post)

    monkeypatch.setenv("VAULT_ADDR", "http://vault:8200")
    monkeypatch.setenv("VAULT_TOKEN", "token")
    monkeypatch.setenv("NOAA_TOKEN", "secret1")
    monkeypatch.setenv("EIA_KEY", "secret2")

    argv = [
        "--mapping",
        "NOAA_TOKEN=secret/data/aurum/noaa:token",
        "--mapping",
        "EIA_KEY=secret/data/aurum/eia:api_key",
    ]

    assert push_main(argv) == 0
    assert len(captured) == 2
    assert captured[0][2] == {"data": {"token": "secret1"}}
    assert captured[1][2] == {"data": {"api_key": "secret2"}}


def test_push_vault_env_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VAULT_ADDR", "http://vault")
    monkeypatch.setenv("VAULT_TOKEN", "token")

    with pytest.raises(RuntimeError):
        push_main(["--mapping", "EIA_KEY=secret/data/aurum/eia:api_key"])
