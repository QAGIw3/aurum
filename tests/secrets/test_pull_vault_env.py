from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.secrets.pull_vault_env import build_exports, parse_mapping


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, object]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_parse_mapping() -> None:
    mapping = parse_mapping("secret/data/aurum/eia:api_key=EIA_API_KEY")
    assert mapping.path == "secret/data/aurum/eia"
    assert mapping.key == "api_key"
    assert mapping.env == "EIA_API_KEY"


def test_build_exports_single_path(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "data": {
            "data": {
                "api_key": "123",
                "units": "USD/MWh",
            }
        }
    }

    def fake_get(url: str, headers: dict[str, str], timeout: int):  # type: ignore[override]
        assert "secret/data/aurum/eia" in url
        assert headers["X-Vault-Token"] == "token"
        return DummyResponse(200, payload)

    monkeypatch.setattr("scripts.secrets.pull_vault_env.requests.get", fake_get)

    exports = build_exports(
        "http://vault:8200",
        "token",
        [
            parse_mapping("secret/data/aurum/eia:api_key=EIA_API_KEY"),
            parse_mapping("secret/data/aurum/eia:units=EIA_UNITS"),
        ],
        format_="shell",
    )

    assert exports == ["export EIA_API_KEY=123", "export EIA_UNITS=USD/MWh"]


def test_build_exports_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"data": {"data": {"token": "abc"}}}

    def fake_get(url: str, headers: dict[str, str], timeout: int):  # type: ignore[override]
        return DummyResponse(200, payload)

    monkeypatch.setattr("scripts.secrets.pull_vault_env.requests.get", fake_get)

    with pytest.raises(RuntimeError) as exc:
        build_exports(
            "http://vault", "token", [parse_mapping("secret/data/noaa:missing=NOAA")], format_="env"
        )
    assert "missing" in str(exc.value)
