from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from aurum.cli import scenario


def test_parse_assumptions_supports_inline_and_files(tmp_path):
    inline = scenario._parse_assumptions(['{"driver_type":"policy","payload":{"policy_name":"Test"}}'])
    assert inline[0]["driver_type"] == "policy"

    payload = {"driver_type": "policy", "payload": {"policy_name": "File"}}
    path = tmp_path / "assumption.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    from_file = scenario._parse_assumptions([f"@{path}"])
    assert from_file == [payload]


def test_cmd_create_requires_tenant(monkeypatch):
    session = scenario._build_session(token=None)
    args = SimpleNamespace(
        base_url="http://localhost:8095",
        tenant=None,
        name="Example",
        description=None,
        assumption=[],
    )
    with pytest.raises(scenario.ScenarioCLIError):
        scenario._cmd_create(args, session)


def test_request_raises_on_error():
    class DummyResponse:
        status_code = 500
        text = "boom"
        reason = "error"
        content = b""

    class DummySession:
        def request(self, *_args, **_kwargs):
            return DummyResponse()

    with pytest.raises(scenario.ScenarioCLIError):
        scenario._request(
            DummySession(),
            base_url="http://localhost:8095",
            method="GET",
            path="/v1/scenarios",
            tenant=None,
        )
