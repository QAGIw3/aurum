import json
import os

import pytest

from aurum import lakefs_client


class DummyResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class DummySession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.headers = {}
        self.auth = ()

    def get(self, url):
        self.calls.append(("GET", url, None))
        return self.responses.get(("GET", url), DummyResponse(404))

    def post(self, url, data=None):
        self.calls.append(("POST", url, data))
        response = self.responses.get(("POST", url))
        if response is None:
            response = DummyResponse(200, {"id": "commit"})
        return response


@pytest.fixture(autouse=True)
def lakefs_env(monkeypatch):
    monkeypatch.setenv("AURUM_LAKEFS_ENDPOINT", "http://lakefs:8000/api/v1")
    monkeypatch.setenv("AURUM_LAKEFS_ACCESS_KEY", "user")
    monkeypatch.setenv("AURUM_LAKEFS_SECRET_KEY", "pass")
    yield
    for key in [
        "AURUM_LAKEFS_ENDPOINT",
        "AURUM_LAKEFS_ACCESS_KEY",
        "AURUM_LAKEFS_SECRET_KEY",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_ensure_branch_creates_when_missing(monkeypatch):
    repo = "demo"
    branch = "eod_20250912"

    responses = {
        ("GET", f"http://lakefs:8000/api/v1/repositories/{repo}/branches/{branch}"): DummyResponse(404),
        ("POST", f"http://lakefs:8000/api/v1/repositories/{repo}/branches"): DummyResponse(201),
    }
    session = DummySession(responses)
    monkeypatch.setattr(lakefs_client, 'requests', type('Req', (), {'Session': staticmethod(lambda: session)}))

    lakefs_client.ensure_branch(repo, branch, "main")

    assert ("POST", f"http://lakefs:8000/api/v1/repositories/{repo}/branches", json.dumps({"name": branch, "source": "main"})) in session.calls


def test_commit_branch_returns_id(monkeypatch):
    repo = "demo"
    branch = "eod_20250912"

    responses = {
        ("POST", f"http://lakefs:8000/api/v1/repositories/{repo}/branches/{branch}/commits"): DummyResponse(200, {"id": "abc123"}),
    }
    session = DummySession(responses)
    monkeypatch.setattr(lakefs_client, 'requests', type('Req', (), {'Session': staticmethod(lambda: session)}))

    commit_id = lakefs_client.commit_branch(repo, branch, "message", {"k": "v"})
    assert commit_id == "abc123"
    assert any(call for call in session.calls if call[0] == "POST" and "commits" in call[1])


def test_create_tag(monkeypatch):
    repo = "demo"
    tag = "release"
    responses = {
        ("POST", f"http://lakefs:8000/api/v1/repositories/{repo}/tags/{tag}"): DummyResponse(201),
    }
    session = DummySession(responses)
    monkeypatch.setattr(lakefs_client, 'requests', type('Req', (), {'Session': staticmethod(lambda: session)}))

    lakefs_client.tag_commit(repo, tag, "abc123")
    assert ("POST", f"http://lakefs:8000/api/v1/repositories/{repo}/tags/{tag}", json.dumps({"id": "abc123"})) in session.calls
