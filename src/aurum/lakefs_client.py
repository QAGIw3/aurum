"""Minimal lakeFS client for orchestration hooks."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    requests = None


@dataclass
class LakeFSConfig:
    endpoint: str
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls) -> "LakeFSConfig":
        endpoint = os.getenv("AURUM_LAKEFS_ENDPOINT", "http://lakefs:8000/api/v1")
        access_key = os.getenv("AURUM_LAKEFS_ACCESS_KEY")
        secret_key = os.getenv("AURUM_LAKEFS_SECRET_KEY")
        if not access_key or not secret_key:
            raise RuntimeError("lakeFS credentials not provided in environment")
        return cls(endpoint=endpoint.rstrip("/"), access_key=access_key, secret_key=secret_key)


class LakeFSClient:
    def __init__(self, config: Optional[LakeFSConfig] = None) -> None:
        self.config = config or LakeFSConfig.from_env()
        if requests is None:
            raise RuntimeError("requests package is required for lakeFS interactions")
        self.session = requests.Session()
        self.session.auth = (self.config.access_key, self.config.secret_key)
        self.session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.config.endpoint}{path}"

    def branch_exists(self, repo: str, branch: str) -> bool:
        resp = self.session.get(self._url(f"/repositories/{repo}/branches/{branch}"))
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    def create_branch(self, repo: str, branch: str, source: str) -> None:
        if self.branch_exists(repo, branch):
            return
        payload = {"name": branch, "source": source}
        resp = self.session.post(self._url(f"/repositories/{repo}/branches"), data=json.dumps(payload))
        resp.raise_for_status()

    def commit(self, repo: str, branch: str, message: str, metadata: Optional[Dict[str, str]] = None) -> str:
        payload: Dict[str, Any] = {
            "message": message,
        }
        if metadata:
            payload["metadata"] = metadata
        resp = self.session.post(
            self._url(f"/repositories/{repo}/branches/{branch}/commits"),
            data=json.dumps(payload),
        )
        resp.raise_for_status()
        return resp.json()["id"]

    def create_tag(self, repo: str, tag: str, ref: str) -> None:
        payload = {"id": ref}
        resp = self.session.post(self._url(f"/repositories/{repo}/tags/{tag}"), data=json.dumps(payload))
        resp.raise_for_status()

    def create_pull_request(
        self,
        repo: str,
        source: str,
        target: str,
        title: str,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "source": source,
            "destination": target,
            "title": title,
        }
        if description:
            payload["description"] = description
        resp = self.session.post(self._url(f"/repositories/{repo}/pull-requests"), data=json.dumps(payload))
        resp.raise_for_status()
        return resp.json()

    def merge_pull_request(self, repo: str, pr_number: int) -> None:
        resp = self.session.post(self._url(f"/repositories/{repo}/pull-requests/{pr_number}/merge"))
        resp.raise_for_status()


def ensure_branch(repo: str, branch: str, source: str = "main") -> None:
    client = LakeFSClient()
    client.create_branch(repo, branch, source)


def commit_branch(repo: str, branch: str, message: str, metadata: Optional[Dict[str, str]] = None) -> str:
    client = LakeFSClient()
    return client.commit(repo, branch, message, metadata)


def tag_commit(repo: str, tag: str, ref: str) -> None:
    client = LakeFSClient()
    client.create_tag(repo, tag, ref)


def open_pull_request(
    repo: str,
    *,
    source: str,
    target: str,
    title: str,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    client = LakeFSClient()
    return client.create_pull_request(repo, source, target, title, description)


def merge_pull_request(repo: str, pr_number: int) -> None:
    client = LakeFSClient()
    client.merge_pull_request(repo, pr_number)
