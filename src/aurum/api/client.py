"""Lightweight HTTP client used in integration smoke tests."""

from __future__ import annotations

import contextlib
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests


class AurumAPIClient:
    """Minimal wrapper around ``requests`` with sensible defaults.

    The real production client historically pulled in a large stack of
    dependencies.  For tests we only need a tiny facade that can issue HTTP
    calls against a configurable base URL while keeping compatibility with the
    previous public API.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = session or requests.Session()
        self._default_headers = headers or {}

    # ``requests`` sessions are context managers; expose the same surface for
    # drop-in compatibility.
    def __enter__(self) -> "AurumAPIClient":  # pragma: no cover - convenience
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - convenience
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> requests.Response:
        url = urljoin(self.base_url + "/", path.lstrip("/"))
        merged_headers = {**self._default_headers, **(headers or {})}
        response = self._session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json,
            headers=merged_headers,
            timeout=timeout or self.timeout,
        )
        return response

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("DELETE", path, **kwargs)

    @contextlib.contextmanager
    def scoped_headers(self, **headers: str):
        """Temporarily merge default headers for a series of requests."""
        original = self._default_headers.copy()
        try:
            self._default_headers.update(headers)
            yield self
        finally:
            self._default_headers = original


__all__ = ["AurumAPIClient"]
