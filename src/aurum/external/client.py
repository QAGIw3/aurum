"""Lightweight HTTP client for external service smoke tests."""

from __future__ import annotations

import contextlib
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests


class ExternalDataClient:
    """Minimal wrapper over ``requests`` tailored for the smoke tests."""

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

    def close(self) -> None:  # pragma: no cover - convenience
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
        return self._session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json,
            headers=merged_headers,
            timeout=timeout or self.timeout,
        )

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> requests.Response:  # pragma: no cover - unused in tests
        return self.request("POST", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:  # pragma: no cover - unused
        return self.request("DELETE", path, **kwargs)

    @contextlib.contextmanager
    def scoped_headers(self, **headers: str):  # pragma: no cover - helper for future tests
        original = self._default_headers.copy()
        try:
            self._default_headers.update(headers)
            yield self
        finally:
            self._default_headers = original


__all__ = ["ExternalDataClient"]
