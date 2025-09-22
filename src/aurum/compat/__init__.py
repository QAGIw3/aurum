"""Thin compatibility package exposing optional shims.

Currently re‑exports a lightweight proxy for the optional `requests`
dependency so modules may import `aurum.compat.requests` and work in
environments where the real package is not installed (e.g. tooling,
minimal runtime images, or tests).
"""

from .requests import requests, RequestsProxy

__all__ = ["requests", "RequestsProxy"]
