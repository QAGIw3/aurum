from __future__ import annotations

import importlib
import os
import sys
import urllib.error
import urllib.request
from typing import Optional

_PROBE_PORT_VARS = (
    "AURUM_WORKER_PROBE_PORT",
    "AURUM_SCENARIO_HTTP_PORT",
    "AURUM_SCENARIO_METRICS_PORT",
)
_PROBE_ADDR_VARS = (
    "AURUM_WORKER_PROBE_ADDR",
    "AURUM_SCENARIO_HTTP_ADDR",
    "AURUM_SCENARIO_METRICS_ADDR",
)


def _probe_url() -> Optional[str]:
    port: Optional[str] = None
    for candidate in _PROBE_PORT_VARS:
        value = os.getenv(candidate)
        if value:
            port = value.strip()
            if port:
                break
    if not port:
        return None
    addr = "127.0.0.1"
    for candidate in _PROBE_ADDR_VARS:
        value = os.getenv(candidate)
        if value:
            addr = value.strip() or addr
            break
    path = os.getenv("AURUM_WORKER_PROBE_PATH", "/ready").strip() or "/ready"
    if not path.startswith("/"):
        path = f"/{path}"
    return f"http://{addr}:{port}{path}"


def _probe_http(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return 200 <= response.status < 400
    except urllib.error.URLError as exc:  # pragma: no cover - integration behaviour
        sys.stderr.write(f"error probing {url}: {exc}\n")
        return False


def _import_worker() -> bool:
    try:
        importlib.import_module("aurum.scenarios.worker")
        return True
    except Exception as exc:  # pragma: no cover - import failure logging
        sys.stderr.write(f"unable to import worker module: {exc}\n")
        return False


def main() -> int:
    url = _probe_url()
    if url and _probe_http(url):
        return 0
    if _import_worker():
        return 0
    return 1


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    raise SystemExit(main())
