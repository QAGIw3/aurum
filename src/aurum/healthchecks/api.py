from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = "8080"
_DEFAULT_PATH = "/health"


def _health_url() -> str:
    host = os.getenv("AURUM_API_HEALTH_HOST", _DEFAULT_HOST).strip() or _DEFAULT_HOST
    port = os.getenv("AURUM_API_HEALTH_PORT", _DEFAULT_PORT).strip() or _DEFAULT_PORT
    path = os.getenv("AURUM_API_HEALTH_PATH", _DEFAULT_PATH).strip() or _DEFAULT_PATH
    if not path.startswith("/"):
        path = "/" + path
    return f"http://{host}:{port}{path}"


def main() -> int:
    url = _health_url()
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            if 200 <= response.status < 400:
                return 0
            sys.stderr.write(f"unhealthy status code from {url}: {response.status}\n")
            return 1
    except urllib.error.URLError as exc:  # pragma: no cover - network failure handling
        sys.stderr.write(f"error probing {url}: {exc}\n")
        return 1


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    raise SystemExit(main())
