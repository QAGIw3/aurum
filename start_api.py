#!/usr/bin/env python3
"""Canonical launcher for the Aurum API (unified app).

Boots `apps.api.main:create_app` under uvicorn.
"""

import os
import uvicorn


def main() -> None:
    host = os.getenv("AURUM_API_HOST", "0.0.0.0")
    port = int(os.getenv("AURUM_API_PORT", "8080"))
    workers = int(os.getenv("AURUM_API_WORKERS", "1"))
    uvicorn.run(
        "apps.api.main:create_app",
        host=host,
        port=port,
        workers=workers,
        factory=True,
    )


if __name__ == "__main__":
    main()
