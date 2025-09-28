#!/usr/bin/env python3
"""Run k6 perf script with CI-friendly thresholds and exit codes.

Requires k6 to be installed in the CI image.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys


def main() -> int:
    base_url = os.getenv("AURUM_PERF_BASE_URL", "http://localhost:8000")
    duration = os.getenv("AURUM_PERF_DURATION", "1m")
    curve_rps = os.getenv("AURUM_PERF_CURVE_RPS", "5")
    export_rps = os.getenv("AURUM_PERF_EXPORT_RPS", "1")
    token = os.getenv("AURUM_API_TOKEN", "")

    env = os.environ.copy()
    env.update(
        {
            "AURUM_PERF_BASE_URL": base_url,
            "AURUM_PERF_DURATION": duration,
            "AURUM_PERF_CURVE_RPS": str(curve_rps),
            "AURUM_PERF_EXPORT_RPS": str(export_rps),
            "AURUM_API_TOKEN": token,
        }
    )

    cmd = "k6 run perf/k6/curves.js"
    print(f"Running: {cmd}")
    try:
        proc = subprocess.run(shlex.split(cmd), env=env, check=False)
        return proc.returncode
    except FileNotFoundError:
        print("k6 not found in PATH", file=sys.stderr)
        return 127


if __name__ == "__main__":
    sys.exit(main())


