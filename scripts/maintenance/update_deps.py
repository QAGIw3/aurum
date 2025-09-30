import json
import os
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], allow_fail: bool = False) -> int:
    result = subprocess.run(cmd, text=True)
    if result.returncode != 0 and not allow_fail:
        sys.exit(result.returncode)
    return result.returncode


def ensure_reports() -> Path:
    path = Path("reports")
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    reports = ensure_reports()

    # Rebuild constraints if pip-tools is present, else skip
    if shutil.which("pip-compile"):
        run([
            "pip-compile",
            "--extra=api",
            "--extra=ingest",
            "--extra=quality",
            "--extra=test",
            "--extra=dev",
            "--output-file=constraints/dev.txt",
            "pyproject.toml",
        ])

    # Sync or install updated deps
    if shutil.which("uv"):
        run(["uv", "pip", "install", "-e", ".[quality,test,dev]" ])
    else:
        run([sys.executable, "-m", "pip", "install", "-e", ".[quality,test,dev]"])

    # Vulnerability scans
    run([sys.executable, "-m", "pip_audit", "-f", "json", "-o", str(reports / "pip-audit.json")], allow_fail=True)
    run(["safety", "check", "--full-report", "--json"], allow_fail=True)

    # SBOM
    run(["cyclonedx-py", "--format", "json", "--output", str(reports / "sbom.json")], allow_fail=True)

    print("✅ Dependency update and security checks complete.")


if __name__ == "__main__":
    import shutil
    main()


