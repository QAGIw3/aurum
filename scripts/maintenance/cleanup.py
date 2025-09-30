import os
import subprocess
import sys
from pathlib import Path


def run_command(command: list[str], cwd: str | None = None, allow_fail: bool = False) -> int:
    result = subprocess.run(command, cwd=cwd, text=True)
    if result.returncode != 0 and not allow_fail:
        sys.exit(result.returncode)
    return result.returncode


def ensure_reports_dir() -> Path:
    reports = Path("reports")
    reports.mkdir(parents=True, exist_ok=True)
    return reports


def main() -> None:
    reports = ensure_reports_dir()

    # 1) Remove unused imports/variables and stray commented-out code
    run_command([
        sys.executable,
        "-m",
        "ruff",
        "check",
        "src/",
        "tests/",
        "--select",
        "F401,F841,ERA001,RUF100",
        "--fix",
    ], allow_fail=True)

    # 2) Sort imports and format
    run_command(["isort", "src/", "tests/"], allow_fail=True)
    run_command(["black", "src/", "tests/"], allow_fail=True)

    # 3) Dead fixtures report
    with open(reports / "deadfixtures.txt", "w", encoding="utf-8") as fh:
        subprocess.run([
            "pytest",
            "-q",
            "--dead-fixtures",
            "--maxfail=1",
        ], stdout=fh, stderr=subprocess.STDOUT, text=True)

    # 4) Vulture dead code report
    with open(reports / "vulture.txt", "w", encoding="utf-8") as fh:
        subprocess.run([
            "vulture",
            "src/",
            "--min-confidence",
            "80",
            "--sort-by-size",
        ], stdout=fh, stderr=subprocess.STDOUT, text=True)

    print("✅ Maintenance cleanup completed. See reports/ for outputs.")


if __name__ == "__main__":
    main()


