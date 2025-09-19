from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "kafka" / "bootstrap.sh"


def test_bootstrap_sh_dry_run(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["SCHEMA_REGISTRY_URL"] = "http://registry:8081"
    result = subprocess.run(
        ["bash", str(SCRIPT), "--dry-run"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "[bootstrap] running in dry-run mode" in result.stdout
    assert "[dry-run] would register" in result.stdout
    assert "[dry-run] would set compatibility" in result.stdout
